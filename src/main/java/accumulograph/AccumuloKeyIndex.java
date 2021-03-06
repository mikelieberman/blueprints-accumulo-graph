package accumulograph;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Element;

/**
 * Key index implementation.  The key index is stored in
 * a separate table, specified using options.
 * 
 * TODO Make this faster by prefixing property rows as appropriate
 * (V/E) and then use Utils.deleteRow() to speedily drop indexed keys.
 * 
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public class AccumuloKeyIndex {

	private AccumuloGraph parent;
	private Scanner indexScanner;
	private BatchWriter indexWriter;
	private Set<String> indexedVertexKeys;
	private Set<String> indexedEdgeKeys;

	public AccumuloKeyIndex(AccumuloGraph parent) throws TableNotFoundException, AccumuloException {
		this.parent = parent;

		String table = parent.opts.getIndexTable();
		if (table == null) {
			throw new IllegalArgumentException("No index table specified");
		}

		indexedVertexKeys = new HashSet<String>();
		indexedEdgeKeys = new HashSet<String>();

		initScannerAndWriter();
		reloadIndexedKeys();
	}

	public void clear() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
		Utils.recreateTable(parent.opts.getConnector(),
				parent.opts.getIndexTable());
		initScannerAndWriter();
	}

	public void close() throws MutationsRejectedException {
		indexWriter.flush();
		indexWriter.close();
	}

	protected void initScannerAndWriter() throws TableNotFoundException, AccumuloException {
		indexScanner = parent.opts.getConnector().createScanner(
				parent.opts.getIndexTable(), Constants.NO_AUTHS);

		indexWriter = parent.opts.getConnector().createBatchWriter(
				parent.opts.getIndexTable(), 1000000L, 10L, 2);
		if (parent.opts.getAutoflush()) {
			indexWriter = new FlushedBatchWriter(indexWriter);
		}
	}

	public <T extends AccumuloElement> void createKeyIndex(String key, Class<T> elementClass) {
		Set<String> indexedKeys;
		Iterable<? extends Element> elements;
		Text propertyList;

		if (elementClass.equals(AccumuloVertex.class)) {
			indexedKeys = indexedVertexKeys;
			elements = parent.getVertices();
			propertyList = Const.VERTEX_PROPERTY_LIST;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			elements = parent.getEdges();
			propertyList = Const.EDGE_PROPERTY_LIST;
		}

		if (indexedKeys.contains(key)) {
			return;
		}

		// Add to indexed key list.
		Mutation m = new Mutation(propertyList);
		m.put(Utils.stringToText(key), Const.EMPTY_TEXT, Const.EMPTY_VALUE);
		Utils.addMutation(indexWriter, m, 50L);
		// Flush explicitly because this is important.
		Utils.flush(indexWriter);
		reloadIndexedKeys();

		// Find elements and do stuff.
		for (Element element : elements) {
			addOrRemoveFromIndex((AccumuloElement) element, true);
		}
	}

	public <T extends AccumuloElement> void dropKeyIndex(String key, Class<T> elementClass) {
		Set<String> indexedKeys;
		Text propertyList;

		if (elementClass.equals(AccumuloVertex.class)) {
			indexedKeys = indexedVertexKeys;
			propertyList = Const.VERTEX_PROPERTY_LIST;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			propertyList = Const.EDGE_PROPERTY_LIST;
		}

		if (!indexedKeys.contains(key)) {
			return;
		}

		// Remove elements with this key in the index.
		for (AccumuloElement element : getElements(key, null, elementClass)) {
			// TODO This doesn't remove stuff from the index table.
			addOrRemoveFromIndex(element, false);
		}

		// Remove index from list.
		Mutation m = new Mutation(propertyList);
		m.putDelete(Utils.stringToText(key), Const.EMPTY_TEXT);
		Utils.addMutation(indexWriter, m, 50L);
		// Flush explicitly because this is important.
		Utils.flush(indexWriter);
		reloadIndexedKeys();
	}

	@SuppressWarnings("unchecked")
	public <T extends AccumuloElement> Iterable<T> getElements(String key, Object value,
			Class<T> elementClass) {
		indexScanner.setRange(new Range(Utils.stringToText(key)));
		indexScanner.clearColumns();

		// If specified, restrict to a specific value.
		if (value != null) {
			indexScanner.fetchColumnFamily(Utils.objectToText(value));
		}

		final Iterator<Map.Entry<Key, Value>> i = indexScanner.iterator();
		final Class<T> eltClass = elementClass;

		return new Iterable<T>() {

			@Override
			public Iterator<T> iterator() {
				return new Iterator<T>() {

					private Text eltIdCq = new Text();

					@Override
					public boolean hasNext() {
						return i.hasNext();
					}

					@Override
					public T next() {
						if (!i.hasNext()) {
							throw new NoSuchElementException();
						}

						Map.Entry<Key, Value> entry = i.next();
						entry.getKey().getColumnQualifier(eltIdCq);

						if (eltClass.equals(AccumuloVertex.class)) {
							return (T) new AccumuloVertex(parent, AccumuloIdManager.fromText(eltIdCq));
						}
						else {
							return (T) new AccumuloEdge(parent, AccumuloIdManager.fromText(eltIdCq));
						}
					}

					@Override
					public void remove() {
						i.remove();
					}
				};
			};
		};
	}

	public <T extends AccumuloElement> Set<String> getIndexedKeys(Class<T> elementClass) {
		return Collections.unmodifiableSet(
				elementClass.equals(AccumuloVertex.class) ? indexedVertexKeys : indexedEdgeKeys);
	}

	public <T extends AccumuloElement> void addPropertyToIndex(T element, String key, Object value) {
		Set<String> indexedKeys;

		if (element instanceof AccumuloVertex) {
			indexedKeys = indexedVertexKeys;
		}
		else {
			indexedKeys = indexedEdgeKeys;
		}

		// Don't add things that are not indexed.
		if (!indexedKeys.contains(key)) {
			return;
		}

		Mutation m = new Mutation(Utils.stringToText(key));
		m.put(Utils.objectToText(value), AccumuloIdManager.toText(element), Const.EMPTY_VALUE);
		Utils.addMutation(indexWriter, m);
	}

	public <T extends AccumuloElement> void removePropertyFromIndex(T element, String key, Object value) {
		Set<String> indexedKeys;

		if (element instanceof AccumuloVertex) {
			indexedKeys = indexedVertexKeys;
		}
		else {
			indexedKeys = indexedEdgeKeys;
		}

		// Don't remove things that are not indexed.
		if (!indexedKeys.contains(key)) {
			return;
		}

		Mutation m = new Mutation(Utils.stringToText(key));
		m.putDelete(Utils.objectToText(value), AccumuloIdManager.toText(element));
		Utils.addMutation(indexWriter, m);
	}

	public <T extends AccumuloElement> void reindexKey(T element, String key, Object value) {
		Set<String> indexedKeys;

		if (element instanceof AccumuloVertex) {
			indexedKeys = indexedVertexKeys;
		}
		else {
			indexedKeys = indexedEdgeKeys;
		}

		Mutation m = new Mutation(Utils.stringToText(key));

		if (indexedKeys.contains(key)) {
			m.put(Utils.objectToText(value), AccumuloIdManager.toText(element), Const.EMPTY_VALUE);
		}
		else {
			m.putDelete(Utils.objectToText(value), AccumuloIdManager.toText(element));
		}

		Utils.addMutation(indexWriter, m);
	}

	public <T extends AccumuloElement> void addOrRemoveFromIndex(T element, boolean add) {
		Set<String> indexedKeys;

		if (element instanceof AccumuloVertex) {
			indexedKeys = indexedVertexKeys;
		}
		else {
			indexedKeys = indexedEdgeKeys;
		}

		Text eltIdCq = AccumuloIdManager.toText(element);

		for (String key : indexedKeys) {
			Object value = element.getProperty(key);

			if (value != null) {
				Mutation m = new Mutation(Utils.stringToText(key));

				if (add) {
					m.put(Utils.objectToText(value), eltIdCq, Const.EMPTY_VALUE);
				}
				else {
					// TODO This is executing, but not actually deleting
					//   from the index table.
					m.putDelete(Utils.objectToText(value), eltIdCq);
				}

				Utils.addMutation(indexWriter, m);
			}
		}
	}

	private void reloadIndexedKeys() {
		Text cf = new Text();


		indexedVertexKeys.clear();

		indexScanner.setRange(new Range(Const.VERTEX_PROPERTY_LIST));
		indexScanner.clearColumns();

		for (Map.Entry<Key, Value> entry : indexScanner) {
			entry.getKey().getColumnFamily(cf);
			indexedVertexKeys.add(Utils.textToString(cf));
		}


		indexedEdgeKeys.clear();

		indexScanner.setRange(new Range(Const.EDGE_PROPERTY_LIST));
		indexScanner.clearColumns();

		for (Map.Entry<Key, Value> entry : indexScanner) {
			entry.getKey().getColumnFamily(cf);
			indexedEdgeKeys.add(Utils.textToString(cf));
		}
	}

}
