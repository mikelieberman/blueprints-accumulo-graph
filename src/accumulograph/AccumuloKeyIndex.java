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

import accumulograph.Const.Type;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * Key index implementation.  The key index is stored in
 * a separate table, specified using options.
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public class AccumuloKeyIndex {

	private AccumuloGraph parent;
	private Scanner indexScanner;
	private BatchWriter indexWriter;
	private Set<String> indexedVertexKeys;
	private Set<String> indexedEdgeKeys;

	public AccumuloKeyIndex(AccumuloGraph parent) throws TableNotFoundException {
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

	protected void initScannerAndWriter() throws TableNotFoundException {
		indexScanner = parent.opts.getConnector().createScanner(
				parent.opts.getIndexTable(), Constants.NO_AUTHS);

		indexWriter = parent.opts.getConnector().createBatchWriter(
				parent.opts.getIndexTable(), 1000000L, 10L, 2);
		if (parent.opts.getAutoflush()) {
			indexWriter = new FlushedBatchWriter(indexWriter);
		}
	}

	@SuppressWarnings("unchecked")
	public <T extends Element> void createKeyIndex(String key, Class<T> elementClass) {
		Set<String> indexedKeys;
		Iterable<T> elements;
		Text propertyList;

		if (elementClass.equals(Vertex.class)) {
			indexedKeys = indexedVertexKeys;
			elements = (Iterable<T>) parent.getVertices();
			propertyList = Const.VERTEXPROPERTYLIST;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			elements = (Iterable<T>) parent.getEdges();
			propertyList = Const.EDGEPROPERTYLIST;
		}

		if (indexedKeys.contains(key)) {
			return;
		}

		// Add to indexed key list.
		Mutation m = new Mutation(propertyList);
		m.put(Utils.stringToText(key), Const.EMPTY, Const.EMPTYVALUE);
		Utils.addMutation(indexWriter, m, 50L);
		// Flush explicitly because this is important.
		Utils.flush(indexWriter);
		reloadIndexedKeys();

		// Find elements and do stuff.
		for (Element element : elements) {
			addOrRemoveFromIndex(element, true);
		}
	}

	public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
		Set<String> indexedKeys;
		Text propertyList;

		if (elementClass.equals(Vertex.class)) {
			indexedKeys = indexedVertexKeys;
			propertyList = Const.VERTEXPROPERTYLIST;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			propertyList = Const.EDGEPROPERTYLIST;
		}

		if (!indexedKeys.contains(key)) {
			return;
		}

		// Remove elements with this key in the index.
		for (Element element : getElements(key, null, elementClass)) {
			// TODO This doesn't remove stuff from the index table.
			addOrRemoveFromIndex(element, false);
		}

		// Remove index from list.
		Mutation m = new Mutation(propertyList);
		m.putDelete(Utils.stringToText(key), Const.EMPTY);
		Utils.addMutation(indexWriter, m, 50L);
		// Flush explicitly because this is important.
		Utils.flush(indexWriter);
		reloadIndexedKeys();
	}

	@SuppressWarnings("unchecked")
	public <T extends Element> Iterable<T> getElements(String key, Object value,
			Class<T> elementClass) {
		Type propertyType;

		if (elementClass.equals(Vertex.class)) {
			propertyType = Type.VERTEXPROPERTY;
		}
		else {
			propertyType = Type.EDGEPROPERTY;
		}

		indexScanner.setRange(new Range(Utils.typedObjectToText(propertyType, key)));
		// If specified, restrict to a specific value.
		if (value != null) {
			indexScanner.fetchColumnFamily(Utils.objectToText(value));
		}

		final Iterator<Map.Entry<Key, Value>> i = indexScanner.iterator();
		final Class<T> eltClass = elementClass;

		indexScanner.clearColumns();

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

						if (eltClass.equals(Vertex.class)) {
							return (T) new AccumuloVertex(parent, Utils.textToTypedObject(eltIdCq));
						}
						else {
							return (T) new AccumuloEdge(parent, Utils.textToTypedObject(eltIdCq));
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

	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		return Collections.unmodifiableSet(
				elementClass.equals(Vertex.class) ? indexedVertexKeys : indexedEdgeKeys);
	}
	
	public <T extends Element> void addPropertyToIndex(T element, String key, Object value) {
		Set<String> indexedKeys;
		Type propertyType;
		Type elementIdType;

		if (element instanceof Vertex) {
			indexedKeys = indexedVertexKeys;
			propertyType = Type.VERTEXPROPERTY;
			elementIdType = Type.VERTEXID;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			propertyType = Type.EDGEPROPERTY;
			elementIdType = Type.VERTEXID;
		}
		
		// Don't add things that are not indexed.
		if (!indexedKeys.contains(key)) {
			return;
		}

		Mutation m = new Mutation(Utils.typedObjectToText(propertyType, key));
		m.put(Utils.objectToText(value),
				Utils.typedObjectToText(elementIdType, element.getId()), Const.EMPTYVALUE);
		Utils.addMutation(indexWriter, m);
	}
	
	public <T extends Element> void removePropertyFromIndex(T element, String key) {
		Set<String> indexedKeys;
		Type propertyType;
		Type elementIdType;

		if (element instanceof Vertex) {
			indexedKeys = indexedVertexKeys;
			propertyType = Type.VERTEXPROPERTY;
			elementIdType = Type.VERTEXID;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			propertyType = Type.EDGEPROPERTY;
			elementIdType = Type.VERTEXID;
		}
		
		// Don't remove things that are still indexed.
		if (indexedKeys.contains(key)) {
			return;
		}
		
		Object value = element.getProperty(key);
		
		if (value != null) {
			Mutation m = new Mutation(Utils.typedObjectToText(propertyType, key));
			m.putDelete(Utils.objectToText(value),
					Utils.typedObjectToText(elementIdType, element.getId()));
			Utils.addMutation(indexWriter, m);	
		}
	}

	public <T extends Element> void reindexKey(T element, String key, Object value) {
		Set<String> indexedKeys;
		Type propertyType;
		Type elementIdType;

		if (element instanceof Vertex) {
			indexedKeys = indexedVertexKeys;
			propertyType = Type.VERTEXPROPERTY;
			elementIdType = Type.VERTEXID;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			propertyType = Type.EDGEPROPERTY;
			elementIdType = Type.VERTEXID;
		}

		Mutation m = new Mutation(Utils.typedObjectToText(propertyType, key));
		
		if (indexedKeys.contains(key)) {
			m.put(Utils.objectToText(value),
					Utils.typedObjectToText(elementIdType, element.getId()), Const.EMPTYVALUE);
		}
		else {
			m.putDelete(Utils.objectToText(value),
					Utils.typedObjectToText(elementIdType, element.getId()));
		}

		Utils.addMutation(indexWriter, m);
	}

	public <T extends Element> void addOrRemoveFromIndex(T element, boolean add) {
		Set<String> indexedKeys;
		Type propertyType;
		Type elementIdType;

		if (element instanceof Vertex) {
			indexedKeys = indexedVertexKeys;
			propertyType = Type.VERTEXPROPERTY;
			elementIdType = Type.VERTEXID;
		}
		else {
			indexedKeys = indexedEdgeKeys;
			propertyType = Type.EDGEPROPERTY;
			elementIdType = Type.EDGEID;
		}

		Text eltIdCq = Utils.typedObjectToText(elementIdType, element.getId());

		for (String key : indexedKeys) {
			Object value = element.getProperty(key);

			if (value != null) {
				Mutation m = new Mutation(Utils.typedObjectToText(propertyType, key));

				if (add) {
					m.put(Utils.objectToText(value), eltIdCq, Const.EMPTYVALUE);
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
		indexScanner.setRange(new Range(Const.VERTEXPROPERTYLIST));
		for (Map.Entry<Key, Value> entry : indexScanner) {
			entry.getKey().getColumnFamily(cf);
			indexedVertexKeys.add(Utils.textToString(cf));
		}

		indexedEdgeKeys.clear();
		indexScanner.setRange(new Range(Const.EDGEPROPERTYLIST));
		for (Map.Entry<Key, Value> entry : indexScanner) {
			entry.getKey().getColumnFamily(cf);
			indexedEdgeKeys.add(Utils.textToString(cf));
		}
	}

}
