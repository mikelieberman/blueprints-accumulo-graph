package accumulograph;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;

import accumulograph.Const.ElementType;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;

/**
 * An implementation of Blueprints backed by Accumulo.
 * 
 * The graph is stored in a single table with the following schema.
   <table border="1">
	<tr><th>Row</th><th>Col Fam</th><th>Col Qual</th><th>Value</th><th>Purpose</th></tr>
	 <tr><td>[vertex id]</td><td>MVERTEX</td><td>-</td><td>-</td><td>States this is a vertex</td></tr>
	 <tr><td>[vertex id]</td><td>EOUT</td><td>[edge id]</td><td>[edge label]</td><td>Vertex's out-edge</td></tr>
	 <tr><td>[vertex id]</td><td>EIN</td><td>[edge id]</td><td>[edge label]</td><td>Vertex's in-edge</td></tr>
	 <tr><td>[edge id]</td><td>MEDGE</td><td>[edge label]</td><td>-</td><td>States this is an edge</td></tr>
	 <tr><td>[edge id]</td><td>VOUT</td><td>[vertex id]</td><td>-</td><td>Edge's out-vertex</td></tr>
	 <tr><td>[edge id]</td><td>VIN</td><td>[vertex id]</td><td>-</td><td>Edge's in-vertex</td></tr>
	 <tr><td>[v/e id]</td><td>PROP</td><td>[property name]</td><td>[property value]</td><td>Property</td></tr>
	 <tr><td>MVERTEX</td><td>[vertex id]</td><td>-</td><td>-</td><td>For vertex list</td></tr>
	 <tr><td>MEDGE</td><td>[edge id]</td><td>-</td><td>-</td><td>For edge list</td></tr>
    </table>
 * 
 * @author Mike Lieberman (http://mikelieberman.org)
 *
 */
public class AccumuloGraph implements KeyIndexableGraph {

	protected AccumuloGraphOptions opts;
	protected Scanner scanner;
	protected BatchScanner batchScanner;
	protected BatchWriter writer;

	protected AccumuloKeyIndex keyIndex;

	/**
	 * Create a graph backed by Accumulo. This is used
	 * when using {@link GraphFactory}.
	 * @param properties
	 * @return
	 * @throws AccumuloException
	 */
	public static AccumuloGraph open(Configuration properties) throws AccumuloException {
		return new AccumuloGraph(properties);
	}

	/**
	 * Create a graph backed by Accumulo. This is used
	 * when using {@link GraphFactory}.
	 * @param properties The properties
	 * @throws AccumuloException 
	 */
	public AccumuloGraph(Configuration properties) throws AccumuloException {
		this(AccumuloGraphConfiguration.parseProperties(properties, Const.FACTORY_PREFIX));
	}

	/**
	 * Create a graph backed by Accumulo.
	 * This is the main constructor.
	 * @param opts Graph options
	 * @throws AccumuloException
	 */
	public AccumuloGraph(AccumuloGraphOptions opts) throws AccumuloException {
		validateOptions(opts);

		try {
			this.opts = opts;

			Utils.createTableIfNotExists(opts.getConnector(), opts.getGraphTable());

			if (opts.getIndexTable() != null) {
				Utils.createTableIfNotExists(opts.getConnector(), opts.getIndexTable());
				keyIndex = new AccumuloKeyIndex(this);
			}

			initScannersAndWriter();

		} catch (TableNotFoundException e) {
			throw new AccumuloException(e);
		} catch (AccumuloSecurityException e) {
			throw new AccumuloException(e);
		} catch (TableExistsException e) {
			throw new AccumuloException(e);
		}
	}

	protected static void validateOptions(AccumuloGraphOptions opts) throws AccumuloException {
		if (opts.getConnector() == null) {
			throw new IllegalArgumentException("Connector not set");
		} else if (opts.getGraphTable() == null) {
			throw new IllegalArgumentException("Graph table not specified");
		}
	}

	protected void initScannersAndWriter() throws TableNotFoundException, AccumuloException {
		scanner = opts.getConnector().createScanner(opts.getGraphTable(), Constants.NO_AUTHS);
		batchScanner = opts.getConnector().createBatchScanner(opts.getGraphTable(),
				Constants.NO_AUTHS, 2);

		writer = opts.getConnector().createBatchWriter(opts.getGraphTable(), 1000000L, 10L, 2);
		if (opts.getAutoflush()) {
			writer = new FlushedBatchWriter(writer);
		}
	}

	public void clear() throws AccumuloException {
		try {
			Utils.recreateTable(opts.getConnector(), opts.getGraphTable());

			initScannersAndWriter();

			if (keyIndex != null) {
				keyIndex.clear();
			}

		} catch (AccumuloSecurityException e) {
			throw new AccumuloException(e);
		} catch (TableNotFoundException e) {
			throw new AccumuloException(e);
		} catch (TableExistsException e) {
			throw new AccumuloException(e);
		}
	}

	@Override
	public Features getFeatures() {
		return new AccumuloFeatures();
	}

	@Override
	public Vertex addVertex(Object id) {
		AccumuloVertex vertex = new AccumuloVertex(this, id);

		// Add vertex.
		Mutation m = new Mutation(vertex.getIdRow());
		m.put(Const.VERTEX_SIGNAL, Const.EMPTY_TEXT, Const.EMPTY_VALUE);
		Utils.addMutation(writer, m);

		// Add to vertex list.
		m = new Mutation(Const.VERTEX_SIGNAL);
		m.put(vertex.getIdRow(), Const.EMPTY_TEXT, Const.EMPTY_VALUE);
		Utils.addMutation(writer, m);

		if (keyIndex != null) {
			keyIndex.addOrRemoveFromIndex(vertex, true);
		}

		return vertex;
	}

	@Override
	public Vertex getVertex(Object id) {
		if (id == null) {
			throw new IllegalArgumentException("Id cannot be null");
		}

		AccumuloElementId eid = new AccumuloElementId(id);
		return containsElement(eid, ElementType.VERTEX) ? new AccumuloVertex(this, eid) : null;
	}

	@Override
	public void removeVertex(Vertex vertex) {
		AccumuloVertex v = (AccumuloVertex) vertex;

		// Remove from index.
		if (keyIndex != null) {
			keyIndex.addOrRemoveFromIndex(v, false);
		}

		// Remove all edges that this vertex participates.
		for (Edge edge : vertex.getEdges(Direction.BOTH)) {
			removeEdge(edge);
		}

		// Remove from vertex list.
		Mutation m = new Mutation(Const.VERTEX_SIGNAL);
		m.putDelete(v.getIdRow(), Const.EMPTY_TEXT);
		Utils.addMutation(writer, m);

		// Everything else related to vertex.
		m = new Mutation(v.getIdRow());
		m.putDelete(Const.EMPTY_TEXT, Const.EMPTY_TEXT);
		Utils.addMutation(writer, m);
	}

	@Override
	public Iterable<Vertex> getVertices() {
		return getVerticesInRange(null, null);
	}

	/**
	 * Retrieve vertices with ids in a given range. Null means infinite.
	 * @param minId Minimum id
	 * @param maxId Maximum id
	 * @return
	 */
	public Iterable<Vertex> getVerticesInRange(final Object minId, final Object maxId) {
		if ((minId == null) != (maxId == null)) {
			throw new IllegalArgumentException("Specify both min and max, or null for both");
		}

		final AccumuloGraph parent = this;

		return new Iterable<Vertex>() {
			private Iterator<Map.Entry<Key, Value>> iterator;
			private Text eltIdCf = new Text();

			@Override
			public Iterator<Vertex> iterator() {
				Key minKey;
				if (minId == null) {
					minKey = new Key(Const.VERTEX_SIGNAL);
				} else {
					minKey = new Key(Const.VERTEX_SIGNAL,
							AccumuloIdManager.toText(
									new AccumuloElementId(minId), ElementType.VERTEX));
				}

				Key maxKey;
				if (maxId == null) {
					maxKey = minKey.followingKey(PartialKey.ROW);
				} else {
					maxKey = new Key(Const.VERTEX_SIGNAL,
							AccumuloIdManager.toText(
									new AccumuloElementId(maxId), ElementType.VERTEX));
				}

				scanner.setRange(new Range(minKey, maxKey));
				iterator = scanner.iterator();

				return new Iterator<Vertex>() {

					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Vertex next() {
						if (!iterator.hasNext()) {
							throw new NoSuchElementException();
						}

						iterator.next().getKey().getColumnFamily(eltIdCf);
						return makeVertex(eltIdCf);
					}

					@Override
					public void remove() {
						iterator.remove();
					}

					private AccumuloVertex makeVertex(Text id) {
						return new AccumuloVertex(parent, AccumuloIdManager.fromText(id));						
					}

				};
			}
		};
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		if (keyIndex != null && keyIndex.getIndexedKeys(AccumuloVertex.class).contains(key)) {
			return new SubclassIterable<Vertex>(
					keyIndex.getElements(key, value, AccumuloVertex.class));
		}
		else {
			return new PropertyFilteredIterable<Vertex>(key, value, getVertices());
		}
	}

	@Override
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex,
			String label) {
		if (label == null) {
			throw new IllegalArgumentException("Cannot add edge with null label");
		}
		AccumuloVertex out = (AccumuloVertex) outVertex;
		AccumuloVertex in = (AccumuloVertex) inVertex;

		AccumuloEdge edge = new AccumuloEdge(this, id, out, in, label);

		// Add the edge and its information.
		Mutation m = new Mutation(edge.getIdRow());
		m.put(Const.EDGE_SIGNAL, Utils.stringToText(label), Const.EMPTY_VALUE);
		m.put(Const.EDGE_OUT_VERTEX, AccumuloIdManager.toText(out), Const.EMPTY_VALUE);
		m.put(Const.EDGE_IN_VERTEX, AccumuloIdManager.toText(in), Const.EMPTY_VALUE);
		Utils.addMutation(writer, m);

		// Add to edge list.
		m = new Mutation(Const.EDGE_SIGNAL);
		m.put(edge.getIdRow(), Const.EMPTY_TEXT, Const.EMPTY_VALUE);
		Utils.addMutation(writer, m);

		// Update out vertex.
		m = new Mutation(out.getIdRow());
		m.put(Const.VERTEX_OUT_EDGE, edge.getIdRow(),
				Utils.stringToValue(label));
		Utils.addMutation(writer, m);

		// Update in vertex.
		m = new Mutation(in.getIdRow());
		m.put(Const.VERTEX_IN_EDGE, edge.getIdRow(),
				Utils.stringToValue(label));
		Utils.addMutation(writer, m);

		if (keyIndex != null) {
			keyIndex.addOrRemoveFromIndex(edge, true);
		}

		return edge;
	}

	@Override
	public Edge getEdge(Object id) {
		if (id == null) {
			throw new IllegalArgumentException("Id cannot be null.");
		}

		AccumuloElementId eid = new AccumuloElementId(id);
		return containsElement(eid, ElementType.EDGE) ? new AccumuloEdge(this, eid) : null;
	}

	@Override
	public void removeEdge(Edge edge) {
		AccumuloEdge e = (AccumuloEdge) edge;

		// Remove from index.
		if (keyIndex != null) {
			keyIndex.addOrRemoveFromIndex(e, false);
		}

		AccumuloVertex out = (AccumuloVertex) e.getVertex(Direction.OUT);
		AccumuloVertex in = (AccumuloVertex) e.getVertex(Direction.IN);

		// Remove edge info from out/in vertices.
		Mutation m = new Mutation(out.getIdRow());
		m.putDelete(Const.VERTEX_OUT_EDGE, e.getIdRow());
		Utils.addMutation(writer, m);

		m = new Mutation(in.getIdRow());
		m.putDelete(Const.VERTEX_IN_EDGE, e.getIdRow());
		Utils.addMutation(writer, m);

		// Remove from edge list.
		m = new Mutation(Const.EDGE_SIGNAL);
		m.putDelete(e.getIdRow(), Const.EMPTY_TEXT);
		Utils.addMutation(writer, m);

		// Remove everything else.
		m = new Mutation(e.getIdRow());
		m.putDelete(Const.EMPTY_TEXT, Const.EMPTY_TEXT);
	}

	@Override
	public Iterable<Edge> getEdges() {
		return getEdgesInRange(null, null);
	}

	public Iterable<Edge> getEdgesInRange(final Object minId, final Object maxId) {
		if ((minId == null) != (maxId == null)) {
			throw new IllegalArgumentException("Specify both min and max, or null for both");
		}

		final AccumuloGraph parent = this;

		return new Iterable<Edge>() {
			private Iterator<Map.Entry<Key, Value>> iterator;
			private Text eltIdCf = new Text();

			@Override
			public Iterator<Edge> iterator() {
				Key minKey;
				if (minId == null) {
					minKey = new Key(Const.EDGE_SIGNAL);
				} else {
					minKey = new Key(Const.EDGE_SIGNAL,
							AccumuloIdManager.toText(new AccumuloElementId(minId), ElementType.EDGE));
				}

				Key maxKey;
				if (maxId == null) {
					maxKey = minKey.followingKey(PartialKey.ROW);
				} else {
					maxKey = new Key(Const.EDGE_SIGNAL,
							AccumuloIdManager.toText(new AccumuloElementId(maxId), ElementType.EDGE));
				}

				scanner.setRange(new Range(minKey, maxKey));
				iterator = scanner.iterator();

				return new Iterator<Edge>() {

					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Edge next() {
						if (!iterator.hasNext()) {
							throw new NoSuchElementException();
						}

						iterator.next().getKey().getColumnFamily(eltIdCf);
						return makeEdge(eltIdCf);
					}

					@Override
					public void remove() {
						iterator.remove();
					}

					private AccumuloEdge makeEdge(Text id) {
						return new AccumuloEdge(parent, AccumuloIdManager.fromText(id));
					}

				};
			}

		};
	}

	// TODO: Use indexes
	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		if (keyIndex != null && keyIndex.getIndexedKeys(AccumuloEdge.class).contains(key)) {
			return new SubclassIterable<Edge>(
					keyIndex.getElements(key, value, AccumuloEdge.class));
		}
		else {
			return new PropertyFilteredIterable<Edge>(key, value, getEdges());
		}
	}

	// TODO: Make this more efficient
	@Override
	public GraphQuery query() {
		return new DefaultGraphQuery(this);
	}

	@Override
	public void shutdown() {
		try {
			writer.flush();
			writer.close();

			if (keyIndex != null) {
				keyIndex.close();
			}

			// TODO Still some timing issues here...
			Utils.sleep(50L);

		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName().toLowerCase()+":"+opts.getGraphTable();
	}

	protected boolean containsElement(AccumuloElementId id, ElementType type) {
		scanner.setRange(new Range(AccumuloIdManager.toText(id, type)));
		return Utils.firstEntry(scanner) != null;
	}

	@Override
	public <T extends Element> void dropKeyIndex(String key,
			Class<T> elementClass) {
		if (keyIndex != null) {
			keyIndex.dropKeyIndex(key,
					elementClass.equals(Vertex.class) ? AccumuloVertex.class : AccumuloEdge.class);
		}
	}

	@Override
	public <T extends Element> void createKeyIndex(String key,
			Class<T> elementClass, @SuppressWarnings("rawtypes") Parameter... indexParameters) {
		if (keyIndex != null) {
			keyIndex.createKeyIndex(key,
					elementClass.equals(Vertex.class) ? AccumuloVertex.class : AccumuloEdge.class);
		}
	}

	@Override
	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		if (keyIndex != null) {
			return keyIndex.getIndexedKeys(
					elementClass.equals(Vertex.class) ? AccumuloVertex.class : AccumuloEdge.class);
		}
		else {
			return null;
		}
	}

}
