package accumulograph;

import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import accumulograph.Const.Type;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

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
public class AccumuloGraph implements Graph {

	protected Connector conn;
	protected String table;
	protected boolean autoflush;
	protected Scanner scanner;
	protected BatchScanner batchScanner;
	protected BatchWriter writer;

	/**
	 * Create a graph backed by Accumulo.
	 * @param conn Accumulo Connector instance
	 * @param table Table name
	 * @throws AccumuloException
	 */
	public AccumuloGraph(Connector conn, String table) throws AccumuloException {
		this(conn, table, true);
	}

	/**
	 * Create a graph backed by Accumulo.
	 * @param conn Accumulo Connector instance
	 * @param table Table name
	 * @param autoflush Whether writes are flushed immediately
	 * @throws AccumuloException
	 */
	public AccumuloGraph(Connector conn, String table,
			boolean autoflush) throws AccumuloException {
		try {
			this.conn = conn;
			this.table = table;
			this.autoflush = autoflush;

			// Check whether table exists already and create if not.
			TableOperations ops = conn.tableOperations();
			if (!ops.exists(table)) {
				ops.create(table);
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

	protected void initScannersAndWriter() throws TableNotFoundException {
		scanner = conn.createScanner(table, Constants.NO_AUTHS);
		batchScanner = conn.createBatchScanner(table,
				Constants.NO_AUTHS, 2);

		writer = conn.createBatchWriter(table, 1000000L, 10L, 2);
		if (autoflush) {
			writer = new FlushedBatchWriter(writer);
		}
	}

	public void clear() throws AccumuloException {
		try {
			TableOperations ops = conn.tableOperations();

			if (ops.exists(table)) {
				ops.delete(table);
			}

			ops.create(table);

			initScannersAndWriter();

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
		Mutation m = new Mutation(vertex.getRow());
		m.put(Const.VERTEXTYPE, Const.EMPTY, Const.EMPTYVALUE);
		Utils.addMutation(writer, m);

		// Add to vertex list.
		m = new Mutation(Const.VERTEXTYPE);
		m.put(vertex.getRow(), Const.EMPTY, Const.EMPTYVALUE);
		Utils.addMutation(writer, m);

		return vertex;
	}

	@Override
	public Vertex getVertex(Object id) {
		if (id == null) {
			throw new IllegalArgumentException("Id cannot be null");
		}

		return containsElement(Type.VERTEX, id) ? new AccumuloVertex(this, id) : null;
	}

	@Override
	public void removeVertex(Vertex vertex) {
		AccumuloVertex v = (AccumuloVertex) vertex;

		// Remove all edges that this vertex participates.
		for (Edge edge : vertex.getEdges(Direction.BOTH)) {
			removeEdge(edge);
		}

		// Remove from vertex list.
		Mutation m = new Mutation(Const.VERTEXTYPE);
		m.putDelete(v.getRow(), Const.EMPTY);
		Utils.addMutation(writer, m);

		// Everything else related to vertex.
		m = new Mutation(v.getRow());
		m.putDelete(Const.EMPTY, Const.EMPTY);
		Utils.addMutation(writer, m);
	}

	@Override
	public Iterable<Vertex> getVertices() {
		final AccumuloGraph parent = this;

		return new Iterable<Vertex>() {

			private Iterator<Map.Entry<Key, Value>> iterator;
			private AccumuloVertex current;

			@Override
			public Iterator<Vertex> iterator() {
				scanner.setRange(new Range(Const.VERTEXTYPE));
				iterator = scanner.iterator();

				return new Iterator<Vertex>() {

					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Vertex next() {
						current = makeVertex(iterator.next().getKey().getColumnFamily());
						return current;
					}

					@Override
					public void remove() {
						current.remove();
					}

					private AccumuloVertex makeVertex(Text row) {
						return new AccumuloVertex(parent, Utils.textToEltId(row));						
					}

				};
			}
		};
	}

	// TODO: Use indexes
	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		final String pkey = key;
		final Object pval = value;

		return new Iterable<Vertex>() {

			@Override
			public Iterator<Vertex> iterator() {
				final Iterator<Vertex> i = getVertices().iterator();

				return new Iterator<Vertex>() {

					private Vertex current = null;
					private Vertex next = null;

					private void loadNext() {
						if (next != null) {
							return;
						}

						while (i.hasNext()) {
							next = i.next();

							Object val = next.getProperty(pkey);
							if (pval.equals(val)) {
								return;
							}

							next = null;
						}
					}

					@Override
					public boolean hasNext() {
						loadNext();
						return next != null;
					}

					@Override
					public Vertex next() {
						loadNext();
						current = next;
						next = null;
						return current;
					}

					@Override
					public void remove() {
						current.remove();
					}

				};
			}

		};
	}

	@Override
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex,
			String label) {
		AccumuloVertex out = (AccumuloVertex) outVertex;
		AccumuloVertex in = (AccumuloVertex) inVertex;

		AccumuloEdge edge = new AccumuloEdge(this, id, out, in, label);

		// Add the edge and its information.
		Mutation m = new Mutation(edge.getRow());
		m.put(Const.EDGETYPE, Utils.stringToText(label), Const.EMPTYVALUE);
		m.put(Const.OUTVERTEX, Utils.eltIdToText(Type.VERTEX, out.getId()), Const.EMPTYVALUE);
		m.put(Const.INVERTEX, Utils.eltIdToText(Type.VERTEX, in.getId()), Const.EMPTYVALUE);
		Utils.addMutation(writer, m);

		// Add to edge list.
		m = new Mutation(Const.EDGETYPE);
		m.put(edge.getRow(), Const.EMPTY, Const.EMPTYVALUE);
		Utils.addMutation(writer, m);

		// Update out vertex.
		m = new Mutation(out.getRow());
		m.put(Const.OUTEDGE, edge.getRow(),
				Utils.stringToValue(label));
		Utils.addMutation(writer, m);

		// Update in vertex.
		m = new Mutation(in.getRow());
		m.put(Const.INEDGE, edge.getRow(),
				Utils.stringToValue(label));
		Utils.addMutation(writer, m);

		return edge;
	}

	@Override
	public Edge getEdge(Object id) {
		if (id == null) {
			throw new IllegalArgumentException("Id cannot be null.");
		}

		return containsElement(Type.EDGE, id) ? new AccumuloEdge(this, id) : null;
	}

	@Override
	public void removeEdge(Edge edge) {
		AccumuloEdge e = (AccumuloEdge) edge;

		AccumuloVertex out = (AccumuloVertex) e.getVertex(Direction.OUT);
		AccumuloVertex in = (AccumuloVertex) e.getVertex(Direction.IN);

		// Remove edge info from out/in vertices.
		Mutation m = new Mutation(out.getRow());
		m.putDelete(Const.OUTEDGE, e.getRow());
		Utils.addMutation(writer, m);

		m = new Mutation(in.getRow());
		m.putDelete(Const.INEDGE, e.getRow());
		Utils.addMutation(writer, m);

		// Remove from edge list.
		m = new Mutation(Const.EDGETYPE);
		m.putDelete(e.getRow(), Const.EMPTY);
		Utils.addMutation(writer, m);

		// Remove everything else.
		m = new Mutation(e.getRow());
		m.putDelete(Const.EMPTY, Const.EMPTY);
	}

	@Override
	public Iterable<Edge> getEdges() {
		final AccumuloGraph parent = this;

		return new Iterable<Edge>() {
			private Iterator<Map.Entry<Key, Value>> iterator;
			private AccumuloEdge current;
			private Text cf = new Text();

			@Override
			public Iterator<Edge> iterator() {
				scanner.setRange(new Range(Const.EDGETYPE));
				iterator = scanner.iterator();

				return new Iterator<Edge>() {

					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Edge next() {
						iterator.next().getKey().getColumnFamily(cf);
						current = makeEdge(cf);
						return current;
					}

					@Override
					public void remove() {
						current.remove();
					}

					private AccumuloEdge makeEdge(Text row) {
						return new AccumuloEdge(parent, Utils.textToEltId(row));
					}

				};
			}

		};
	}

	// TODO: Use indexes
	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		final String pkey = key;
		final Object pval = value;

		return new Iterable<Edge>() {

			@Override
			public Iterator<Edge> iterator() {
				final Iterator<Edge> i = getEdges().iterator();

				return new Iterator<Edge>() {

					private Edge current = null;
					private Edge next = null;

					private void loadNext() {
						if (next != null) {
							return;
						}

						while (i.hasNext()) {
							next = i.next();

							Object val = next.getProperty(pkey);
							if (pval.equals(val)) {
								return;
							}

							next = null;
						}
					}

					@Override
					public boolean hasNext() {
						loadNext();
						return next != null;
					}

					@Override
					public Edge next() {
						loadNext();
						current = next;
						next = null;
						return current;
					}

					@Override
					public void remove() {
						current.remove();
					}

				};
			}

		};
	}

	// TODO: Make this more efficient
	@Override
	public GraphQuery query() {
		return new DefaultGraphQuery(this);
	}

	@Override
	public void shutdown() {
		try {
			writer.close();

		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName().toLowerCase()+":"+table;
	}

	protected boolean containsElement(Type type, Object id) {
		scanner.setRange(new Range(Utils.eltIdToText(type, id)));
		return Utils.firstEntry(scanner) != null;
	}

}
