package accumulograph;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import accumulograph.Const.Type;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * Vertex implementation.
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public class AccumuloVertex extends AccumuloElement implements Vertex {

	public AccumuloVertex(AccumuloGraph parent, Object id) {
		super(parent, id, Type.VERTEXID);
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		final Direction dir = direction;
		final String[] edgeLabels = labels;

		return new Iterable<Edge>() {

			private Iterator<Map.Entry<Key, Value>> si;

			@Override
			public Iterator<Edge> iterator() {
				parent.scanner.setRange(new Range(idRow));

				if (takeOut(dir)) {
					parent.scanner.fetchColumnFamily(Const.OUTEDGE);
				}

				if (takeIn(dir)) {
					parent.scanner.fetchColumnFamily(Const.INEDGE);
				}

				si = parent.scanner.iterator();
				parent.scanner.clearColumns();

				return new Iterator<Edge>() {

					private Map.Entry<Key, Value> next = null;
					private AccumuloEdge current;
					private Text cq = new Text();

					private void loadNext() {
						if (next != null) {
							return;
						}

						while (si.hasNext()) {
							next = si.next();

							// No labels to check so we found a good one.
							if (edgeLabels.length == 0) {
								return;
							}

							// Check the label.
							String label = Utils.valueToString(next.getValue());

							for (String l : edgeLabels) {
								if (label.equals(l)) {
									return;
								}
							}
						}

						next = null;
					}

					@Override
					public boolean hasNext() {
						loadNext();
						return next != null;
					}

					@Override
					public Edge next() {
						loadNext();

						if (next == null) {
							return null;
						}

						next.getKey().getColumnQualifier(cq);
						next = null;

						current = new AccumuloEdge(parent, Utils.textToTypedObject(cq));
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
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		final Direction dir = direction;
		final String[] edgeLabels = labels;

		return new Iterable<Vertex>() {

			private Iterator<Map.Entry<Key, Value>> i;

			@Override
			public Iterator<Vertex> iterator() {
				// First we get edges attached to this vertex.
				parent.scanner.setRange(new Range(idRow));

				if (takeOut(dir)) {
					parent.scanner.fetchColumnFamily(Const.OUTEDGE);
				}

				if (takeIn(dir)) {
					parent.scanner.fetchColumnFamily(Const.INEDGE);
				}

				Iterator<Map.Entry<Key, Value>> si = parent.scanner.iterator();
				parent.scanner.clearColumns();

				List<Range> ranges = new LinkedList<Range>();
				while (si.hasNext()) {
					Map.Entry<Key, Value> entry = si.next();

					boolean take = true;
					if (edgeLabels.length > 0) {
						// Need to check edge label.
						take = false;

						String label = Utils.valueToString(entry.getValue());

						for (String l : edgeLabels) {
							if (l.equals(label)) {
								take = true;
								break;
							}
						}
					}

					if (!take) {
						continue;
					}

					ranges.add(new Range(entry.getKey().getColumnQualifier()));
				}

				if (ranges.isEmpty()) {
					return new NullIterator<Vertex>();
				}

				parent.batchScanner.setRanges(ranges);
				// Need both endpoints of each edge.
				parent.batchScanner.fetchColumnFamily(Const.INVERTEX);
				parent.batchScanner.fetchColumnFamily(Const.OUTVERTEX);
				i = parent.batchScanner.iterator();
				parent.batchScanner.clearColumns();

				return new Iterator<Vertex>() {

					private Queue<Text> rowIds = new LinkedList<Text>();
					private Text endpointType = new Text();
					private Text outVertexRow = new Text();
					private Text inVertexRow = new Text();
					private Vertex current = null;

					private void loadMore() {
						if (!rowIds.isEmpty()) {
							return;
						}

						while (i.hasNext()) {
							// Get the two edge endpoints.
							// TODO This code relies on the endpoints being
							//   adjacent in Accumulo, which does not seem robust.
							Map.Entry<Key, Value> entry = i.next();
							entry.getKey().getColumnFamily(endpointType);
							entry.getKey().getColumnQualifier(endpointType.equals(Const.OUTVERTEX) ? outVertexRow : inVertexRow);

							entry = i.next();
							entry.getKey().getColumnFamily(endpointType);
							entry.getKey().getColumnQualifier(endpointType.equals(Const.OUTVERTEX) ? outVertexRow : inVertexRow);

							if (takeOut(dir) && outVertexRow.equals(idRow)) {
								rowIds.add(inVertexRow);
							}

							if (takeIn(dir) && inVertexRow.equals(idRow)
									// Don't want duplicates for self-loops.
									&& !rowIds.contains(outVertexRow)) {
								rowIds.add(outVertexRow);
							}

							if (!rowIds.isEmpty()) {
								return;
							}
						}
					}

					@Override
					public boolean hasNext() {
						loadMore();
						return !rowIds.isEmpty();
					}

					@Override
					public Vertex next() {
						loadMore();
						current = new AccumuloVertex(parent,
								Utils.textToTypedObject(rowIds.remove()));
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

	protected boolean takeIn(Direction direction) {
		return direction == Direction.IN || direction == Direction.BOTH;
	}

	protected boolean takeOut(Direction direction) {
		return direction == Direction.OUT || direction == Direction.BOTH;
	}

	// TODO: Make this more efficient
	@Override
	public VertexQuery query() {
		return new DefaultVertexQuery(this);
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex) {
		return parent.addEdge(null, this, inVertex, label);
	}

	@Override
	public void remove() {
		parent.removeVertex(this);
	}

	@Override
	public String toString() {
		return "[V:"+id+"]";
	}

	protected class NullIterator<T> implements Iterator<T> {
		@Override
		public boolean hasNext() {return false;}
		@Override
		public T next() {return null;}
		@Override
		public void remove() {}
	}

}
