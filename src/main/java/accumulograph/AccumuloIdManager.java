package accumulograph;

import org.apache.hadoop.io.Text;

import accumulograph.Const.ElementType;

/**
 * Utility class that helps in converting element ids
 * to/from a {@link Text} field suitable for storing in Accumulo.
 * When storing ids in Accumulo, they will be prefixed according
 * to the type of element (vertex/edge). This will help in
 * MapReducing and range searching.
 * 
 * @author Michael Lieberman
 */
public class AccumuloIdManager {

	/**
	 * Return the appropriate id prefix for the given type.
	 * This can be used in range searches (e.g. for all vertices).
	 * @param type
	 * @return
	 */
	public static Text toIdPrefix(ElementType type) {
		if (type == ElementType.VERTEX) {
			return new Text(Const.VERTEX_ID_PREFIX);
		} else if (type == ElementType.EDGE) {
			return new Text(Const.EDGE_ID_PREFIX);
		} else {
			throw new IllegalArgumentException("Unrecognized type");
		}
	}
	
	/**
	 * Get a sentinel value indicating we have reached the end
	 * of vertices/edges in the table.
	 * @param type
	 * @return
	 */
	public static Text afterIdPrefix(ElementType type) {
		if (type == ElementType.VERTEX) {
			return new Text(Const.VERTEX_ID_PREFIX_AFTER);
		} else if (type == ElementType.EDGE) {
			return new Text(Const.EDGE_ID_PREFIX_AFTER);
		} else {
			throw new IllegalArgumentException("Unrecognized type");
		}		
	}
	
	/**
	 * Take an element and compute the Text value from its id.
	 * @param element The element
	 * @return
	 */
	public static Text toText(AccumuloElement element) {
		return toText(element.getId(), element.getType());
	}

	/**
	 * Take an element id and its type to compute the Text value.
	 * @param id
	 * @param type
	 * @return
	 */
	public static Text toText(AccumuloElementId id, ElementType type) {
		if (type == ElementType.VERTEX) {
			return new Text(Const.VERTEX_ID_PREFIX+id);
		} else if (type == ElementType.EDGE) {
			return new Text(Const.EDGE_ID_PREFIX+id);
		} else {
			throw new IllegalArgumentException("Unrecognized type");
		}
	}
	
	/**
	 * Convert Text value back into an element id.
	 * @param text
	 * @return
	 */
	public static AccumuloElementId fromText(Text text) {
		// Read past the type prefix.
		return new AccumuloElementId(text.toString().substring(1));
	}
}
