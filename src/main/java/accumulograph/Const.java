package accumulograph;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Various constants.
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public final class Const {

	private Const() {

	}

	// For graph configuration.
	public static final String REXSTER_PREFIX = "properties";
	public static final String FACTORY_PREFIX = "blueprints.accumulo";

	public static final String ZOOKEEPERS = "zookeepers";
	public static final String INSTANCE = "instance";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String GRAPH_TABLE = "graphTable";
	public static final String INDEX_TABLE = "indexTable";
	public static final String AUTOFLUSH = "autoflush";
	public static final String MOCK = "mock";
	public static final String RETURN_REMOVED_PROPERTY_VALUES =
			"returnRemovedPropertyValues";
	public static final String USE_PROPERTY_CACHE =
			"usePropertyCache";


	// Type of element.
	public static enum ElementType {VERTEX, EDGE};

	// Vertex-related items.
	public static final String VERTEX_ID_PREFIX = "V";
	// This is a value indicating we have reached the end of vertices in the table.
	public static final String VERTEX_ID_PREFIX_AFTER = "W";

	public static final Text VERTEX_SIGNAL = new Text("MVERTEX");
	public static final Text VERTEX_SIGNAL_AFTER = new Text("MVERTEY");
	public static final Text VERTEX_OUT_EDGE = new Text("EOUT");
	public static final Text VERTEX_IN_EDGE = new Text("EIN");

	// Edge-related items.
	public static final String EDGE_ID_PREFIX = "E";
	// This is a value indicating we have reached the end of edges in the table.
	public static final String EDGE_ID_PREFIX_AFTER = "F";

	public static final Text EDGE_SIGNAL = new Text("MEDGE");
	public static final Text EDGE_SIGNAL_AFTER = new Text("MEDGF");
	public static final Text EDGE_OUT_VERTEX = new Text("VOUT");
	public static final Text EDGE_IN_VERTEX = new Text("VIN");


	// Property-related.
	public static final Text PROPERTY_SIGNAL = new Text("PROP");
	// Index table stuff.
	public static final Text VERTEX_PROPERTY_LIST = new Text("PVLIST"); // Property lists
	public static final Text EDGE_PROPERTY_LIST = new Text("PELIST"); // Property lists


	// Misc.
	public static final Text EMPTY_TEXT = new Text();
	public static final Text NULL_TEXT = null;
	public static final Value EMPTY_VALUE = new Value(new byte[]{});
	public static final Value NULL_VALUE = null;

}
