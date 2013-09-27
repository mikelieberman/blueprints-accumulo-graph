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

	// Types of Accumulo records.
	public static enum Type {METADATA, VERTEX_ID, EDGE_ID, VERTEX_PROPERTY, EDGE_PROPERTY};

	// Vertex-related.
	public static final Text VERTEX_TYPE = new Text("MVERTEX");
	public static final Text VERTEX_OUT_EDGE = new Text("EOUT");
	public static final Text VERTEX_IN_EDGE = new Text("EIN");

	// Edge-related.
	public static final Text EDGE_TYPE = new Text("MEDGE");
	public static final Text EDGE_OUT_VERTEX = new Text("VOUT");
	public static final Text EDGE_IN_VERTEX = new Text("VIN");

	// Property-related.
	public static final Text PROPERTY_TYPE = new Text("PROP"); // Property signal
	// Index table stuff.
	public static final Text VERTEX_PROPERTY_LIST = new Text("PVLIST"); // Property lists
	public static final Text EDGE_PROPERTY_LIST = new Text("PELIST"); // Property lists

	// Misc.
	public static final Text EMPTY = new Text();
	public static final Text NULL = null;
	public static final Value EMPTY_VALUE = new Value(new byte[]{});
	public static final Value NULL_VALUE = null;

}
