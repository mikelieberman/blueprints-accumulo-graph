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
	
	// For Rexster configuration.
	public static final String PREFIX = "properties";
	public static final String ZOOKEEPERS = PREFIX+".zookeepers";
	public static final String INSTANCE = PREFIX+".instance";
	public static final String USERNAME = PREFIX+".username";
	public static final String PASSWORD = PREFIX+".password";
	public static final String GRAPHTABLE = PREFIX+".graphTable";
	public static final String INDEXTABLE = PREFIX+".indexTable";
	public static final String AUTOFLUSH = PREFIX+".autoflush";
	public static final String MOCK = PREFIX+".mock";
	public static final String RETURNREMOVEDPROPERTYVALUES =
			PREFIX+".returnRemovedPropertyValues";
	public static final String USEPROPERTYCACHE =
			PREFIX+".usePropertyCache";
	
	// Types of Accumulo records.
	public static enum Type {METADATA, VERTEXID, EDGEID, VERTEXPROPERTY, EDGEPROPERTY};

	// Vertex-related.
	public static final Text VERTEXTYPE = new Text("MVERTEX");
	public static final Text OUTEDGE = new Text("EOUT");
	public static final Text INEDGE = new Text("EIN");
	
	// Edge-related.
	public static final Text EDGETYPE = new Text("MEDGE");
	public static final Text OUTVERTEX = new Text("VOUT");
	public static final Text INVERTEX = new Text("VIN");
	
	// Property-related.
	public static final Text PROPERTY = new Text("PROP"); // Property signal
	// Index table stuff.
	public static final Text VERTEXPROPERTYLIST = new Text("PVLIST"); // Property lists
	public static final Text EDGEPROPERTYLIST = new Text("PELIST"); // Property lists
	
	// Misc.
	public static final Text EMPTY = new Text();
	public static final Text NULL = null;
	public static final Value EMPTYVALUE = new Value(new byte[]{});
	public static final Value NULLVALUE = null;
	
}
