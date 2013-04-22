package accumulograph;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public final class Const {

	private Const() {
		
	}
	
	public static enum Type {META, VERTEX, EDGE, PROP};

	// Vertex-related.
	public static final Text VERTEXTYPE = new Text("MVERTEX");
	public static final Text OUTEDGE = new Text("EOUT");
	public static final Text INEDGE = new Text("EIN");
	
	// Edge-related.
	public static final Text EDGETYPE = new Text("MEDGE");
	public static final Text OUTVERTEX = new Text("VOUT");
	public static final Text INVERTEX = new Text("VIN");
	
	// Element-related.
	public static final Text PROP = new Text("PROP");
	
	public static final Text EMPTY = new Text();
	public static final Text NULL = null;
	public static final Value EMPTYVALUE = new Value(new byte[]{});
	public static final Value NULLVALUE = null;
	
}
