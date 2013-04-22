package accumulograph;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import accumulograph.Const.Type;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class AccumuloEdge extends AccumuloElement implements Edge {

	protected AccumuloVertex out;
	protected AccumuloVertex in;
	protected String label;
	
	public AccumuloEdge(AccumuloGraph parent, Object id) {
		super(parent, id, Type.EDGE);
		
		parent.scanner.setRange(new Range(row));
		parent.scanner.fetchColumnFamily(Const.EDGETYPE);
		parent.scanner.fetchColumnFamily(Const.OUTVERTEX);
		parent.scanner.fetchColumnFamily(Const.INVERTEX);
		Iterator<Map.Entry<Key, Value>> i = parent.scanner.iterator();
		parent.scanner.clearColumns();
		
		Text cf = new Text();
		Text cq = new Text();
		while (i.hasNext()) {
			Map.Entry<Key, Value> entry = i.next();
			entry.getKey().getColumnFamily(cf);
			entry.getKey().getColumnQualifier(cq);
			
			if (cf.equals(Const.EDGETYPE)) {
				label = cq.toString();
			}
			else if (cf.equals(Const.OUTVERTEX)) {
				out = new AccumuloVertex(parent, Utils.textToEltId(cq));
			}
			else if (cf.equals(Const.INVERTEX)) {
				in = new AccumuloVertex(parent, Utils.textToEltId(cq));
			}
			else {
				throw new RuntimeException("Unexpected CF: "+cf);
			}
		}
	}
	
	public AccumuloEdge(AccumuloGraph parent, Object id,
			AccumuloVertex out, AccumuloVertex in,
			String label) {
		super(parent, id, Type.EDGE);
		this.out = out;
		this.in = in;
		this.label = label;
	}
	
	@Override
	public void setProperty(String key, Object value) {
		if ("label".equals(key)) {
			throw new IllegalArgumentException("Edge property cannot be label.");
		}
		
		super.setProperty(key, value);
	}
	
	@Override
	public Vertex getVertex(Direction direction)
			throws IllegalArgumentException {
		if (direction == Direction.BOTH) {
			throw new IllegalArgumentException();
		}

		return direction == Direction.OUT ? out : in;
	}

	@Override
	public String getLabel() {
		return label;
	}

	@Override
	public void remove() {
		parent.removeEdge(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((in == null) ? 0 : in.hashCode());
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		result = prime * result + ((out == null) ? 0 : out.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		AccumuloEdge other = (AccumuloEdge) obj;
		if (in == null) {
			if (other.in != null)
				return false;
		} else if (!in.equals(other.in))
			return false;
		if (label == null) {
			if (other.label != null)
				return false;
		} else if (!label.equals(other.label))
			return false;
		if (out == null) {
			if (other.out != null)
				return false;
		} else if (!out.equals(other.out))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[E:");
		buffer.append(id);
		for (String prop : getPropertyKeys()) {
			buffer.append(" ");
			buffer.append(prop);
			buffer.append("=");
			buffer.append(getProperty(prop));
		}
		buffer.append("]");
		return buffer.toString();
	}
	
}
