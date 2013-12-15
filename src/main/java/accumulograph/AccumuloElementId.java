package accumulograph;

import java.util.UUID;

import org.apache.hadoop.io.Text;

/**
 * Represents an element (v/e) id in the graph.
 * Essentially a thin wrapper around a string.
 * @author Michael Lieberman
 */
public class AccumuloElementId {

	protected final String id;

	public AccumuloElementId() {
		this(null);
	}

	public AccumuloElementId(Object id) {
		if (id == null) {
			id = UUID.randomUUID();
		}

		this.id = id.toString();
	}

	public Text toText() {
		return new Text(id);
	}

	@Override
	public String toString() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AccumuloElementId other = (AccumuloElementId) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

}
