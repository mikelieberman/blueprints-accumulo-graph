package accumulograph;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import accumulograph.Const.Type;

import com.tinkerpop.blueprints.Element;

/**
 * Element implementation. This provides hooks for
 * setting/removing properties.
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public abstract class AccumuloElement implements Element {

	protected AccumuloGraph parent;
	protected Type type;
	protected Object id;
	protected Text idRow;

	protected AccumuloElement(AccumuloGraph parent,
			Object id, Type type) {
		this.parent = parent;
		this.type = type;
		
		if (id == null) {
			id = Utils.makeId();
		}
		
		this.id = id;
		this.idRow = Utils.typedObjectToText(type, id);
	}

	@Override
	public <T> T getProperty(String key) {
		parent.scanner.setRange(new Range(idRow));
		parent.scanner.fetchColumn(Const.PROPERTY, Utils.stringToText(key));
		Map.Entry<Key, Value> entry = Utils.firstEntry(parent.scanner);
		parent.scanner.clearColumns();
		return entry != null ? Utils.<T>valueToObject(entry.getValue()) : null;
	}

	@Override
	public Set<String> getPropertyKeys() {
		Set<String> keys = new HashSet<String>();

		parent.scanner.setRange(new Range(idRow));
		parent.scanner.fetchColumnFamily(Const.PROPERTY);

		for (Map.Entry<Key, Value> entry : parent.scanner) {
			keys.add(Utils.textToString(entry.getKey().getColumnQualifier()));
		}

		parent.scanner.clearColumns();

		return keys;
	}

	@Override
	public void setProperty(String key, Object value) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null.");
		}
		else if ("".equals(key)) {
			throw new IllegalArgumentException("Key cannot be empty.");
		}
		else if ("id".equals(key)) {
			throw new IllegalArgumentException("Key cannot be id.");
		}
		else if (value == null) {
			throw new IllegalArgumentException("Value cannot be null.");
		}
		
		Mutation m = new Mutation(idRow);
		m.put(Const.PROPERTY, Utils.stringToText(key),
				Utils.objectToValue(value));
		Utils.addMutation(parent.writer, m);
		
		if (parent.keyIndex != null) {
			parent.keyIndex.addPropertyToIndex(this, key, value);
		}
	}

	@Override
	public <T> T removeProperty(String key) {
		T old = null;
		
		if (parent.opts.getReturnRemovedPropertyValues()) {
			old = getProperty(key);
		}

		Mutation m = new Mutation(idRow);
		m.putDelete(Const.PROPERTY, Utils.stringToText(key));
		Utils.addMutation(parent.writer, m);
		
		if (parent.keyIndex != null) {
			parent.keyIndex.removePropertyFromIndex(this, key);
		}

		return old;
	}

	@Override
	public Object getId() {
		return id;
	}

	protected Text getIdRow() {
		return idRow;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((idRow == null) ? 0 : idRow.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		AccumuloElement other = (AccumuloElement) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (idRow == null) {
			if (other.idRow != null)
				return false;
		} else if (!idRow.equals(other.idRow))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	
}
