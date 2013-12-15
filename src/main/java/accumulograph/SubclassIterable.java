package accumulograph;

import java.util.Iterator;

/**
 * @author Michael Lieberman
 */
public class SubclassIterable<T> implements Iterable<T> {

	private Iterable<T> inner;

	@SuppressWarnings("unchecked")
	public SubclassIterable(Iterable<? extends T> iterable) {
		this.inner = (Iterable<T>) iterable;
	}

	@Override
	public Iterator<T> iterator() {
		return inner.iterator();
	}

}
