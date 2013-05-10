package accumulograph;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

/**
 * This class wraps a BatchWriter and flushes Mutations
 * immediately, rather than letting the BatchWriter
 * decide.  It helps with timing issues.
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public class FlushedBatchWriter implements BatchWriter {

	protected BatchWriter writer;
	
	public FlushedBatchWriter(BatchWriter writer) {
		this.writer = writer;
	}
	
	@Override
	public void addMutation(Mutation m) throws MutationsRejectedException {
		writer.addMutation(m);
		flush();
	}

	@Override
	public void addMutations(Iterable<Mutation> iterable)
			throws MutationsRejectedException {
		for (Mutation m : iterable) {
			addMutation(m);
		}
	}

	@Override
	public void flush() throws MutationsRejectedException {
		writer.flush();
		// TODO Timing issues
		Utils.sleep(1);
	}

	@Override
	public void close() throws MutationsRejectedException {
		writer.close();
	}

}
