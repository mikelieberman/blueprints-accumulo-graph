package accumulograph;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

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
	}

	@Override
	public void close() throws MutationsRejectedException {
		writer.close();
	}

}
