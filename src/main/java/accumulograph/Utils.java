package accumulograph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Various utility methods.
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public final class Utils {

	private Utils() {

	}

	public static <T> byte[] toBytes(T obj) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(obj);
			oos.close();
			return baos.toByteArray();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T fromBytes(byte[] bytes) {
		try {
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
			@SuppressWarnings("unchecked")
			T obj = (T) ois.readObject();
			ois.close();
			return obj;

		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> Value objectToValue(T obj) {
		return new Value(toBytes(obj));
	}

	public static <T> T valueToObject(Value value) {
		return fromBytes(value.get());
	}
	
	public static Text elementIdToText(AccumuloElementId id) {
		return id.toText();
	}
	
	public static AccumuloElementId textToElementId(Text text) {
		return new AccumuloElementId(text);
	}

	public static Value textToValue(Text text) {
		return new Value(text.getBytes());
	}

	public static Text valueToText(Value value) {
		return new Text(value.get());
	}

	public static Text stringToText(String str) {
		return new Text(str);
	}

	public static String textToString(Text text) {
		return text.toString();
	}

	public static Value stringToValue(String str) {
		return new Value(str.getBytes());
	}

	public static String valueToString(Value value) {
		return new String(value.get());
	}

	public static <T> Text objectToText(T obj) {
		return new Text(toBytes(obj));
	}

	public static <T> T textToObject(Text text) {
		return fromBytes(text.getBytes());
	}

	public static void addMutation(BatchWriter writer, Mutation mut) {
		addMutation(writer, mut, 0L);
	}

	public static void addMutation(BatchWriter writer, Mutation mut, long sleep) {
		try {
			writer.addMutation(mut);
			Thread.sleep(sleep); // Needed for timing issues
		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public static void flush(BatchWriter writer) {
		try {
			writer.flush();
		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		}
	}

	public static Map.Entry<Key, Value> firstEntry(Scanner scanner) {
		Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
		return i.hasNext() ? i.next() : null;
	}

	public static void deleteAllEntries(Scanner scanner, BatchWriter writer) {
		Text row = new Text();
		Text cf = new Text();
		Text cq = new Text();

		for (Map.Entry<Key, Value> entry : scanner) {
			entry.getKey().getRow(row);
			entry.getKey().getColumnFamily(cf);
			entry.getKey().getColumnQualifier(cq);

			Mutation m = new Mutation(row);
			m.putDelete(cf, cq);
			addMutation(writer, m);
		}
	}

	public static void createTableIfNotExists(Connector conn, String table) throws AccumuloException, AccumuloSecurityException, TableExistsException {
		// Check whether table exists already and create if not.
		TableOperations ops = conn.tableOperations();
		if (!ops.exists(table)) {
			ops.create(table);
		}
	}

	public static void recreateTable(Connector conn, String table)
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
		TableOperations ops = conn.tableOperations();

		if (ops.exists(table)) {
			ops.delete(table);
		}

		ops.create(table);
	}

	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
