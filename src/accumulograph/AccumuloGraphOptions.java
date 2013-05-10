package accumulograph;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;

/**
 * Graph options.
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public class AccumuloGraphOptions {

	private Connector connector;
	private Connector mockConnector;
	private String graphTable;
	private String indexTable;
	private boolean mock = false;
	private boolean autoflush = true;
	private boolean returnRemovedPropertyValues = true;

	public boolean getAutoflush() {
		return autoflush;
	}

	/**
	 * Flush changes to Accumulo immediately, to avoid timing
	 * issues on write operations, at the expense of performance.
	 * @param autoflush Autoflush
	 */
	public void setAutoflush(boolean autoflush) {
		this.autoflush = autoflush;
	}

	public boolean getMock() {
		return mock;
	}

	/**
	 * If true, use a mock instance of Accumulo.  Useful for testing
	 * or if you don't have an Accumulo cluster lying around.
	 * @param mock
	 * @throws AccumuloException
	 */
	public void setMock(boolean mock) throws AccumuloException {
		this.mock = mock;

		if (mock && mockConnector == null) {
			try {
				mockConnector = new MockInstance().getConnector("", "");
			} catch (AccumuloSecurityException e) {
				throw new AccumuloException(e);
			}
		}
	}

	/**
	 * Set Accumulo connector information.
	 * @param instance Instance name
	 * @param zookeepers Zookeeper hosts
	 * @param username Username
	 * @param password Password
	 * @throws AccumuloException
	 */
	public void setConnectorInfo(String instance, String zookeepers,
			String username, String password) throws AccumuloException {
		if (instance == null) {
			throw new IllegalArgumentException("Instance cannot be null");
		} else if (zookeepers == null) {
			throw new IllegalArgumentException("Zookeepers cannot be null");
		} else if (username == null) {
			throw new IllegalArgumentException("Username cannot be null");
		} else if (password == null) {
			throw new IllegalArgumentException("Password cannot be null");
		}

		try {
			Instance inst = new ZooKeeperInstance(instance, zookeepers);
			connector = inst.getConnector(username, password);
			
		} catch (AccumuloSecurityException e) {
			throw new AccumuloException(e);
		}
	}

	/**
	 * Use an existing Accumulo connector.
	 * @param connector The connector
	 */
	public void setConnector(Connector connector) {
		if (connector == null) {
			throw new IllegalArgumentException("Connector cannot be null");
		}
		this.connector = connector;
	}

	public Connector getConnector() {
		return mock ? mockConnector : connector;
	}

	public String getGraphTable() {
		return graphTable;
	}

	/**
	 * Set the table in which the graph will be stored.
	 * @param graphTable The table
	 */
	public void setGraphTable(String graphTable) {
		if (graphTable == null) {
			throw new IllegalArgumentException("Graph table cannot be null");
		}
		this.graphTable = graphTable;
	}

	public String getIndexTable() {
		return indexTable;
	}

	/**
	 * Enables key/value indexing, and indicates
	 * what table to store the index.
	 * @param indexTable The index table
	 */
	public void setIndexTable(String indexTable) {
		this.indexTable = indexTable;
	}

	public boolean getReturnRemovedPropertyValues() {
		return returnRemovedPropertyValues;
	}

	/**
	 * If set to false, when removing properties, the
	 * old property value is not returned.  This avoids
	 * another read from Accumulo and increases performance.
	 * @param returnRemovedPropertyValues Return values or not
	 */
	public void setReturnRemovedPropertyValues(boolean returnRemovedPropertyValues) {
		this.returnRemovedPropertyValues = returnRemovedPropertyValues;
	}

}
