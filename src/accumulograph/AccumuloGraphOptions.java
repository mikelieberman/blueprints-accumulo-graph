package accumulograph;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;

/**
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
	private boolean usePropertyCache = false;

	public boolean getAutoflush() {
		return autoflush;
	}

	public void setAutoflush(boolean autoflush) {
		this.autoflush = autoflush;
	}

	public boolean getMock() {
		return mock;
	}

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

	public void setGraphTable(String graphTable) {
		if (graphTable == null) {
			throw new IllegalArgumentException("Graph table cannot be null");
		}
		this.graphTable = graphTable;
	}

	public String getIndexTable() {
		return indexTable;
	}

	public void setIndexTable(String indexTable) {
		this.indexTable = indexTable;
	}

	public boolean getReturnRemovedPropertyValues() {
		return returnRemovedPropertyValues;
	}

	public void setReturnRemovedPropertyValues(boolean returnRemovedPropertyValues) {
		this.returnRemovedPropertyValues = returnRemovedPropertyValues;
	}

	public boolean getUsePropertyCache() {
		return usePropertyCache;
	}

	public void setUsePropertyCache(boolean usePropertyCache) {
		this.usePropertyCache = usePropertyCache;
	}
	
}
