package accumulograph;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.configuration.Configuration;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationException;

/**
 * Rexster configuration class for AccumuloGraph.
 * 
 * Sample configuration:
 * 
 * <pre>
 * {@code
 *   <graph>
 *     <graph-enabled>true</graph-enabled>
 *     <graph-name>AccumuloGraph</graph-name>
 *     <graph-type>accumulograph.AccumuloGraphConfiguration</graph-type>
 *     <properties>
 *       <!-- Accumulo instance name -->
 *       <instance>accumulo</instance>
 *       <!-- Zookeeper hosts -->
 *       <zookeepers>localhost</zookeepers>
 *       <!-- Accumulo login info -->
 *       <username>root</username>
 *       <password>secret</password>
 *       <!-- Accumulo table to store the graph -->
 *       <table>blueprints</table>
 *
 *       <!-- ======== OPTIONAL ======== -->
 *       <!-- Whether to flush changes immediately -->
 *       <!-- <autoflush>true</autoflush> -->
 *       <!-- Whether to use mock instance for testing -->
 *       <!-- <mock>false</mock> -->
 *     </properties>
 *   </graph>
 * }
 * </pre>
 *   
 * @author Mike Lieberman (http://mikelieberman.org)
 *
 */
public class AccumuloGraphConfiguration implements GraphConfiguration {

	protected static final String[] REQPROPS = new String[]{
		Const.INSTANCE, Const.ZOOKEEPERS,
		Const.USERNAME, Const.PASSWORD,
		Const.TABLE,
	};

	@Override
	public Graph configureGraphInstance(Configuration properties)
			throws GraphConfigurationException {
		for (String prop : REQPROPS) {
			if (!properties.containsKey(prop)) {
				throw new GraphConfigurationException("Missing configuration property: "+prop);
			}
		}

		String zookeepers = properties.getString(Const.ZOOKEEPERS);
		String instance = properties.getString(Const.INSTANCE);
		String username = properties.getString(Const.USERNAME);
		String password = properties.getString(Const.PASSWORD);
		String table = properties.getString(Const.TABLE);
		boolean autoflush = properties.getBoolean(Const.AUTOFLUSH, true);
		boolean mock = properties.getBoolean(Const.MOCK, false);

		AccumuloGraph graph;

		try {
			if (mock) {
				Instance inst = new MockInstance(instance);
				Connector conn = inst.getConnector("", "");
				graph = new AccumuloGraph(conn, table, autoflush);
			}
			else {
				Instance inst = new ZooKeeperInstance(instance, zookeepers);
				Connector conn = inst.getConnector(username, password);
				graph = new AccumuloGraph(conn, table, autoflush);
			}

		} catch (AccumuloException e) {
			throw new GraphConfigurationException(e);
		} catch (AccumuloSecurityException e) {
			throw new GraphConfigurationException(e);
		}

		return graph;
	}

}
