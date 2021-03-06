package accumulograph;

import org.apache.accumulo.core.client.AccumuloException;
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
 *       <graphTable>blueprints</graphTable>
 *
 *       <!-- ======== OPTIONAL ======== -->
 *       <!-- Index table -->
 *       <!-- <indexTable>blueprintsIndex</indexTable> -->
 *       <!-- Whether to flush changes immediately -->
 *       <!-- <autoflush>true</autoflush> -->
 *       <!-- Whether to use mock instance for testing -->
 *       <!-- <mock>false</mock> -->
 *       <!-- Whether to return values for properties that are removed. This can slow things down -->
 *       <!-- <returnRemovedPropertyValues>true</returnRemovedPropertyValues> -->
 *     </properties>
 *   </graph>
 * }
 * </pre>
 *   
 * @author Mike Lieberman (http://mikelieberman.org)
 *
 */
public class AccumuloGraphConfiguration implements GraphConfiguration {

	protected static final String[] REQUIRED_PROPERTIES = new String[]{
		Const.INSTANCE, Const.ZOOKEEPERS,
		Const.USERNAME, Const.PASSWORD,
		Const.GRAPH_TABLE,
	};

	@Override
	public Graph configureGraphInstance(Configuration properties)
			throws GraphConfigurationException {
		try {
			AccumuloGraphOptions opts = parseProperties(
					properties, Const.REXSTER_PREFIX);
			return new AccumuloGraph(opts);

		} catch (AccumuloException e) {
			throw new GraphConfigurationException(e);
		}
	}

	static AccumuloGraphOptions parseProperties(Configuration properties, String prefix) throws AccumuloException {
		// Strip out the prefix.
		properties = properties.subset(prefix);

		for (String prop : REQUIRED_PROPERTIES) {
			if (!properties.containsKey(prop)) {
				throw new IllegalArgumentException("Missing configuration property: "+prop);
			}
		}

		String zookeepers = properties.getString(Const.ZOOKEEPERS);
		String instance = properties.getString(Const.INSTANCE);
		String username = properties.getString(Const.USERNAME);
		String password = properties.getString(Const.PASSWORD);
		String graphTable = properties.getString(Const.GRAPH_TABLE);
		String indexTable = properties.getString(Const.INDEX_TABLE);

		boolean autoflush = properties.getBoolean(Const.AUTOFLUSH, true);
		boolean mock = properties.getBoolean(Const.MOCK, false);
		boolean returnRemovedPropertyValues =
				properties.getBoolean(Const.RETURN_REMOVED_PROPERTY_VALUES, true);		

		AccumuloGraphOptions opts = new AccumuloGraphOptions();
		opts.setConnectorInfo(instance, zookeepers, username, password);
		opts.setGraphTable(graphTable);
		opts.setIndexTable(indexTable);
		opts.setAutoflush(autoflush);
		opts.setMock(mock);
		opts.setReturnRemovedPropertyValues(returnRemovedPropertyValues);

		return opts;
	}

}
