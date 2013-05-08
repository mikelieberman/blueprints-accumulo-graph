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
 *       <!-- Whether to use a property cache to speed things up -->
 *       <!-- <usePropertyCache>false</usePropertyCache> -->
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
		Const.GRAPHTABLE,
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
		String graphTable = properties.getString(Const.GRAPHTABLE);
		String indexTable = properties.getString(Const.INDEXTABLE);

		boolean autoflush = properties.getBoolean(Const.AUTOFLUSH, true);
		boolean mock = properties.getBoolean(Const.MOCK, false);
		boolean returnRemovedPropertyValues =
				properties.getBoolean(Const.RETURNREMOVEDPROPERTYVALUES, true);
		boolean usePropertyCache = properties.getBoolean(Const.USEPROPERTYCACHE, false);

		try {
			AccumuloGraphOptions opts = new AccumuloGraphOptions();
			opts.setConnectorInfo(instance, zookeepers, username, password);
			opts.setGraphTable(graphTable);
			opts.setIndexTable(indexTable);
			opts.setAutoflush(autoflush);
			opts.setMock(mock);
			opts.setReturnRemovedPropertyValues(returnRemovedPropertyValues);
			opts.setUsePropertyCache(usePropertyCache);

			return new AccumuloGraph(opts);

		} catch (AccumuloException e) {
			throw new GraphConfigurationException(e);
		}
	}

}
