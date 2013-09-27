package accumulograph;

import java.io.InputStream;

import org.apache.commons.configuration.PropertiesConfiguration;

import com.tinkerpop.blueprints.GraphFactory;

import junit.framework.TestCase;

/**
 * @author Michael Lieberman
 */
public class GraphFactoryTest extends TestCase {

	public void testGraphFactory() throws Exception {
		InputStream in = GraphFactoryTest.class.getClassLoader().getResourceAsStream("accumulo.properties");
		PropertiesConfiguration config = new PropertiesConfiguration();
		config.load(in);
		GraphFactory.open(config);
	}
}
