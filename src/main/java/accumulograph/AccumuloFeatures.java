package accumulograph;

import com.tinkerpop.blueprints.Features;

/**
 * @author Mike Lieberman (http://mikelieberman.org)
 */
public class AccumuloFeatures extends Features {

	public AccumuloFeatures() {
		ignoresSuppliedIds = false;
		// TODO The graph is persistent but the unit test fails...
		isPersistent = false;
		isWrapper = false;
		supportsBooleanProperty = true;
		supportsDoubleProperty = true;
		supportsDuplicateEdges = true;
		supportsEdgeIndex = false; // TODO
		supportsEdgeIteration = true;
		supportsEdgeKeyIndex = true;
		supportsEdgeProperties = true;
		supportsEdgeRetrieval = true;
		supportsFloatProperty = true;
		supportsIndices = false; // TODO
		supportsIntegerProperty = true;
		supportsKeyIndices = true;
		supportsLongProperty = true;
		supportsMapProperty = true;
		supportsMixedListProperty = true;
		supportsPrimitiveArrayProperty = true;
		supportsSelfLoops = true;
		supportsSerializableObjectProperty = true;
		supportsStringProperty = true;
		supportsThreadedTransactions = false;
		supportsTransactions = false;
		supportsUniformListProperty = true;
		supportsVertexIndex = false; // TODO
		supportsVertexIteration = true;
		supportsVertexKeyIndex = true;
		supportsVertexProperties = true;
	}
	
}
