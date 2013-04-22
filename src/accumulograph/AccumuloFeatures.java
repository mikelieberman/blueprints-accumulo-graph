package accumulograph;

import com.tinkerpop.blueprints.Features;

public class AccumuloFeatures extends Features {

	public AccumuloFeatures() {
		ignoresSuppliedIds = false;
		isRDFModel = false;
		// TODO The graph is persistent but the unit test fails...
		isPersistent = false;
		isWrapper = false;
		supportsBooleanProperty = true;
		supportsDoubleProperty = true;
		supportsDuplicateEdges = true;
		supportsEdgeIndex = false; // TODO
		supportsEdgeIteration = true;
		supportsEdgeKeyIndex = false; // TODO
		supportsEdgeProperties = true;
		supportsEdgeRetrieval = true;
		supportsFloatProperty = true;
		supportsIndices = false; // TODO
		supportsIntegerProperty = true;
		supportsKeyIndices = false; // TODO
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
		supportsVertexKeyIndex = false; // TODO
		supportsVertexProperties = true;
	}
	
}
