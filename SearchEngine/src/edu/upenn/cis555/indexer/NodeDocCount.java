package edu.upenn.cis555.indexer;
import rice.p2p.commonapi.NodeHandle;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
@Entity
public class NodeDocCount {


@PrimaryKey(sequence = "DocumentCountSequence")
private int id;
//private NodeHandle hfrom;
private String nodeHandle;
private int numDocs;

public int getNumDocs() {
	return numDocs;
}
public void setNumDocs(int numDocs) {
	this.numDocs = numDocs;
}
public String getNodeHandle() {
	return nodeHandle;
}
public void setNodeHandle(String nodeHandle) {
	this.nodeHandle = nodeHandle;
}

}
