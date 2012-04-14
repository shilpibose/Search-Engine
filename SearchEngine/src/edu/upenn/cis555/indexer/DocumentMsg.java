package edu.upenn.cis555.indexer;


import org.apache.log4j.Logger;

import rice.p2p.commonapi.NodeHandle;

public class DocumentMsg extends MyMsg {
	
	public DocumentMsg(NodeHandle hfrom) {
		super(hfrom);
	}

	int numDocs;

	public int getNumDocs() {
		return numDocs;
	}

	public void setNumDocs(int numDocs) {
		this.numDocs = numDocs;
	}

	
}
