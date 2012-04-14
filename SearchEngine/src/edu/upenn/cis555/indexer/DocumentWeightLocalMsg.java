package edu.upenn.cis555.indexer;

import rice.p2p.commonapi.NodeHandle;

public class DocumentWeightLocalMsg extends MyMsg{
	
	public DocumentWeightLocalMsg(NodeHandle hfrom) {
		super (hfrom);
	}
	
	DocumentLocal documentLocal;

	public DocumentLocal getDocumentLocal() {
		return documentLocal;
	}

	public void setDocumentLocal(DocumentLocal documentLocal) {
		this.documentLocal = documentLocal;
	}
	
}
