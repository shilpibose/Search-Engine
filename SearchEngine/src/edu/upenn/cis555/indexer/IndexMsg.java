package edu.upenn.cis555.indexer;

import rice.p2p.commonapi.NodeHandle;

public class IndexMsg extends MyMsg {

	public IndexMsg(NodeHandle hfrom) {
		super(hfrom);
	
	}
	LexiconEntryLocal lexiconEntryLocal;//to be removed if u are not using threading

	public LexiconEntryLocal getLexiconEntryLocal() {
		return lexiconEntryLocal;
	}

	public void setLexiconEntryLocal(LexiconEntryLocal lexiconEntryLocal) {
		this.lexiconEntryLocal = lexiconEntryLocal;
	}
	

	
}
