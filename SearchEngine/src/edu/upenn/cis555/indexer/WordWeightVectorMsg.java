package edu.upenn.cis555.indexer;

import rice.p2p.commonapi.NodeHandle;

public class WordWeightVectorMsg extends MyMsg {

	String searchQueryId;
	String wordSearchTerm;
	LexiconEntry lexiconEntry;

	public WordWeightVectorMsg (NodeHandle hfrom) {
		super (hfrom);
	}

	public String getSearchQueryId () {
		return searchQueryId;
	}

	public void setSearchQueryId (String queryId) {
		this.searchQueryId = queryId;
	}

	public String getWordSearchTerm () {
		return wordSearchTerm;
	}

	public void setWordSearchTerm (String wordSearchTerm) {
		this.wordSearchTerm = wordSearchTerm;
	}

	public LexiconEntry getLexiconEntry () {
		return lexiconEntry;
	}

	public void setLexiconEntry (LexiconEntry lexiconEntry) {
		this.lexiconEntry = lexiconEntry;
	}

}
