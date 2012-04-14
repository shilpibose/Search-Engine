package edu.upenn.cis555.indexer;

import java.io.Serializable;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class WordHits  {

	String wordId;
	int nhits;
	HitsList hitsList;
	
	public String getWordId() {
		return wordId;
	}
	public int getNhits() {
		return nhits;
	}
	public HitsList getHitsList() {
		return hitsList;
	}
	public void setWordId(String wordId) {
		this.wordId = wordId;
	}
	public void setNhits(int nhits) {
		this.nhits = nhits;
	}
	public void setHitsList(HitsList hitsList) {
		this.hitsList = hitsList;
	}
	
}
