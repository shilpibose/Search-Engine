package edu.upenn.cis555.indexer;

import java.io.Serializable;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class LexiconEntry implements Serializable{

	@PrimaryKey
	String wordId;
	String word;
	int ndocs;
	long maxNumDocs;
	double idf;
	InvertedBarrel invertedBarrel;

	public LexiconEntry () {

	}

	public int getNdocs () {
		return ndocs;
	}

	public InvertedBarrel getInvertedBarrel () {
		return invertedBarrel;
	}

	public void setNdocs (int ndocs) {
		this.ndocs = ndocs;
	}

	public void setInvertedBarrel (InvertedBarrel invertedBarrel) {
		this.invertedBarrel = invertedBarrel;
	}

	public String getWordId () {
		return wordId;
	}

	public void setWordId (String wordId) {
		this.wordId = wordId;
	}

	public String getWord () {
		return word;
	}

	public void setWord (String word) {
		this.word = word;
	}

	public long getMaxNumDocs () {
		return maxNumDocs;
	}

	public void setMaxNumDocs (long maxNumDocs) {
		this.maxNumDocs = maxNumDocs;
	}

	public double getIdf () {
		return idf;
	}

	public void setIdf (double idf) {
		this.idf = idf;
	}
}
