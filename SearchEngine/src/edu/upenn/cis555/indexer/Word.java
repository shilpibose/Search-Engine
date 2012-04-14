package edu.upenn.cis555.indexer;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Word {

	@PrimaryKey//(sequence = "WordSequence")
	private String id;
	private String wordValue;
	public String getId() {
		return id;
	}
	public String getWordValue() {
		return wordValue;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setWordValue(String word) {
		this.wordValue = word;
	}
	
	
}
