package edu.upenn.cis555.indexer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.sleepycat.persist.model.Persistent;
@Persistent
public class ForwardBarrel {
	
	
	ArrayList<WordHits> wordHitsList;
	int maxWordFrequency;
	
	public ArrayList<WordHits> getWordHitsList() {
		return wordHitsList;
	}

	public void setWordHitsList(ArrayList<WordHits> wordHitsList) {
		this.wordHitsList = wordHitsList;
	}

	public ForwardBarrel(){
		wordHitsList = new ArrayList<WordHits>();
	}
	
	public int  size(){
		return wordHitsList.size();
	}
	
	public int getMaxWordFrequency() {
		return maxWordFrequency;
	}

	public void setMaxWordFrequency(int maxWordFrequency) {
		this.maxWordFrequency = maxWordFrequency;
	}
	  
	
}

