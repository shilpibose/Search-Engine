package edu.upenn.cis555.indexer;

import org.apache.log4j.Logger;

public class Counter {
	private final Logger log = Logger.getLogger (Counter.class);
	
	int count;
	
	Counter(){
		count = 0;
	}
	
	public int next(){
		return count++;
		
	}

	public void reset(){
		count = 0;
	}
}
