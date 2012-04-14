package edu.upenn.cis555.indexer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;

import com.sleepycat.persist.model.Persistent;
@Persistent
public class HitsList implements Serializable {

  ArrayList<Hit> hits;
  
  public HitsList(){
	  hits = new ArrayList<Hit>();
  }

public ArrayList<Hit> getHits() {
	return hits;
}

public void setHits(ArrayList<Hit> hits) {
	this.hits = hits;
}
 
public  int size(){
	return hits.size();
}
  
}
