package edu.upenn.cis555.indexer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

import edu.upenn.cis555.crawler.bean.Document;

public class DocumentWeightCalculator implements Runnable {

	Thread t;
	int id;        //id of this thread
	MyStore store;// the database store
	
	public DocumentWeightCalculator(MyStore myStore,int id) {
		this.store = myStore;
		this.id = id;
		//this.wordsReceived = wordsReceived;
		this.t = new Thread(this, "InvertedIndexer: " + id);
		this.t.start();
	}
	
	DocumentWeightCalculator() {
	}

	@Override
	public void run() {
		
			while(true){
			//	System.out.println("Getting next wordReceived:");
				Document document  = store.getNextDocument();
				if(document == null) {
			//	System.out.println("All words over");
				break;
			}
		  //  System.out.println("WordReceived"+wordReceived.getWordValue());
			store.calculateDocumentWeight(document);
		
		
	}
  }
}
