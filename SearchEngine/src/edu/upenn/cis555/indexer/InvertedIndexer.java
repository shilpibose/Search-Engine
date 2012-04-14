package edu.upenn.cis555.indexer;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

public class InvertedIndexer implements Runnable {
	private final Logger log = Logger.getLogger (Index.class);

	Thread t;
	int id;        //id of this thread
	MyStore store;// the database store
	long totalNumDocs;
	
	public InvertedIndexer(MyStore myStore,int id,long totalNumDocs) {
		this.store = myStore;
		this.id = id;
		this.totalNumDocs = totalNumDocs;
		//this.wordsReceived = wordsReceived;
		this.t = new Thread(this, "InvertedIndexer: " + id);
		this.t.start();
	}
	
	InvertedIndexer() {
	}

	@Override
	public void run() {
		
			while(true){
			//	System.out.println("Getting next wordReceived:");
				WordReceived wordReceived = store.getNextWordReceived();
				if(wordReceived == null) {
			//	System.out.println("All words over");
				break;
			}
		  //  System.out.println("WordReceived"+wordReceived.getWordValue());
			String  wordId = wordReceived.getId();
		    String wordstr = wordReceived.getWordValue();
		    store.createLexiconEntry(wordId,wordstr,totalNumDocs);
		  System.out.println("Inverted Index created for word "+wordstr);
		log.info("Inverted Index created for word "+wordstr);
	}
  }
}
