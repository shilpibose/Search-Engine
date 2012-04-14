package edu.upenn.cis555.indexer;

import java.util.ArrayList;

import org.apache.log4j.Logger;

public class WordDocumentWeightMaker implements Runnable {
	private final Logger log = Logger.getLogger (Index.class);

	Thread t;
	int id;          //id of this thread
	MyStore store; // the database store
	MyApp app;
	int count;
	int printCount ;
	public WordDocumentWeightMaker(MyStore myStore,int id,MyApp app) {
		this.store = myStore;
		this.id = id;
	    this.app = app;
	    printCount = 0;
		this.t = new Thread(this, "WordDocumentWeightMaker: " + id);
		this.t.start();
	    count = 0;
	}

	
	
	@Override
	public void run() {
		while(true){
			  LexiconEntry lexiconEntry = this.store.getNextLexiconEntry();
			if(lexiconEntry  == null) {
		//   		System.out.println("All Lexicon Entry Local send !!");
		   		break;
		   	}
			String wordstr = lexiconEntry.getWord();  
			
			createWordDocumentWeights(lexiconEntry);
		 printCount = printCount + 1;
			if(printCount == 100){
			printCount = 0;
			   System.out.println("Created word documentweight entries for word: "+wordstr);
		   log.info("Created word documentweight entries for word: "+wordstr);
		   }
		   }
	}
 	
	public  void createWordDocumentWeights (LexiconEntry lexiconEntry) {
		InvertedBarrel invertedBarrel = lexiconEntry.getInvertedBarrel ();
		ArrayList<DocumentHits> documnetHitsList = invertedBarrel
				.getDocumentHitsList ();
		for (DocumentHits documentHits : documnetHitsList) {
			String documentId = documentHits.getDocumentId();
			double weight = documentHits.getWeight ();
			String url = documentHits.getUrl();
		    WordDocumentWeight wordDocumentWeight = new WordDocumentWeight();
		    wordDocumentWeight.setDocumentFk(documentId);
		    wordDocumentWeight.setWeight(weight);
		    wordDocumentWeight.setUrl(url);
		    
		    store.putWordDocumentWeight(wordDocumentWeight);
		}
    }


}


/*

	        count ++;
			if(count == 50){    // changed the value...as it was dropping packets...
				count = 0;

    try {
				Thread.currentThread().sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		
*/