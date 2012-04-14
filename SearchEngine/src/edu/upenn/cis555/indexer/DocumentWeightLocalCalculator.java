package edu.upenn.cis555.indexer;

import java.util.ArrayList;

public class DocumentWeightLocalCalculator implements Runnable {
	
	Thread t;
	int id;          //id of this thread
	MyStore store; // the database store
	MyApp app;
	int count;
	public DocumentWeightLocalCalculator(MyStore myStore,int id,MyApp app) {
		this.store = myStore;
		this.id = id;
	    this.app = app;
		this.t = new Thread(this, "DocumentWeightLocalCalculator: " + id);
		this.t.start();
	    count = 0;
	}

	
	
	@Override
	public void run() {
		while(true){
			  DocumentLocal documentLocal  = this.store.getNextDocumentLocal();
			  
			if(documentLocal == null) {
		//   		System.out.println("All Lexicon Entry Local send !!");
		   		break;
		   	}
			store.createDocumentWeightLocal(documentLocal);
			
			
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