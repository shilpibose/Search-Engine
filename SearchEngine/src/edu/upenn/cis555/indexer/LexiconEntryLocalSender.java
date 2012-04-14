package edu.upenn.cis555.indexer;

import org.apache.log4j.Logger;


public class LexiconEntryLocalSender implements Runnable {
	private final Logger log = Logger.getLogger (LexiconEntryLocalSender.class);
	
	Thread t;
	int id;          //id of this thread
	MyStore store; // the database store
	MyApp app;
	int count;
	public LexiconEntryLocalSender(MyStore myStore,int id,MyApp app) {
		this.store = myStore;
		this.id = id;
	    this.app = app;
		this.t = new Thread(this, "LexiconEntrySender: " + id);
		this.t.start();
	    count = 0;
	}

	
	
	@Override
	public void run() {
		while(true){
			  LexiconEntryLocal lexiconEntryLocal = this.store.getNextLexiconEntryLocal();
			  
			if(lexiconEntryLocal  == null) {
		//   		System.out.println("All Lexicon Entry Local send !!");
		   		break;
		   	}
			sendLexiconEntryLocal(lexiconEntryLocal);
	    	 
		}
	}
	
	
    public void sendLexiconEntryLocal(LexiconEntryLocal lexiconEntryLocal){
    	String wordId = lexiconEntryLocal.getWordId();
    	Word  word = store.getWord(wordId);
	    String wordstr = word.getWordValue();
    	app.sendLocalIndexMessage(MsgType.PUT_INDEX,wordstr,lexiconEntryLocal);
      
        try {
			Thread.currentThread().sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
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
