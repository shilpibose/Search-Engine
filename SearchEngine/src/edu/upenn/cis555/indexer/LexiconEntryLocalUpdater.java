package edu.upenn.cis555.indexer;

import java.util.ArrayList;

public class LexiconEntryLocalUpdater implements Runnable {
	
	Thread t;
	int id;          //id of this thread
	MyStore store; // the database store
	MyApp app;
	
	public LexiconEntryLocalUpdater(MyStore myStore,int id,MyApp app) {
		this.store = myStore;
		this.id = id;
	    this.app = app;
		this.t = new Thread(this, "LexiconEntryUpdater: " + id);
		this.t.start();
	}

	@Override
	public void run() {
		while(true){
			  LexiconEntryLocal lexiconEntryLocal = this.store.getNextLexiconEntryLocal();
			  if(lexiconEntryLocal  == null) {
		  // 		System.out.println("All Lexicon Entry Local Updated !!");
		   		break;
		   	}
			  String wordstr = lexiconEntryLocal.getWord();
				InvertedBarrel invertedBarrel = lexiconEntryLocal.getInvertedBarrel();
				ArrayList<DocumentHits> documentHitsList = invertedBarrel.getDocumentHitsList();
				  for (DocumentHits documentHits : documentHitsList) {
					String documentId = documentHits.getDocumentId();
					int nhits = documentHits.getNhits();
					int documentMaxWordFrequency = store.getDocumentMaxWordFrequency(documentId);  
					double tf = ((double)nhits/documentMaxWordFrequency);
					documentHits.setMaxWordFrequency(documentMaxWordFrequency);
					documentHits.setTf(tf);
					String url = store.getUrl(documentId);
					
			//		System.out.println("\nThe word "+wordstr+" occurs in document  "+url+" "+nhits+"times");
			//		System.out.println("Documet "+url+"  has max word frequency of "+documentMaxWordFrequency);
			//	    System.out.println("The word "+wordstr+" has normalized frequency of "+tf);
				  }
				  store.updateLexiconEntryLocal(lexiconEntryLocal);
				}
	}
		
    public void sendLexiconEntryLocal(LexiconEntryLocal lexiconEntryLocal){
    	String wordId = lexiconEntryLocal.getWordId();
    	Word  word = store.getWord(wordId);
	    String wordstr = word.getWordValue();
    	app.sendLocalIndexMessage(MsgType.PUT_INDEX,wordstr,lexiconEntryLocal);
	    
    }
	

}
