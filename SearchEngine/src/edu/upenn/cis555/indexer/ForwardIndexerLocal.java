package edu.upenn.cis555.indexer;

import edu.upenn.cis555.crawler.bean.Document;


public class ForwardIndexerLocal implements Runnable {

	Thread t;
	int id; // id of this thread
	MyStore store;// the database store

	public ForwardIndexerLocal (MyStore myStore, int id) {
		this.store = myStore;
		this.id = id;
		this.t = new Thread (this, "ForwardIndexerLocal: " + id);
		this.t.start ();
	}

	ForwardIndexerLocal () {
	}

	@Override
	public void run () {
		while (true) {
		//	System.out.println ("Getting next Document:");

			Document document = store.getNextDocument ();
			if (document == null) {
			//	System.out.println ("All wdocuments over");
				break;
			}

			String documentId = document.getId ();
			String url = document.getUrl ();
			String type = document.getContentType ();
			store.createForwardIndexEntry (documentId);
		}
	}
}
