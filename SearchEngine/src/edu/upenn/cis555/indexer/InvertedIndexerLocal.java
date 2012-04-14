package edu.upenn.cis555.indexer;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

public class InvertedIndexerLocal implements Runnable {
	private final Logger log = Logger.getLogger (Index.class);

	Thread t;
	int id; // id of this thread
	MyStore store;// the database store
int printCount;
	public InvertedIndexerLocal (MyStore myStore, int id) {
		this.store = myStore;
		this.id = id;
		printCount = 0;
		this.t = new Thread (this, "InvertedIndexerLocal: " + id);
		this.t.start ();
	}

	InvertedIndexerLocal () {
	}

	@Override
	public void run () {
		while (true) {
			// System.out.println("Getting next word:");
			Word word = store.getNextWord ();
			if (word == null) {
				// System.out.println("All words over");
				break;
			}

			String wordId = word.getId ();
			String wordstr = word.getWordValue ();
			store.createLexiconEntryLocal (wordId);
			printCount = printCount +1;
			if(printCount== 100){
				printCount = 0;
			System.out.println ("\nCreated Lexicon Entry Local for word: "
					+ wordstr);
			log.info("\nCreated Lexicon Entry Local for word: "
					+ wordstr);
			}
			}
	}
}
