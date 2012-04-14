package edu.upenn.cis555.indexer;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import rice.pastry.PastryNode;

import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.node.NodeManager;
import edu.upenn.cis555.node.utils.NodeUtils;

/**
 * The index class launches a bunch of indexer threads. Each thread picks up a
 * document from the databse and indexes it.
 * 
 * @author Rahul
 * 
 */

public class Index {
	private final Logger log = Logger.getLogger (Index.class);

	MyStore myStore;
	MyApp app;
	PastryNode node;
	EntityStore entityStore;
	boolean quitLocalIndexing;
	Indexer[] indexer;
	int numThreads;
	Thread t;
	int id;
	long indexStartTime;
	long indexEndTime;

	public Index (EntityStore entityStore, PastryNode node) {
		this.entityStore = entityStore;
		this.node = node;
		numThreads = 0;
		myStore = new MyStore (entityStore); // "./export/dbEnv"
		myStore.initiailize ();
		app = new MyApp (node, myStore);

		// statistics();
	}

	public void statistics () {
		System.out.println("Starting to print statistics files");
		log.info("Starting to print statistics files");
		myStore.printAllDocumnets ();
		myStore.printAllDocumentsLocalReceived ();
		myStore.printAllDocumentsLocal ();
		myStore.printAllWordDocumentWeight ();
		myStore.printInvertedIndex ();
		myStore.printAllWordReceived ();
		myStore.printAllWords ();
		myStore.printAllLexiconEntries ();
		myStore.printAllWordDocumentsHits ();
		log.info("Print statistics files over");
		
		System.out.println("Print statistics files over");
		
	}

	public int getNumThreads () {
		return numThreads;
	}

	public void setNumThreads (int numThreads) {
		this.numThreads = numThreads;
	}

	public void startLocalIndexing () {
		indexStartTime = System.currentTimeMillis ();
		initialize ();
		createIndex ();
	}

	public void startGlobalIndexing () {
		createInvertedIndex ();
		createWordDocumentWeights ();
		myStore.createDocumentLocal ();
		calculateLocalDocumentWeights ();
		sendDocumentWeightlocal ();
		// myStore.printAllWordReceived();
		// myStore.printInvertedIndex ();

	}

	public void startDocumentWeightCalculation () {
		calculateDocumentWeights ();
     //	statistics();
	}

	public void quitLocalIndexing () {
		System.out.println ("Got command to quit parsing documents");
		log.info ("Got command to quit parsing documents");
		int num = getNumThreads ();

		System.out.println ("Num of threads:" + num);
		for (int i = 0; i < num; i++) {
			System.out.println ("Giving quit command to thread " + i);
			indexer[i].quit ();
			if (indexer[i].t.getState () == Thread.State.WAITING) {
				System.out.println ("Interrupting thread " + i);
				indexer[i].t.interrupt ();
			}
		}

		for (int i = 0; i < num; i++) {
			try {
				(indexer[i]).t.join ();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println ("Interrupted exception in join");
				e.printStackTrace ();
			}
		}
		int numOfDocParsed = 0;
		for (int i = 0; i < num; i++) {
			numOfDocParsed = numOfDocParsed + indexer[i].getNumDocsParsed ();
		}

		System.out.println ("Parsing documents over!!!!");
		log.info ("Parsing documents over!!!!");
		int size1 = myStore.sizeWords ();
		System.out.println ("Words size " + size1);
		log.info ("Words size " + size1);

		int size2 = myStore.sizeDocuments ();
		System.out.println ("Documents size " + size2);
		log.info ("Documents size " + size2);
		int size = myStore.sizeWordDocumentHit ();
		System.out.println ("Documents parsed :" + numOfDocParsed);
		log.info ("Documents parsed :" + numOfDocParsed);
		System.out.println ("WordDocumentHit size " + size);
		log.info ("WordDocumentHit size " + size);
		try {
			Thread.currentThread ().sleep (1000 * 5);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		// myStore.printAllWords ();
		// myStore.printAllWordDocuments ();
		// myStore.printAllDocumnets ();

		// createForwardIndex ();
		// myStore.printForwardIndexEntries ();

		createLocalInvertedIndex ();
		// myStore.printAllWords();
		// myStore.printAllLexiconEntries ();

		// updateLocalInvertedIndex ();
		// myStore.printUpdatedLexiconLocal ();

		sendLocalInvertedIndex ();

	}

	public void initialize () {
		myStore.deleteAllWords ();

		myStore.deleteAllWordDocumentHits ();
		myStore.deleteAllLexiconEntriesLocal ();

		// myStore.deleteAllForwardIndexEntries ();
		myStore.deleteAllWordsReceived ();
		myStore.deleteAllLexiconEntries ();

	}

	public void createIndex () {
		int num = Integer.parseInt (NodeUtils.getInstance ().getValue (
				Constants.NUM_INDEXER_THREADS));
		setNumThreads (num);
		System.out.println ("Starting to index...");
		log.info ("Starting to index...");
		indexer = new Indexer[numThreads];

		for (int i = 0; i < numThreads; i++) {
			indexer[i] = new Indexer (myStore, i);
		}
	}

	public void createLocalInvertedIndex () {
		myStore.initializeEntityCursorWord ();

		System.out.println ("Word Cursor initialized");
		System.out.println ("Starting to create Local Inverted Index....");
		log.info ("Starting to create Local Inverted Index....");
		long t1 = System.currentTimeMillis ();

		int num = getNumThreads ();
		InvertedIndexerLocal[] invertedIndexerLocal = new InvertedIndexerLocal[num];
		for (int i = 0; i < num; i++) {
			invertedIndexerLocal[i] = new InvertedIndexerLocal (myStore, i);
		}

		for (int i = 0; i < num; i++) {
			try {
				(invertedIndexerLocal[i]).t.join ();
			} catch (InterruptedException e) {
				e.printStackTrace ();
			}
		}
		myStore.closeEntityCursorWord ();
		long t2 = System.currentTimeMillis ();
		long t3 = (t2 - t1);
		System.out
				.println ("\n Inverted Index Local created !!!! Time taken to create inverted index Local :"
						+ (t3 / 1000) + "  secondss");
		log
				.info ("\n Inverted Index Local created !!!! Time taken to create inverted index Local :"
						+ (t3 / 1000) + "  secondss");

		int size1 = myStore.sizeLexiconEntryLocal ();
		System.out.println ("LexiconEntryLocal size " + size1);
		log.info ("LexiconEntryLocal size " + size1);
		try {
			Thread.sleep (5000);
		} catch (InterruptedException e) {
			e.printStackTrace ();
		}
	}

	public void createForwardIndex () {
		System.out.println ("Starting to create Forward Index....");

		long time1 = System.currentTimeMillis ();
		myStore.initializeEntityCursorDocument ();

		int num = getNumThreads ();
		ForwardIndexerLocal[] forwardIndexerLocal = new ForwardIndexerLocal[num];

		for (int i = 0; i < num; i++) {
			forwardIndexerLocal[i] = new ForwardIndexerLocal (myStore, i);
		}

		for (int i = 0; i < num; i++) {
			try {
				(forwardIndexerLocal[i]).t.join ();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace ();
			}
		}

		myStore.closeEntityCursorDocument ();

		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / 1000;
		System.out.println ("\nCreating Forward Index over !!!!");
		System.out
				.println ("\nTime taken to create Forward Index entries in  seconds"
						+ diffTime);
	}

	public void updateLocalInvertedIndex () {
		System.out.println ("Starting to update Local Inverted Index....");

		long time1 = System.currentTimeMillis ();
		myStore.initializeEntityCursorLexiconEntryLocal ();

		int num = getNumThreads ();
		LexiconEntryLocalUpdater[] lexiconEntryLocalUpdater = new LexiconEntryLocalUpdater[num];

		for (int i = 0; i < num; i++) {
			lexiconEntryLocalUpdater[i] = new LexiconEntryLocalUpdater (
					myStore, i, app);
		}

		for (int i = 0; i < num; i++) {
			try {
				(lexiconEntryLocalUpdater[i]).t.join ();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace ();
			}
		}

		myStore.closeEntityCursorLexiconEntryLocal ();

		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000);
		System.out.println ("\nUpdating lexicon entries over !!!!");
		System.out.println ("\nTime taken to update lexicon entries in seconds"
				+ diffTime);
	}

	/**
	 * 
	 * This function is used to send the local inverted index to appropriate
	 * pastry node. A single message is send for each wordid as the key and its
	 * inverted barrel as value.
	 */
	public void sendLocalInvertedIndex () {
		System.out.println ("Starting to send Local Inverted Index....");
		log.info ("Starting to send Local Inverted Index....");
		long time1 = System.currentTimeMillis ();
		myStore.initializeEntityCursorLexiconEntryLocal ();

		int num; // = getNumThreads ();
		num = 1;

		LexiconEntryLocalSender[] lexiconEntrySender = new LexiconEntryLocalSender[num];

		for (int i = 0; i < num; i++) {
			lexiconEntrySender[i] = new LexiconEntryLocalSender (myStore, i,
					app);
		}

		for (int i = 0; i < num; i++) {
			try {
				(lexiconEntrySender[i]).t.join ();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace ();
			}
		}

		myStore.closeEntityCursorLexiconEntryLocal ();

		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000);
		System.out.println ("\nSending lexicon entries over !!!!");
		log.info ("\nSending lexicon entries over !!!!");
		System.out.println ("\nTime taken to send lexicon entries in seconds"
				+ diffTime);
		log.info ("\nTime taken to send lexicon entries in seconds" + diffTime);
		System.out
				.println ("Waiting for a minute so that local inverted indexes are received by all nodes.....");
		try {
			Thread.currentThread ().sleep (1000 * 10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
	}

	public void sendDocumentWeightlocal () {
		System.out.println ("Starting to send Document Weight Local....");
		log.info ("Starting to send Document Weight Local....");
		long time1 = System.currentTimeMillis ();
		myStore.initializeEntityCursorDocumentLocal ();

		int num; // = getNumThreads ();
		num = 1;

		WordDocumentWeightSender[] wordDocumentWeightSender = new WordDocumentWeightSender[num];

		for (int i = 0; i < num; i++) {
			wordDocumentWeightSender[i] = new WordDocumentWeightSender (
					myStore, i, app);
		}

		for (int i = 0; i < num; i++) {
			try {
				(wordDocumentWeightSender[i]).t.join ();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace ();
			}
		}

		myStore.closeEntityCursorDocumentLocal ();

		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000);
		System.out.println ("\nSending document weight local over !!!!");
		log.info ("\nSending document weight local over !!!!");
		System.out.println ("\nTime taken to send lexicon entries in seconds"
				+ diffTime);
		log.info ("\nTime taken to send lexicon entries in seconds" + diffTime);
		System.out
				.println ("Waiting for a minute so that document weight local are received by all nodes.....");

		try {
			Thread.currentThread ().sleep (1000 * 60);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
	}

	public void createWordDocumentWeights () {
		System.out.println ("Starting to make Word Document Weight entries...");
		log.info ("Starting to make Word Document Weight entries...");
		long time1 = System.currentTimeMillis ();
		myStore.initializeEntityCursorLexiconEntry ();

		int num = getNumThreads ();

		WordDocumentWeightMaker[] wordDocumentWeightMaker = new WordDocumentWeightMaker[num];

		for (int i = 0; i < num; i++) {
			wordDocumentWeightMaker[i] = new WordDocumentWeightMaker (myStore,
					i, app);
		}

		for (int i = 0; i < num; i++) {
			try {
				(wordDocumentWeightMaker[i]).t.join ();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace ();
			}
		}

		myStore.closeEntityCursorLexiconEntry ();

		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000);
		System.out
				.println ("\n Making Word Document Weight  entries over !!!!");
		log.info ("\n Making Word Document Weight  entries over !!!!");
		System.out
				.println ("\n Time taken to make word document weight entries in seconds"
						+ diffTime);
		log
				.info ("\n Time taken to make word document weight entries in seconds"
						+ diffTime);
		try {
			Thread.currentThread ().sleep (1000 * 5);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
	}

	public void calculateLocalDocumentWeights () {
		myStore.initializeEntityCursorDocumentLocal ();
		System.out.println ("DocumentLocalCurosr initialized");
		System.out
				.println ("Starting to calculate document local weights.....");
		log.info ("Starting to calculate document local weights.....");
		long t1 = System.currentTimeMillis ();
		int num = getNumThreads ();
		DocumentWeightLocalCalculator[] documentWeightLocalCalculator = new DocumentWeightLocalCalculator[num];

		for (int i = 0; i < num; i++) {
			documentWeightLocalCalculator[i] = new DocumentWeightLocalCalculator (
					myStore, i, app);
		}

		for (int i = 0; i < num; i++) {
			try {
				(documentWeightLocalCalculator[i]).t.join ();
			} catch (InterruptedException e) {
				e.printStackTrace ();
			}
		}
		myStore.closeEntityCursorDocumentLocal ();

		long t2 = System.currentTimeMillis ();
		long t3 = (t2 - t1);
		System.out
				.println ("\n Calculated document local weights  !!!! Time taken to calculate document local weights :"
						+ (t3 / 1000) + "  seconds");
		log
				.info ("\n Calculated document local weights  !!!! Time taken to calculate document local weights :"
						+ (t3 / 1000) + "  seconds");
		try {
			Thread.currentThread ().sleep (5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

	}

	public void calculateDocumentWeights () {

		// myStore.initializeEntityCursorDocument();
		System.out.println ("DocumentCurosr initialized");
		System.out.println ("Starting to calculate document  weights.....");
		log.info ("Starting to calculate document  weights.....");
		long t1 = System.currentTimeMillis ();
		int num = getNumThreads ();

		myStore.calculateDocumentWeights ();

		long t2 = System.currentTimeMillis ();
		long t3 = (t2 - t1);
		System.out
				.println ("\n Calculated document  weights  !!!! Time taken to calculate document weights :"
						+ (t3 / 1000) + "  seconds");
		log
				.info ("\n Calculated document  weights  !!!! Time taken to calculate document weights :"
						+ (t3 / 1000) + "  seconds");
		indexEndTime = System.currentTimeMillis ();

		System.out.println ("\n Indexer completed all its tasks in: "
				+ (indexEndTime - indexStartTime) / (1000 * 60) + " minutes ");
		log.info ("\n Indexer completed all its tasks in: "
				+ (indexEndTime - indexStartTime) / (1000 * 60) + " minutes ");
		try {
			Thread.currentThread ().sleep (5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

	}

	public void sendLexiconEntryLocal (LexiconEntryLocal lexiconEntryLocal) {
		String wordId = lexiconEntryLocal.getWordId ();
		Word word = myStore.getWord (wordId);
		String wordstr = word.getWordValue ();
		app.sendLocalIndexMessage (MsgType.PUT_INDEX, wordstr,
				lexiconEntryLocal);

	}

	public void createInvertedIndex () {
		myStore.createWordReceived ();
		myStore.initializeEntityCursorWordReceived ();
		System.out.println ("WordreceivedCursor initialized");
		System.out.println ("Starting to create Inverted Index....");
		log.info ("Starting to create Inverted Index....");
		long t1 = System.currentTimeMillis ();
		int num = getNumThreads ();
		InvertedIndexer[] invertedIndexer = new InvertedIndexer[num];
		long totalNumDocs = NodeManager.getInstance ().getTotalDocs ();

		System.out.println ("Total Num of Documents: " + totalNumDocs);
		log.info ("Total Num of Documents: " + totalNumDocs);
		for (int i = 0; i < num; i++) {
			invertedIndexer[i] = new InvertedIndexer (myStore, i, totalNumDocs);
		}

		for (int i = 0; i < num; i++) {
			try {
				(invertedIndexer[i]).t.join ();
			} catch (InterruptedException e) {
				e.printStackTrace ();
			}
		}
		myStore.closeEntityCursorWordReceived ();

		int size1 = myStore.sizeLexiconEntry ();
		System.out.println ("LexiconEntry  size " + size1);
		log.info ("LexiconEntry  size " + size1);
		long t2 = System.currentTimeMillis ();
		long t3 = (t2 - t1);
		System.out
				.println ("\n Inverted Index created !!!! Time taken to create inverted index:"
						+ (t3 / 1000) + "  seconds");
		log
				.info ("\n Inverted Index created !!!! Time taken to create inverted index:"
						+ (t3 / 1000) + "  seconds");
		try {
			Thread.currentThread ().sleep (5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
	}

	public MyApp getIndexerApp () {
		return app;
	}
}

/*
 * 
 * 
 * DocumentWeightCalculator[] documentWeightCalculator = new
 * DocumentWeightCalculator[num];
 * 
 * for (int i = 0; i < num; i++) { documentWeightCalculator[i] = new
 * DocumentWeightCalculator(myStore, i); }
 * 
 * for (int i = 0; i < num; i++) { try { (documentWeightCalculator[i]).t.join
 * (); } catch (InterruptedException e) { e.printStackTrace (); } }
 * myStore.closeEntityCursorDocument();
 */