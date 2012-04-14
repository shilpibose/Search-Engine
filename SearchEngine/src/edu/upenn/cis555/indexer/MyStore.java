package edu.upenn.cis555.indexer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityJoin;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.ForwardCursor;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.crawler.bean.Document;

// have to chnage maxWordFrequency = 1 ..statement...
public class MyStore {
	private final Logger log = Logger.getLogger (MyStore.class);

	private DataAccessor da;
	private EntityStore store;

	EntityCursor<WordReceived> cursorWordReceived;
	Transaction txnWordReceived;

	EntityCursor<Word> cursorWord;
	Transaction txnWord;

	EntityCursor<Document> currDocument;
	Transaction txnDocument;

	EntityCursor<LexiconEntryLocal> cursorLexiconEntryLocal;
	Transaction txnLexiconEntryLocal;

	EntityCursor<LexiconEntry> cursorLexiconEntry;
	Transaction txnLexiconEntry;

	EntityCursor<DocumentLocal> cursorDocumentLocal;
	Transaction txnDocumentLocal;

	public MyStore (EntityStore store) {
		this.store = store;
	}

	public void initiailize () {
		try {
			da = new DataAccessor (store);

		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}

	
	// removed synchronized...
	public synchronized void putDocument (Document document) {
		try {
			da.documentIndex.put (document);

		} catch (DatabaseException dbe) {
			System.out.println ("DocumentPut" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}

	
	// removed synchronized...
	public synchronized void putDocumentLocal (DocumentLocal documentLocal) {
		try {
			da.documentLocalIndex.put(null, documentLocal);

		} catch (DatabaseException dbe) {
			System.out.println ("DocumentLocalPut" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}

// removed synchronized...	
	public synchronized void putWordDocumentWeight(WordDocumentWeight wordDocumentWeight) {
		try {
			da.wordDocumentWeightIndex.put (null, wordDocumentWeight);

		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentWeightPut" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}

	// synchronized...
	public  synchronized void putForwardIndexEntry (
			ForwardIndexEntry forwardIndexEntry) {
		try {
			da.documentForwardIndexEntryIndex.put (null, forwardIndexEntry);

		} catch (DatabaseException dbe) {
			System.out.println ("DocumentPut" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}
// synchronized...
	public  synchronized void putWord (Word word) {

		Transaction txn = store.getEnvironment ().beginTransaction (null, null);
        try{
			da.wordIndex.put (txn, word);
			txn.commit ();
			txn = null;
		
		}catch (Exception e) {

			if (txn != null) {
				txn.abort ();
				txn = null;
			}
			System.out.println ("Got exception in PutWord" + e.toString ());

		}

	}
// removed synchronized
	public  synchronized void putWordReceived (WordReceived wordReceived) {
		try {
			da.wordReceivedIndex.put (null, wordReceived);

		} catch (DatabaseException dbe) {
			System.out.println ("PutWordReceived" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}

	
	// removed synchronized...
	public synchronized void putWordDocumentHit (WordDocumentHit wordDocumentHit) {
		
			Transaction txn = store.getEnvironment ().beginTransaction (null, null);
         try{
			da.wordDocumentHitIndex.put (txn, wordDocumentHit);
			txn.commit ();
			txn = null;
		
		}catch (Exception e) {

			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}
 
	}
// removed synchronized
	public synchronized void putLexiconEntryLocalReceived (
			LexiconEntryLocalReceived lexiconEntryLocalReceived) {
		try {
			da.lexiconEntryLocalReceivedIndex.put (null,
					lexiconEntryLocalReceived);

		} catch (DatabaseException dbe) {
			System.out.println ("LexiconEntryLocalReceived" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}

// removed synchronized
	public synchronized void putDocumentLocalReceived (
			DocumentLocalReceived documentLocalReceived ) {
		try {
			da.documentLocalReceivedIndex.put (null,
					documentLocalReceived);

		} catch (DatabaseException dbe) {
			System.out.println ("EXCEPTION : DocumentLocalReceived" + dbe.toString ());
			dbe.printStackTrace ();
		}
	}

	
	
	// removed synchronized...
	public synchronized  void putLexiconEntry (LexiconEntry lexiconEntry) {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		try {
			da.lexiconEntryIndex.put (txn, lexiconEntry);
			txn.commit ();
			txn = null;
		} catch (Exception e) {

			if (txn != null) {
				txn.abort ();
				txn = null;
			}
			e.printStackTrace ();
		}
	}

	// removed synchronized...
	public synchronized void putLexiconEntryLocal (
			LexiconEntryLocal lexiconEntryLocal) {

		try {
			da.lexiconEntryLocalIndex.put (null, lexiconEntryLocal);
		} catch (DatabaseException e) {

			e.printStackTrace ();
		}
	}

	
	// removed synchronized
	public synchronized Document getDocument (String id) {
		Document document = null;

		try {
			document = da.documentIndex.get (id);
			// System.out.println("document:"+document.getDocumentUrlString());
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();

		}
		return document;

	}
// removed synchronized...
	public synchronized  Word getWord (String id) {
		Word word = null;

		try {
			word = da.wordIndex.get (id);
			// System.out.println("document:"+document.getDocumentUrlString());
		} catch (DatabaseException dbe) {
			System.out.println ("WordGet:" + dbe.toString ());
			dbe.printStackTrace ();

		}
		return word;

	}

// remove synchronized...
	public synchronized String getWordStr (String id) {
		Word word;
		String wordStr = null;
		try {
			word = da.wordIndex.get (id);
			wordStr = word.getWordValue ();
			// System.out.println("document:"+document.getDocumentUrlString());
		} catch (DatabaseException dbe) {
			System.out.println ("WordGet:" + dbe.toString ());
			dbe.printStackTrace ();

		}
		return wordStr;

	}

	// removed synchronized
	public  synchronized String getUrl (String id) {
		Document document;
		String url = null;
		try {
			document = da.documentIndex.get (id);
			url = document.getUrl ();
			// System.out.println("document:"+document.getDocumentUrlString());
		} catch (DatabaseException dbe) {
			System.out.println ("UrlGet:" + dbe.toString ());
			dbe.printStackTrace ();

		}
		return url;

	}

	// removed synchronized...
	public synchronized boolean containsWord (String id) {
		Word word = null;
		try {
			word = da.wordIndex.get (id);
			// System.out.println("document:"+document.getDocumentUrlString());
		} catch (DatabaseException dbe) {
			System.out.println ("WordGet:" + dbe.toString ());
			dbe.printStackTrace ();

		}
		if (word == null)
			return false;
		else
			return true;

	}

	
	//removed synchronized...
	public  synchronized void getAllDocuments () {

		EntityCursor<Document> documents = da.documentIndex.entities ();
		// System.out.println("Inside all documents");

		try {
			for (Document document : documents) {
				// System.out.println(" ");
				// System.out.print("ID: "+ document.getId()+"   ");
				// System.out.print("TYPE: "+document.getContentType()+"   ");
				// System.out.println(document.getUrl());

			}
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			documents.close ();
		}
	}

	// removed synchronized
	public  synchronized EntityCursor<WordReceived> getWordReceivedEntityCursor () {
		EntityCursor<WordReceived> wordsReceived = da.wordReceivedIndex
				.entities ();
		return wordsReceived;
	}

	
	// removed synchronized
	public  synchronized void closeEntityCursorWordReceived () {
		cursorWordReceived.close ();
		cursorWordReceived = null;
		txnWordReceived.commit ();
		txnWordReceived = null;
	}

	// removed synchronized
	public synchronized void closeEntityCursorDocumentLocal () {
		cursorDocumentLocal.close ();
		cursorDocumentLocal = null;
		txnDocumentLocal.commit ();
		txnDocumentLocal = null;
	}

	// removed synchronized
	public  synchronized void closeEntityCursorWord () {
		cursorWord.close ();
		cursorWord = null;
		txnWord.commit ();
		txnWord = null;
	}

	
	// removed synchronized
	public  synchronized void closeEntityCursorDocument () {

		currDocument.close ();
		currDocument = null;
		txnDocument.commit ();
		txnDocument = null;

	}

	
	// removed synchronized
	public  synchronized void closeEntityCursorLexiconEntryLocal () {

		cursorLexiconEntryLocal.close ();
		cursorLexiconEntryLocal = null;
		txnLexiconEntryLocal.commit ();
		txnLexiconEntryLocal = null;

	}

	
	// removed synchronized
	public synchronized  void closeEntityCursorLexiconEntry () {

		cursorLexiconEntry.close ();
		cursorLexiconEntry = null;
		txnLexiconEntry.commit ();
		txnLexiconEntry = null;

	}

	// removed synchronized
	public synchronized Set<String> getDocumnetIds (String wordId) {
		Set<String> documentIdSet = new HashSet<String> ();

		EntityCursor<WordDocumentHit> wordDocumentHits = da.wordFkIndex
				.subIndex (wordId).entities ();

		try {
			for (WordDocumentHit wordDocumentHit : wordDocumentHits) {
				documentIdSet.add (wordDocumentHit.getDocumentFk ());
			}
		} catch (DatabaseException dbe) {
			dbe.printStackTrace ();
		} finally {
			wordDocumentHits.close ();
		}
		return documentIdSet;
	}

	
	// removed synchronized
	public synchronized  Set<String> getWordIds (String documentId) {
		Set<String> wordIdSet = new HashSet<String> ();

		EntityCursor<WordDocumentHit> wordDocumentHits = da.documentFkIndex
				.subIndex (documentId).entities ();

		try {
			for (WordDocumentHit wordDocumentHit : wordDocumentHits) {
				wordIdSet.add (wordDocumentHit.getWordFk ());
			}
		} catch (DatabaseException dbe) {
			dbe.printStackTrace ();
		} finally {
			wordDocumentHits.close ();
		}
		return wordIdSet;
	}

	
	// removed synchronized
	public synchronized int sizeWordReceived () {
		EntityCursor<WordReceived> words = da.wordReceivedIndex.entities ();

		int size = 0;
		try {
			for (WordReceived wordReceived : words) {
				size = size + 1;
			}
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			words.close ();
		}

		return size;
	}

	
	// removed synchronized
	public  synchronized int sizeForwardIndexEntry () {
		EntityCursor<ForwardIndexEntry> entries = da.documentForwardIndexEntryIndex
				.entities ();

		int size = 0;
		try {
			for (ForwardIndexEntry forwardIndexEntry : entries) {
				size = size + 1;
			}
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			entries.close ();
		}

		return size;
	}

	
	// removed synchronized
	public  synchronized int sizeWordDocumentHit () {
		EntityCursor<WordDocumentHit> wordDocumentHits = da.wordDocumentHitIndex
				.entities ();

		int size = 0;
		try {
			for (WordDocumentHit wordDocumentHit : wordDocumentHits) {
				size = size + 1;
			}
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			wordDocumentHits.close ();
		}

		return size;
	}

	// removed synchronized
	public  synchronized int sizeWords () {
		EntityCursor<Word> words = da.wordIndex.entities ();

		int size = 0;
		try {
			for (Word word : words) {
				size = size + 1;
			}
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			words.close ();
		}

		return size;
	}
	// removed synchronized
	public  synchronized int sizeDocuments () {

		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<Document> documents = da.documentIndex
				.entities (txn, null);

		int size = 0;
		try {
			for (Document document : documents) {
				size = size + 1;
			}
			documents.close ();
			documents = null;
			txn.commit ();
			txn = null;
		} catch (Exception e) {
			if (documents != null) {
				documents.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}

		return size;
	}

	// removed synchronized
	public  synchronized int sizeLexiconEntryLocal () {
		EntityCursor<LexiconEntryLocal> lexiconEntries = da.lexiconEntryLocalIndex
				.entities ();

		int size = 0;
		try {
			for (LexiconEntryLocal lexiconEntryLocal : lexiconEntries) {
				size = size + 1;
			}
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			lexiconEntries.close ();
		}

		return size;
	}

	// removed synchronized
	public  synchronized int sizeLexiconEntry () {
		EntityCursor<LexiconEntry> lexiconEntries = da.lexiconEntryIndex
				.entities ();

		int size = 0;
		try {
			for (LexiconEntry lexiconEntry : lexiconEntries) {
				size = size + 1;
			}
		} catch (DatabaseException dbe) {
			System.out.println ("DocumentGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			lexiconEntries.close ();
		}

		return size;
	}

	// removed synchronized
	public synchronized void getAllWords () {
		EntityCursor<Word> words = da.wordIndex.entities ();
		// System.out.println("Inside all Words");

		try {
			for (Word word : words) {
				// System.out.println(" ");
				// System.out.print("ID: "+ word.getId()+"   ");
				// System.out.print("Value: "+word.getWordValue()+"   ");

			}
			// System.out.println("All Words over");
		} catch (DatabaseException dbe) {
			System.out.println ("WordsGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			words.close ();
		}
	}

	/**
 * 
 * 
 */
	// removed synchronized
	public  synchronized void getAllWordDocuments () {
		EntityCursor<WordDocumentHit> wordDocumentHits = da.wordDocumentHitIndex
				.entities ();
		// System.out.println("Inside all WordDocuments");

		try {
			for (WordDocumentHit wordDocumentHit : wordDocumentHits) {
				// System.out.println(" ");
				// System.out.print("ID: "+
				// wordDocument.getWordDocumentId()+"   ");
				// System.out.print("WordId: "+wordDocument.getWordFk()+"   ");
				Word word = getWord (wordDocumentHit.getWordFk ());
				String wordstr = word.getWordValue ();
				// System.out.print("Word: "+wordstr+"   ");
				// System.out.print("DocumentId: "+wordDocument.getDocumentFk()+"   ");
				Document document = getDocument (wordDocumentHit
						.getDocumentFk ());
				String url = document.getUrl ();
				// System.out.print("Document url : "+url+"  ");
				Hit hit = wordDocumentHit.getHit ();
				int position = hit.getPosition ();
				// System.out.println("Position : " +position+"  ");

			}
			// System.out.println("Inside all WordDocuments over");

		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentsGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			wordDocumentHits.close ();
		}
	}

	/**
	 * 
	 * 
	 * Prints all words to a file
	 */
	
	// removed synchronized
	public  synchronized void printAllWordDocumentsHits () {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting WordDocumentsHits Local");
		EntityCursor<WordDocumentHit> wordDocumentHits = da.wordDocumentHitIndex
				.entities ();
		// System.out.println("Inside all WordDocuments");

		String filepath = "";
		try {
			filepath = Constants.WORD_DOCUMENT_HIT_LOCAL_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("word" + " | " + "documenturl" + " | " + "position"
					+ "\n");

			for (WordDocumentHit wordDocumentHit : wordDocumentHits) {
				Word word = getWord (wordDocumentHit.getWordFk ());
				String wordstr = word.getWordValue ();
				Document document = getDocument (wordDocumentHit
						.getDocumentFk ());
				String url = document.getUrl ();
				Hit hit = wordDocumentHit.getHit ();
				int position = hit.getPosition ();
				fw1.write (wordstr + " | " + url + " | " + position + "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Priniting  all WordDocumentsHits Local over");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out
					.println ("\n Inverted Index Local created !!!! Time taken to create inverted index Local :"
							+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentsGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			wordDocumentHits.close ();
		}
	}

	public  synchronized void printAllWordDocumentWeight () {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting Documents Local");
		EntityCursor<WordDocumentWeight> wordDocumentWeights  = da.wordDocumentWeightIndex.entities ();
		
		// System.out.println("Inside all WordDocuments");

		String filepath = "";
		try {
			filepath = Constants.WORD_DOCUMENT_LOCAL_WEIGHT_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("url" + " | " + "weight"+"\n");

			for (WordDocumentWeight wordDocumentWeight  : wordDocumentWeights) {
				 String url = wordDocumentWeight.getUrl();
				 double weight = wordDocumentWeight.getWeight();
				 fw1.write (url + " | " + weight + "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Priniting  all WordDocumentWeights");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out
					.println ("\n Word Document Weights  created !!!! Time taken to create wordDocumentWeights :"
							+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentWeights:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			wordDocumentWeights.close ();
		}
	}

	

	public  synchronized void printAllDocumentsLocal () {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting Documents Local");
		EntityCursor<DocumentLocal> documentsLocal  = da.documentLocalIndex.entities ();
		// System.out.println("Inside all WordDocuments");

		String filepath = "";
		try {
			filepath = Constants.DOCUMENT_LOCAL_WEIGHT_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("url" + " | " + "weight"+"\n");

			for (DocumentLocal documentLocal : documentsLocal) {
				 String url = documentLocal.getUrl();
				 double weight = documentLocal.getCalcWeight();
				 fw1.write (url + " | " + weight + "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Priniting  all DocumentsLocal");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out
					.println ("\n Documents Local created !!!! Time taken to create documentsLocal :"
							+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("DocumentsLocal:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			documentsLocal.close ();
		}
	}

	public  synchronized void printAllDocumentsLocalReceived() {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting Documents Local Received");
		EntityCursor<DocumentLocalReceived> documentsLocalReceived  = da.documentLocalReceivedIndex.entities ();
		// System.out.println("Inside all WordDocuments");

		String filepath = "";
		try {
			filepath = Constants.DOCUMENT_LOCAL_RECEIVED_WEIGHT_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("url" + " | " + "weight"+"\n");

			for (DocumentLocalReceived documentLocalReceived : documentsLocalReceived) {
				 String url = documentLocalReceived.getUrl();
				 double weight = documentLocalReceived.getCalcWeight();
				 fw1.write (url + " | " + weight + "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Priniting  all DocumentsLocalReceived");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out
					.println ("\n Documents Local Receivedcreated !!!! Time taken to create documentsLocal Received:"
							+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("DocumentsLocalReceived :" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			documentsLocalReceived.close ();
		}
	}

	
	
	/*
	
	public synchronized void printAllWords () {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting word documents");
		EntityCursor<Word> words = da.wordIndex.entities ();
		// System.out.println("Inside all Words");

		String filepath = "";
		try {
			filepath = Constants.WORD_LOCAL_FILENAME_PRINT.concat (InetAddress
					.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("wordValue" + " | "+"wordId" + "\n");

			for (Word word : words) {

				String wordId = word.getId();
				String wordstr = word.getWordValue ();
				int length =wordstr.length();
				fw1.write (wordstr+" | "+length+" |"+ wordId + "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Prinited all Words !!!!");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out.println ("\n Time taken to print words :"
					+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentsGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			words.close ();
		}
	}

*/   // removed synchronized
	public  synchronized void printAllWords() {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting words");
		EntityCursor<LexiconEntryLocal> lexiconEntries = da.lexiconEntryLocalIndex.entities ();
		// System.out.println("Inside all Words");

		String filepath = "";
		try {
			filepath = Constants.WORD_LOCAL_FILENAME_PRINT.concat (InetAddress
					.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("wordValue" + " | "+"wordId" + "\n");

			for (LexiconEntryLocal lexiconEntryLocal : lexiconEntries) {

				String wordId = lexiconEntryLocal.getWordId();
				String wordstr = lexiconEntryLocal.getWord();
				int length =wordstr.length();
				fw1.write (wordstr+" | "+length+" |"+ wordId + "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Prinited all Words !!!!");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out.println ("\n Time taken to print words :"
					+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentsGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			lexiconEntries.close ();
		}
	}

	
	// removed synchronized
	public  synchronized void printAllWordReceived () {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting words Received start...");
		EntityCursor<LexiconEntry> lexiconEntries = da.lexiconEntryIndex.entities ();
		// System.out.println("Inside all Words");

		String filepath = "";
		try {
			filepath = Constants.WORD_RECEIVED_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("wordValue" +" | "+"length"+" | "+"wordId"+ "\n");

			for (LexiconEntry lexiconEntry : lexiconEntries) {
                String wordId = lexiconEntry.getWordId();
				String wordstr = lexiconEntry.getWord();
				int length = wordstr.length();
				fw1.write (wordstr + " | "+length+" | "+wordId+ "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Prinited all Words Received!!!!");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out.println ("\n Time taken to print words :"
					+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("WordReceived:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			lexiconEntries.close ();
		}
	}

	
	// removed synchronized
	public  synchronized void printAllDocumnets () {
		long t1 = System.currentTimeMillis ();

		System.out.println ("Priniting All Documents");
		EntityCursor<Document> documents = da.documentIndex.entities ();
		// System.out.println("Inside all Documents");

		String filepath = "";
		try {
			filepath = Constants.DOCUMENT_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		File file = new File (filepath);
		FileWriter fw1;

		try {
			fw1 = new FileWriter (file);
			fw1.write ("URL" + " | " + "Type" + " | " + "MaxWordFrequency"
					+ "\n");

			for (Document document : documents) {

				String url = document.getUrl ();
				String type = document.getContentType ();
				int maxWordFrequency = document.getMaxWordFrequency ();
				double weight = document.getDj();
				double pageRank = document.getPagerank();
				
				fw1
						.write (url + " | " + type + " | " + maxWordFrequency+" | "+weight+" | "+pageRank
								+ "\n");
				// fw1.flush();
			}
			fw1.flush ();
			fw1.close ();

			System.out.println ("Printing All Documents Over");
			long t2 = System.currentTimeMillis ();
			long t3 = (t2 - t1);
			System.out
					.println ("\n Inverted Index Local created !!!! Time taken to create inverted index Local :"
							+ (t3 / (1000 * 60)) + "  minutes");

		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentsGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			documents.close ();
		}
	}

	
	// removed synchronized
	public  synchronized void deleteAllDocuments () {
		EntityCursor<Document> documents = da.documentIndex.entities ();

		try {
			for (Document document : documents) {
				documents.delete ();
			}
		} catch (DatabaseException dbe) {
			System.out.println ("deleteAllDocuments:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			documents.close ();
		}
	}

	
	// removed synchronized
	public  synchronized void deleteAllWords () {

		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<Word> words = da.wordIndex.entities (txn, null);
		try {
			for (Word word : words) {
				words.delete ();

			}
			System.out.println ("All words deleted");
			words.close ();
			words = null;
			txn.commit ();
			txn = null;

		} catch (Exception e) {
			if (words != null) {
				words.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}

	}

	// removed synchronized
	public  synchronized void deleteAllWordsReceived () {

		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<WordReceived> words = da.wordReceivedIndex.entities (txn,
				null);
		try {
			for (WordReceived wordReceived : words) {
				words.delete ();

			}
			System.out.println ("All words Received deleted");
			words.close ();
			words = null;
			txn.commit ();
			txn = null;

		} catch (Exception e) {
			if (words != null) {
				words.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}

	}

	// removed synchronized
	public  synchronized void deleteAllWordDocumentHits () {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<WordDocumentHit> wordDocumentHits = da.wordDocumentHitIndex
				.entities (txn, null);

		try {
			for (WordDocumentHit wordDocumentHit : wordDocumentHits) {
				wordDocumentHits.delete ();
			}
			System.out.println ("All existing WordDocumentHits deleted");
			wordDocumentHits.close ();
			wordDocumentHits = null;
			txn.commit ();
			txn = null;

		} catch (Exception e) {
			if (wordDocumentHits != null) {
				wordDocumentHits.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}
	}

	// removed synchronized
	public synchronized EntityCursor<Document> getEntityCursorDocument () {
		EntityCursor<Document> documents = da.documentIndex.entities ();

		return documents;
	}

	public synchronized EntityCursor<LexiconEntryLocal> getEntityCursorLexiconEntryLocal () {
		EntityCursor<LexiconEntryLocal> lexiconEntryLocalEntries = da.lexiconEntryLocalIndex
				.entities ();

		return lexiconEntryLocalEntries;
	}

	// removed synchronized
	public  synchronized void initializeEntityCursorWordReceived () {
		txnWordReceived = store.getEnvironment ().beginTransaction (null, null);
		this.cursorWordReceived = da.wordReceivedIndex.entities (
				txnWordReceived, null);
		// System.out.println("WordReceived cursor initialized");
	}


	// removed synchronized
	public  synchronized void initializeEntityCursorDocumentLocal() {
		txnDocumentLocal = store.getEnvironment ().beginTransaction (null, null);
		this.cursorDocumentLocal = da.documentLocalIndex.entities (
				txnDocumentLocal, null);
	}

	
	// removed synchronized
	public  synchronized void initializeEntityCursorWord () {
		txnWord = store.getEnvironment ().beginTransaction (null, null);
		this.cursorWord = da.wordIndex.entities (txnWord, null);
		// System.out.println("Word cursor initialized");
	}

	// removed synchronized
	public  synchronized void initializeEntityCursorDocument () {
		txnDocument = store.getEnvironment ().beginTransaction (null, null);
		this.currDocument = da.documentIndex.entities (txnDocument, null);
		// System.out.println("Document cursor initialized");
	}
	// removed synchronized
	public  synchronized void initializeEntityCursorLexiconEntryLocal () {
		txnLexiconEntryLocal = store.getEnvironment ().beginTransaction (null,
				null);
		this.cursorLexiconEntryLocal = da.lexiconEntryLocalIndex.entities (
				txnLexiconEntryLocal, null);
		// System.out.println("Lexicon Entry Local cursor initialized");
	}

	// removed synchronized
	public  synchronized void initializeEntityCursorLexiconEntry() {
		txnLexiconEntry = store.getEnvironment ().beginTransaction (null,
				null);
		this.cursorLexiconEntry = da.lexiconEntryIndex.entities (
				txnLexiconEntry, null);
		// System.out.println("Lexicon Entry Local cursor initialized");
	}

	
	// removed synchronized
	public  synchronized WordReceived getNextWordReceived () {
		WordReceived wordReceived = null;
		try {
			wordReceived = cursorWordReceived.next (LockMode.DEFAULT);

		} catch (Exception e) {
			if (cursorWordReceived != null) {
				cursorWordReceived.close ();
			}
			if (txnWordReceived != null) {
				txnWordReceived.abort ();
				txnWordReceived = null;
			}
		}
		return wordReceived;

	}

	// removed synchronized
	public  synchronized Word getNextWord () {
		Word word = null;
		try {
			word = cursorWord.next (LockMode.DEFAULT);

		} catch (Exception e) {
			if (cursorWord != null) {
				cursorWord.close ();
			}
			if (txnWord != null) {
				txnWord.abort ();
				txnWord = null;
			}
		}
		return word;

	}

	// removed synchronized
	public  synchronized Document getNextDocument () {

		Document document = null;
		try {
			document = currDocument.next (null);

		} catch (Exception e) {
			if (currDocument != null) {
				currDocument.close ();
			}
			if (txnDocument != null) {
				txnDocument.abort ();
				txnDocument = null;
			}
		}
		return document;

	}

	
	// removed synchronized
	public  synchronized LexiconEntryLocal getNextLexiconEntryLocal () {

		LexiconEntryLocal lexiconEntryLocal = null;
		try {
			lexiconEntryLocal = cursorLexiconEntryLocal.next (null);

		} catch (Exception e) {
			if (cursorLexiconEntryLocal != null) {
				cursorLexiconEntryLocal.close ();
			}
			if (txnLexiconEntryLocal != null) {
				txnLexiconEntryLocal.abort ();
				txnLexiconEntryLocal = null;
			}
		}
		return lexiconEntryLocal;

	}

	// removed synchronized
	public  synchronized LexiconEntry getNextLexiconEntry() {

		LexiconEntry lexiconEntry = null;
		try {
			lexiconEntry = cursorLexiconEntry.next (null);

		} catch (Exception e) {
			if (cursorLexiconEntry != null) {
				cursorLexiconEntry.close ();
			}
			if (txnLexiconEntry != null) {
				txnLexiconEntry.abort ();
				txnLexiconEntry = null;
			}
			e.printStackTrace();
		}
		return lexiconEntry;

	}

	// removed synchronized
	public  synchronized DocumentLocal getNextDocumentLocal() {

		DocumentLocal documentLocal = null;
		try {
			documentLocal = cursorDocumentLocal.next (null);

		} catch (Exception e) {
			if (cursorDocumentLocal != null) {
				cursorDocumentLocal.close ();
			}
			if (txnDocumentLocal != null) {
				txnDocumentLocal.abort ();
				txnDocumentLocal = null;
			}
			e.printStackTrace();
		}
		return documentLocal;
	
	}

	// removed synchronized
	public synchronized void createDocumentWeightLocal(DocumentLocal documentLocal){
		String documentId = documentLocal.getDocumentId();
		String url = documentLocal.getUrl();
	    double documentWeightLocal = 0;  //by default keeping as zero.....
		EntityCursor<WordDocumentWeight> entries = da.documentFkWeightIndex.subIndex (documentId).entities ();
        try {
	       for (WordDocumentWeight wordDocumentWeight : entries) {
		      double weight = wordDocumentWeight.getWeight();
		      documentWeightLocal= documentWeightLocal+ weight*weight;
	        }
	    } catch (DatabaseException dbe) {
	        dbe.printStackTrace ();
          }  finally {
	       entries.close ();
       }
     
     
     documentLocal.setCalcWeight(documentWeightLocal);
     updateDocumentLocal(documentLocal);
     System.out.println("Calculated document local weight for "+url+". Local weight: "+documentWeightLocal);
	log.info("Calculated document local weight for "+url+". Local weight: "+documentWeightLocal);
	}
	
	// removed synchronized
	public  synchronized void updateDocumentLocal (
			DocumentLocal documentLocal) {
		cursorDocumentLocal.update (documentLocal);

	}

	// removed synchronized
	public synchronized void updateLexiconEntryLocal (
			LexiconEntryLocal lexiconEntryLocal) {
		cursorLexiconEntryLocal.update (lexiconEntryLocal);

	}
	// removed synchronized
	public synchronized void printAllLexiconEntries () {
		System.out.println ("Starting printing lexicon entries...");
		long time1 = System.currentTimeMillis ();

		EntityCursor<Word> words = da.wordIndex.entities ();
		LexiconEntryLocal lexiconEntryLocal;
		String filepath = "";
		try {
			filepath = Constants.LEXICON_LOCAL_NOTUPDATED_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

		try {
			File file = new File (filepath);
			FileWriter fw;
			fw = new FileWriter (file);
			fw.write ("word" + " | " + "documenturl" + " | " + "position"
					+ "\n");

			for (Word word : words) {
				String wordId = word.getId ();
				String wordstr = word.getWordValue ();
				lexiconEntryLocal = getLexiconEntryLocal (wordId);
				int numDocs = lexiconEntryLocal.getNdocs ();
				fw.write (wordstr + " | " + numDocs + "\n");

				InvertedBarrel invertedBarrel = lexiconEntryLocal
						.getInvertedBarrel ();
				ArrayList<DocumentHits> documnetHitsList = invertedBarrel
						.getDocumentHitsList ();
				for (DocumentHits documentHits : documnetHitsList) {
					String url = documentHits.getUrl ();
					int nHits = documentHits.getNhits ();
					int maxWordFrequency = documentHits.getMaxWordFrequency ();
					double tf = documentHits.getTf ();

					fw.write (wordstr + " | " + url + " | " + nHits + " | "
							+ maxWordFrequency + " | " + tf + "\n");
					//  	
					String documentId = documentHits.getDocumentId ();
					Document document = da.documentIndex.get (documentId);

					HitsList hitsList = documentHits.getHitsList ();
					ArrayList<Hit> hits = hitsList.getHits ();
					for (Hit hit : hits) {
						int position = hit.getPosition ();
						// System.out.println(wordstr+" | "+url+" | "+
						// position);

						fw.write (wordstr + " | " + url + " | " + position
								+ "\n");
						// fw.flush();
					}
				}
			}
			fw.flush ();
			fw.close ();
		} catch (DatabaseException dbe) {
			System.out.println ("PrintLexiconLocal:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			words.close ();
		}
		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000 * 60);
		System.out.println ("Printing lexicon entries over !!!!");
		System.out
				.println ("Time taken to printlexicon in minutes:" + diffTime);

	}

	
	// removed synchronized
	public synchronized void printForwardIndexEntries () {
		System.out.println ("Starting printing Forward entries...");
		long time1 = System.currentTimeMillis ();
		String filepath = "";
		try {
			filepath = Constants.FORWARD_INDEX_LOCAL_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

		EntityCursor<ForwardIndexEntry> entries = da.documentForwardIndexEntryIndex
				.entities ();

		try {

			File file = new File (filepath);
			FileWriter fw;
			fw = new FileWriter (file);
			fw.write ("word" + " | " + "documenturl" + " | " + "position"
					+ "\n");

			for (ForwardIndexEntry forwardIndexEntry : entries) {
				String documentId = forwardIndexEntry.getDocumentId ();
				String url = getUrl (documentId);
				ForwardBarrel forwardBarrel = forwardIndexEntry
						.getForwardBarrel ();
				int maxWordFrequency = forwardIndexEntry.getMaxWordFrequency ();
				int nWords = forwardIndexEntry.getNwords ();
				// System.out.println("NoOfWords"+nWords+"MaxWordFrequency"+maxWordFrequency);
				ArrayList<WordHits> wordHitsList = forwardBarrel
						.getWordHitsList ();
				for (WordHits wordHits : wordHitsList) {
					HitsList hitsList = wordHits.getHitsList ();
					ArrayList<Hit> hits = hitsList.getHits ();
					int nHits = wordHits.getNhits ();
					String wordId = wordHits.getWordId ();
					String wordstr = getWordStr (wordId);

					for (Hit hit : hits) {
						int position = hit.getPosition ();
						fw.write (wordstr + " | " + url + " | " + position
								+ "\n");
						// System.out.println(wordstr+" | "+url+" | "+position);
					}
				}
			}
			fw.flush ();
			fw.close ();

		} catch (DatabaseException dbe) {
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		}

		finally {
			entries.close ();
		}
		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000);
		System.out.println ("Printing forward index entries over !!!!");
		System.out.println ("Time taken to print forward index in seconds"
				+ diffTime);

	}

	
	// removed synchronized
	public synchronized  LexiconEntryLocal getLexiconEntryLocal (String wordId) {

		LexiconEntryLocal lexiconEntryLocal = null;
		try {
			lexiconEntryLocal = da.lexiconEntryLocalIndex.get (wordId);
		} catch (DatabaseException dbe) {
			System.out.println ("deleteAllWordDocuments:" + dbe.toString ());
			dbe.printStackTrace ();

		}
		return lexiconEntryLocal;
	}

	// removed synchronized
	public  synchronized void deleteAllLexiconEntries () {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<LexiconEntry> lexiconEntries = da.lexiconEntryIndex
				.entities ();

		try {
			for (LexiconEntry lexiconEntry : lexiconEntries) {
				lexiconEntries.delete ();
			}
			lexiconEntries.close ();
			lexiconEntries = null;
			txn.commit ();
			txn = null;
		} catch (Exception e) {
			if (lexiconEntries != null) {
				lexiconEntries.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}

	}

	
	// removed synchronized
	public  synchronized void deleteAllLexiconEntriesLocal () {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<LexiconEntryLocal> lexiconEntriesLocal = da.lexiconEntryLocalIndex
				.entities (txn, null);

		try {
			for (LexiconEntryLocal lexiconEntryLocal : lexiconEntriesLocal) {
				lexiconEntriesLocal.delete ();
			}
			System.out.println ("All existing LexiconEntriesLocal deleted");
			lexiconEntriesLocal.close ();
			lexiconEntriesLocal = null;
			txn.commit ();
			txn = null;

		} catch (Exception e) {
			if (lexiconEntriesLocal != null) {
				lexiconEntriesLocal.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}

	}

	// removed synchronized
	public  synchronized void deleteAllForwardIndexEntries () {

		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<ForwardIndexEntry> entries = da.documentForwardIndexEntryIndex
				.entities (txn, null);

		try {
			for (ForwardIndexEntry forwardIndexEntry : entries) {
				entries.delete ();
			}
			System.out.println ("\n All existing ForwardIndexEntries deleted");
			entries.close ();
			entries = null;
			txn.commit ();
			txn = null;
		} catch (Exception e) {
			if (entries != null) {
				entries.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}

	}

	// removed synchronized
	public  synchronized void printUpdatedLexiconLocal () {
		System.out.println ("Starting printing updated lexicon entries...");
		long time1 = System.currentTimeMillis ();

		EntityCursor<Word> words = da.wordIndex.entities ();
		LexiconEntryLocal lexiconEntryLocal;
		String filepath = "";
		try {
			filepath = Constants.LEXICON_LOCAL_UPDATED_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

		try {
			File file = new File (filepath);
			FileWriter fw;
			fw = new FileWriter (file);
			fw.write ("word" + " | " + "documenturl" + " | " + "position"
					+ "\n");

			for (Word word : words) {
				String wordId = word.getId ();
				String wordstr = word.getWordValue ();
				lexiconEntryLocal = getLexiconEntryLocal (wordId);
				InvertedBarrel invertedBarrel = lexiconEntryLocal
						.getInvertedBarrel ();
				ArrayList<DocumentHits> documnetHitsList = invertedBarrel
						.getDocumentHitsList ();
				for (DocumentHits documentHits : documnetHitsList) {
					String documentId = documentHits.getDocumentId ();
					Document document = da.documentIndex.get (documentId);
					String url = document.getUrl ();
					int nHits = documentHits.getNhits ();
					int maxWordFrequency = documentHits.getMaxWordFrequency ();
					double tf = documentHits.getTf ();

					fw.write (wordstr + " | " + url + " | " + nHits + " | "
							+ maxWordFrequency + " | " + tf + "\n");
					// System.out.println("\n"+wordstr+" | "+url+" | "+nHits+" | "+maxWordFrequency+" | "+tf);

					HitsList hitsList = documentHits.getHitsList ();
					ArrayList<Hit> hits = hitsList.getHits ();
					for (Hit hit : hits) {
						int position = hit.getPosition ();
						// System.out.println(wordstr+" | "+url+" | "+
						// position);

						fw.write (wordstr + " | " + url + " | " + position
								+ "\n");
						// fw.flush();
					}
				}
			}
			fw.flush ();
			fw.close ();
		} catch (DatabaseException dbe) {
			System.out.println ("CreateLexicon:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			words.close ();
		}
		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000);
		System.out.println ("Printing updated lexicon entries over !!!!");
		System.out.println ("Time taken to print updated lexicon in seconds"
				+ diffTime);

	}
	// removed synchronized
	public   synchronized void printInvertedIndex () {
		System.out.println ("Starting printing Inverted Index entries...");
		long time1 = System.currentTimeMillis ();

		EntityCursor<LexiconEntry> entries = da.lexiconEntryIndex.entities ();
		String filepath = "";
		try {
			filepath = Constants.LEXICON_FINAL_FILENAME_PRINT
					.concat (InetAddress.getLocalHost ().getHostAddress ());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

		try {
			File file = new File (filepath);
			FileWriter fw;
			fw = new FileWriter (file);
			fw.write ("word" + " | " + "documenturl" + " | " + "position"
					+ "\n");

			for (LexiconEntry lexiconEntry : entries) {
				String wordId = lexiconEntry.getWordId ();
				String wordstr = lexiconEntry.getWord ();
				int numDocs = lexiconEntry.getNdocs ();
				long maxNumDocs = lexiconEntry.getMaxNumDocs ();
				double idf = lexiconEntry.getIdf ();
				fw.write (wordstr + " | " + numDocs + " | " + maxNumDocs
						+ " | " + idf + "\n");
				// System.out.println("\n"+wordstr+" | "+numDocs+" | "+maxNumDocs+" | "+idf+"\n");

				InvertedBarrel invertedBarrel = lexiconEntry
						.getInvertedBarrel ();
				ArrayList<DocumentHits> documnetHitsList = invertedBarrel
						.getDocumentHitsList ();
				for (DocumentHits documentHits : documnetHitsList) {
					String documentId = documentHits.getDocumentId ();
					String url = documentHits.getUrl ();
					int nHits = documentHits.getNhits ();
					int maxWordFrequency = documentHits.getMaxWordFrequency ();
					double tf = documentHits.getTf ();
					double weight = documentHits.getWeight ();

					fw.write (wordstr + " | " + url + " | " + nHits + " | "
							+ maxWordFrequency + " | " + tf + " | " + weight
							+ "\n");
					// System.out.println("\n"+wordstr+" | "+url+" | "+nHits+" | "+maxWordFrequency+" | "+tf+" | "+weight);

					HitsList hitsList = documentHits.getHitsList ();
					ArrayList<Hit> hits = hitsList.getHits ();
					for (Hit hit : hits) {
						int position = hit.getPosition ();
						// System.out.println(wordstr+" | "+url+" | "+
						// position);

						fw.write (wordstr + " | " + url + " | " + position
								+ "\n");
					}
				}
			}
			fw.flush ();
			fw.close ();
		} catch (DatabaseException dbe) {
			System.out.println ("CreateLexicon:" + dbe.toString ());
			dbe.printStackTrace ();
		} catch (IOException ioe) {
			ioe.printStackTrace ();
		} finally {
			entries.close ();
		}
		long time2 = System.currentTimeMillis ();
		long diffTime = (time2 - time1) / (1000);
		System.out.println ("Printing Inverted Index entries over !!!!");
		System.out.println ("Time taken to print Inverted Index in seconds"
				+ diffTime);

	}
// added synchronized...
	public synchronized void createDocumentLocal(){
		//Set<String> documentIds = new HashSet<String> ();
		System.out.println("Starting to create document Local");
log.info("Starting to create document Local");
		long t1 = System.currentTimeMillis ();

		Hashtable<String,String > documentIdsUrl = new Hashtable<String, String>();
		EntityCursor<WordDocumentWeight> entries = da.wordDocumentWeightIndex.entities ();
		try {
			for (WordDocumentWeight wordDocumentWeight: entries) {
		      String documentId = wordDocumentWeight.getDocumentFk();
		        String url =     wordDocumentWeight.getUrl();
		      documentIdsUrl.put(documentId, url);
		     }
			
			
		} catch (DatabaseException dbe) {
			System.out.println ("WordDocumentWeightGet:" + dbe.toString ());
			dbe.printStackTrace ();
		} finally {
			entries.close ();
		}
		
		Enumeration<String> e =documentIdsUrl.keys();
		
		while(e.hasMoreElements()){
		 String documentId = 	e.nextElement();
		 String url = documentIdsUrl.get(documentId);
	       DocumentLocal documentLocal = new DocumentLocal();
           documentLocal.setDocumentId(documentId);
           documentLocal.setUrl(url);
           putDocumentLocal(documentLocal);	      
	 
		}
		long t2 = System.currentTimeMillis ();
		long t3 = (t2 - t1);
		System.out
				.println ("\n Document Local created complete  !!!! Time taken to create document Local:"
						+ (t3 / 1000) + "  secondss");
log.info("\n Document Local created complete  !!!! Time taken to create document Local:"
		+ (t3 / 1000) + "  secondss");
		
	}
	
	
	
	//added synchronized
	public synchronized int getDocumentMaxWordFrequency (String documentId) {
		int documentMaxWordFrequency = 0;
		try {
			String url = getUrl (documentId);

			ForwardIndexEntry forwardIndexEntry = da.documentForwardIndexEntryIndex
					.get (documentId);
			documentMaxWordFrequency = forwardIndexEntry.getMaxWordFrequency ();
			// System.out.println("\nDocument  "+url+" has max word frequency of "+documentMaxWordFrequency);

		} catch (DatabaseException dbe) {
			dbe.printStackTrace ();
		}
		return documentMaxWordFrequency;
	}

	// synchronized 
	public void createLexiconEntryLocal (String wordId) {
		LexiconEntryLocal lexiconEntryLocal = new LexiconEntryLocal ();
		lexiconEntryLocal.setWordId (wordId);
		lexiconEntryLocal.setWord (getWordStr (wordId));
		InvertedBarrel invertedBarrel = getInvertedBarrel (wordId);
		int ndocs = invertedBarrel.size ();
		// System.out.println("\nWord: "+wordstr+" is in :"+ndocs+"  documents");

		ArrayList<DocumentHits> documentHitsList = invertedBarrel
				.getDocumentHitsList ();

		for (DocumentHits documentHits : documentHitsList) {
			String documentId = documentHits.getDocumentId ();
			Document document = getDocument (documentId);
			int nhits = documentHits.getNhits ();
			
			HitsList hitsList = documentHits.getHitsList();
			ArrayList<Hit> hits = hitsList.getHits();
			for (Hit hit : hits) {
				if(hit.isTitle()) nhits = nhits + 15;
				if(hit.isBold()) nhits = nhits + 10;
				if(hit.isCapital()) nhits = nhits +5;
			}
			int documentMaxWordFrequency = document.getMaxWordFrequency ();// getDocumentMaxWordFrequency(documentId);
			double tf = ((double) nhits / documentMaxWordFrequency);
			documentHits.setMaxWordFrequency (documentMaxWordFrequency);
			documentHits.setTf (tf);
		}
		lexiconEntryLocal.setNdocs (ndocs);
		lexiconEntryLocal.setInvertedBarrel (invertedBarrel);
		putLexiconEntryLocal (lexiconEntryLocal);

	}

	//synchronized added now
	public  InvertedBarrel getInvertedBarrel (String wordId) {
		InvertedBarrel invertedBarrel = new InvertedBarrel ();
		ArrayList<DocumentHits> documentHitsList = new ArrayList<DocumentHits> ();

		Set<String> set = getDocumnetIds (wordId);
		Word word = getWord (wordId);
		String wordstr = word.getWordValue ();

		for (String documentId : set) {
			DocumentHits documentHits = getDocumnetHits (wordId, documentId);
			int nhits = documentHits.getNhits ();
			String url = getUrl (documentId);
			// System.out.println("The word "+wordstr+" appears in document   "+url+" "+nhits+"  times");
			// if(documentHits == null) continue;
			documentHitsList.add (documentHits);
		}

		invertedBarrel.setDocumentHitsList (documentHitsList);
		return invertedBarrel;
	}
	//synchronized added synchronized now
	public  DocumentHits getDocumnetHits (String wordId,
			String documentId) {

		DocumentHits documentHits = new DocumentHits ();
		documentHits.setDocumentId (documentId);
		String url = getUrl (documentId);
		documentHits.setUrl (url);
		HitsList hitsList = getHits (wordId, documentId);
		ArrayList<Hit> hits = hitsList.getHits ();
		String wordstr = getWordStr (wordId);
		// System.out.println();
		for (Hit hit : hits) {
			int position = hit.getPosition ();
			// System.out.println("The word "+wordstr+" appears in document  "+url+" at position "+position);
		}
		// System.out.println();
		documentHits.setHitsList (hitsList);

		int nhits = hitsList.size ();
		documentHits.setNhits (nhits);

		return documentHits;

	}

	// removed synchronized
	public   void createForwardIndexEntry (String documentId) {
		ForwardIndexEntry forwardIndexEntry = new ForwardIndexEntry ();
		forwardIndexEntry.setDocumentId (documentId);
		forwardIndexEntry.setUrl (getUrl (documentId));
		ForwardBarrel forwardBarrel = getForwardBarrel (documentId);
		// if(invertedBarrel == null) return null;
		int nwords = forwardBarrel.size ();
		forwardIndexEntry.setForwardBarrel (forwardBarrel);
		forwardIndexEntry.setNwords (nwords);
		int maxWordFrequency = forwardBarrel.getMaxWordFrequency ();// getMaxWordFrequency(forwardBarrel);
		forwardIndexEntry.setMaxWordFrequency (maxWordFrequency); // the
		// attribute
		// from
		// forward
		String url = getUrl (documentId);
		// System.out.println("Document: "+url+" contains  "+nwords+"  distinct words");
		// System.out.println("Document: "+url+" has max word frequency of "+maxWordFrequency);
		putForwardIndexEntry (forwardIndexEntry);
		System.out.println ("Created Forward Entry for document  : " + url);

	}

	// removed synchronized
	public  ForwardBarrel getForwardBarrel (String documentId) {

		ForwardBarrel forwardBarrel = new ForwardBarrel ();
		ArrayList<WordHits> wordHitsList = new ArrayList<WordHits> ();
		Set<Integer> numHits = new HashSet<Integer> ();

		Set<String> set = getWordIds (documentId);

		Document document = getDocument (documentId);
		String url = document.getUrl ();

		for (String wordId : set) {
			WordHits wordHits = getWordHits (wordId, documentId);
			String wordstr = getWordStr (wordId);
			// if(documentHits == null) continue;
			int nhits = wordHits.getNhits ();
			numHits.add (nhits);
			// System.out.println("The word "+wordstr+" appears in document   "+url+" "+nhits+"  times");

			wordHitsList.add (wordHits);
		}

		forwardBarrel.setWordHitsList (wordHitsList);

		// have to change this.....
		int maxWordFrequency = 1;
		if (numHits.size () > 0) {
			maxWordFrequency = Collections.max (numHits);
		}

		forwardBarrel.setMaxWordFrequency (maxWordFrequency);
		return forwardBarrel;
	}

	public  int getMaxWordFrequency (ForwardBarrel forwardBarrel) {
		Set<Integer> numHits = new HashSet<Integer> ();

		ArrayList<WordHits> wordHitsList = forwardBarrel.getWordHitsList ();
		for (WordHits wordHits : wordHitsList) {
			int nhits = wordHits.getNhits ();
			numHits.add (nhits);
		}

		int maxWordFrequency = Collections.max (numHits);

		return maxWordFrequency;
	}

	// removed synchronized
	public  WordHits getWordHits (String wordId, String documentId) {

		WordHits wordHits = new WordHits ();
		wordHits.setWordId (wordId);
		HitsList hitsList = getHits (wordId, documentId);
		ArrayList<Hit> hits = hitsList.getHits ();
		String wordstr = getWordStr (wordId);
		String url = getUrl (documentId);

		// System.out.println();
		for (Hit hit : hits) {
			int position = hit.getPosition ();
			// System.out.println("The word "+wordstr+" appears in document  "+url+" at position "+position);
		}
		// System.out.println();

		wordHits.setHitsList (hitsList);
		int nhits = hitsList.size ();
		wordHits.setNhits (nhits);
		return wordHits;

	}

	// removed synchronized
	public  synchronized HitsList getHits (String wordId, String documentId) {
		HitsList hitsList = new HitsList ();
		ArrayList<Hit> hits = new ArrayList<Hit> ();
		EntityJoin<Integer, WordDocumentHit> join = new EntityJoin<Integer, WordDocumentHit> (
				da.wordDocumentHitIndex);
		join.addCondition (da.wordFkIndex, wordId);
		join.addCondition (da.documentFkIndex, documentId);

		ForwardCursor<WordDocumentHit> wordDocumentHits = join.entities ();
		Hit hit;
		try {
			for (WordDocumentHit wordDocumentHit : wordDocumentHits) {
				hit = wordDocumentHit.getHit ();
				hits.add (hit);
			}

		} finally {
			wordDocumentHits.close ();
		}
		hitsList.setHits (hits);
		return hitsList;
	}

	
	// removed synchronized...
	public synchronized void createLexiconEntry (String wordId, String word,
			long totalNumDocs) {
		LexiconEntry lexiconEntry = new LexiconEntry ();
		// System.out.println("Word:"+word);
		lexiconEntry.setWordId (wordId);
		lexiconEntry.setWord (word);
		InvertedBarrel invertedBarrel = new InvertedBarrel ();
		InvertedBarrel otherInvertedBarrel;

		EntityCursor<LexiconEntryLocalReceived> lexiconEntries = da.wordFkLexiconEntryLocalReceivedIndex
				.subIndex (wordId).entities ();
		try {
			for (LexiconEntryLocalReceived lexiconEntryLocalReceived : lexiconEntries) {
				otherInvertedBarrel = lexiconEntryLocalReceived
						.getInvertedBarrel ();
				invertedBarrel.sumInvertedBarrel (otherInvertedBarrel);
			}
		} catch (DatabaseException dbe) {
			dbe.printStackTrace ();
		} finally {
			lexiconEntries.close ();
		}
		int ndocs = invertedBarrel.size ();

		// System.out.println("NDocs:"+ndocs);
		double idf = Math.log ( ((float) totalNumDocs / ndocs));
		lexiconEntry.setNdocs (ndocs);
		lexiconEntry.setMaxNumDocs (totalNumDocs);
		lexiconEntry.setIdf (idf);
		calculateWeight (invertedBarrel, idf);
		DocComparator comp = new DocComparator ();
		ArrayList<DocumentHits> documentHitsList = invertedBarrel
				.getDocumentHitsList ();
		Collections.sort (documentHitsList, comp);
		lexiconEntry.setInvertedBarrel (invertedBarrel);
		putLexiconEntry (lexiconEntry);

	}

	
	public synchronized void updateDocument(Document document){
		currDocument.update(document);
		
	}
	
	public synchronized void calculateDocumentWeights() {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);

		EntityCursor<Document> documents = da.documentIndex.entities (txn,null);
		
		try {
			for (Document document : documents) {
	              calculateDocumentWeight(document);
	              documents.update(document);			
			}
		 documents.close ();
		 documents = null;
		 txn.commit ();
		 txn = null;
	  } catch (Exception e) {
		if (documents != null) {
			documents.close ();
		}
		if (txn != null) {
			txn.abort ();
			txn = null;
		}
	}


	}
	
	public synchronized void calculateDocumentWeight(Document document){
		double documentWeight= 0;
		String documentId = document.getId();
		EntityCursor<DocumentLocalReceived > documentLocalReceivedEntries = da.documentFkDocumentLocalReceivedIndex
				.subIndex (documentId).entities ();
        try{
		for (DocumentLocalReceived documentLocalReceived : documentLocalReceivedEntries) {
			double localCalcWeight = documentLocalReceived.getCalcWeight();
			documentWeight = documentWeight+localCalcWeight;
		}
        }catch(DatabaseException dbe){
        	dbe.printStackTrace();
        }finally{
        	documentLocalReceivedEntries.close();
        }
        
        documentWeight = Math.sqrt(documentWeight);
		document.setDj(documentWeight);
		String url = document.getUrl();
		System.out.println("Calculated document weight for  "+url+".Weight: "+documentWeight);
		log.info("Calculated document weight for  "+url+".Weight: "+documentWeight);
	}
	
	
	public  void calculateWeight (InvertedBarrel invertedBarrel, double idf) {

		ArrayList<DocumentHits> documentHitsList = invertedBarrel
				.getDocumentHitsList ();
		for (DocumentHits documentHits : documentHitsList) {
			double tf = documentHits.getTf ();
			double weight = tf * idf;
			documentHits.setWeight (weight);
		}

	}

	// removed synchronized
	public   void createWordReceived () {
		HashMap<String, String> words = getWordLexiconEntriesLocalReceived ();
		Set<String> wordIds = words.keySet ();
		String wordstr;
		WordReceived wordReceived;
		for (String wordId : wordIds) {
			wordstr = words.get (wordId);
			wordReceived = new WordReceived ();
			wordReceived.setId (wordId);
			wordReceived.setWordValue (wordstr);
			putWordReceived (wordReceived);
		}

	}

	// removed synchronized
	public   void createDocumentReceived () {
		HashMap<String, String> words = getWordLexiconEntriesLocalReceived ();
		Set<String> wordIds = words.keySet ();
		String wordstr;
		WordReceived wordReceived;
		for (String wordId : wordIds) {
			wordstr = words.get (wordId);
			wordReceived = new WordReceived ();
			wordReceived.setId (wordId);
			wordReceived.setWordValue (wordstr);
			putWordReceived (wordReceived);
		}

	}

	// removed synchronized
	public  synchronized HashMap<String, String> getWordLexiconEntriesLocalReceived () {

		HashMap<String, String> words = new HashMap<String, String> ();

		EntityCursor<LexiconEntryLocalReceived> lexiconEntries = da.lexiconEntryLocalReceivedIndex
				.entities ();

		try {
			for (LexiconEntryLocalReceived lexiconEntryLocalReceived : lexiconEntries) {
				String wordId = lexiconEntryLocalReceived.getWordId ();
				String wordstr = lexiconEntryLocalReceived.getWord ();
				words.put (wordId, wordstr); // if duplicates it replaces the
				// previous value.
			}
		} catch (DatabaseException dbe) {
			dbe.printStackTrace ();
		} finally {
			lexiconEntries.close ();
		}
		return words;
	}

	
	// removed synchronized
	public synchronized  LexiconEntry getLexiconEntry (String wordId) {

		LexiconEntry lexiconEntry = null;

		try {
			lexiconEntry = da.lexiconEntryIndex.get (wordId);
			// System.out.println("document:"+document.getDocumentUrlString());
		} catch (DatabaseException dbe) {
			System.out.println ("LexiconEntryIndexGet:" + dbe.toString ());
			dbe.printStackTrace ();

		}
		return lexiconEntry;

	}
	// removed synchronized
	public synchronized void updateDocumentMaxWordFrequency () {

	}

}


/**
	fw.write (wordstr + " | " + numDocs + " | " + maxNumDocs
						+ " | " + idf + "\n");
				// System.out.println("\n"+wordstr+" | "+numDocs+" | "+maxNumDocs+" | "+idf+"\n");



**/