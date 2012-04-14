package edu.upenn.cis555.indexer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.tidy.Tidy;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.common.CrawlerIndexer;
import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.distributed.WebGraphText;
import edu.upenn.cis555.node.utils.NodeUtils;
import edu.upenn.cis555.node.utils.Stemmer;
import edu.upenn.cis555.node.utils.StopWords;
import edu.upenn.cis555.node.utils.Utility;

/**
 * This class is used to fetch a document from the database,deletes that
 * document from the databse and then parses it. It stores the wordId and
 * documentId pairs in the database.
 * 
 * @author Rahul
 * 
 */
public class Indexer implements Runnable {
	private final Logger log = Logger.getLogger (Indexer.class);

	Thread t;
	int id; // id of this thread
	MyStore store; // the database store
	SAXHandler h; // parser to parse the xml document
	Document currentDocument;
	Counter counter;
	Stemmer stemmer;
	int numDocsParsed;
	int wordPosition;
	boolean boldFlag;
	boolean titleFlag;
	int printCount;

	public void setNumDocsParsed (int numDocsParsed) {
		this.numDocsParsed = numDocsParsed;
	}

	Hashtable<String, Integer> words;// URLConnectionReader reader;
	volatile boolean quit;
	static final String SPLIT_EXPR = "([,<.>/?'\":;\\[{\\]}\\+|=\\-_)(*&^%$#@!`~])|(\\s+)";
	static final String WORD_EXPR = "[a-zA-Z]+";
	Pattern pattern;
	Pattern wordPattern;
	// ArrayList<WordDocumentHit> wordDocumentHitList;
	int documentCount;
	String filepath;
	File file;

	FileWriter fw;

	public void quit () {
		quit = true;
		documentCount = 0;

	}

	public boolean isBoldFlag () {
		return boldFlag;
	}

	public void setBoldFlag (boolean boldFlag) {
		this.boldFlag = boldFlag;
	}

	public boolean isTitleFlag () {
		return titleFlag;
	}

	public void setTitleFlag (boolean titleFlag) {
		this.titleFlag = titleFlag;
	}

	public Indexer (MyStore myStore, int id) {
		this.store = myStore;
		this.id = id;
		numDocsParsed = 0;
		h = new SAXHandler ();
		counter = new Counter ();
		stemmer = new Stemmer ();
		// reader = new URLConnectionReader ();
		quit = false;
		pattern = Pattern.compile (SPLIT_EXPR);
		wordPattern = Pattern.compile (WORD_EXPR);
		// wordDocumentHitList = new ArrayList<WordDocumentHit> ();
		wordPosition = 0;
		printCount = 9999;
		boldFlag = false;
		this.t = new Thread (this, "Indexer: " + id);
		this.t.start ();
	}

	Indexer () {
	}

	@Override
	public void run () {
		// openFile ();
		try {
			while (!quit) {
				// System.out.println("Trying to deque...");
				String documentId = CrawlerIndexer.getInstance ().dequeue ();
				// System.out.println("dequed !!!...");
				if (documentId == null)
					continue; // to keep check that what we got is not null.
				Document document = this.store.getDocument (documentId);
				if (document == null)
					continue; // if the document is not in the database.
				int docCount = getDocumentCount ();
				setDocumentCount (docCount + 1);
				currentDocument = document;
				words = new Hashtable ();
				// System.out.println (" ");
				// System.out.print ("Thread:" + Thread.currentThread () +
				// "  ");
				// System.out.print ("ID: " + document.getId () + "   ");
				// System.out
				// .print ("TYPE: " + document.getContentType () + "   ");
				// System.out.println (document.getUrl ());
				String url = document.getUrl ();
				counter.reset ();

				if (document.getContentType ().endsWith ("xml")) {// ("text/xml"))
					// {
					indexXML (document); // ends wwith xml....

					System.out.println ("Parsed XML document");
					log.info ("Parsed XML document");
				} else if (document.getContentType ().equalsIgnoreCase (
						"text/html")) {
					// indexPlainText(document);

					indexHtml (document);
					System.out.println ("Parsed HTML document");
					log.info ("Parsed HTML document");

				} else if (document.getContentType ().equalsIgnoreCase (
						"text/plain")) {
					indexPlainText (document);
					System.out.println ("Parsed PlainText document");
					log.info ("Parsed PlainText document");

				} else {
					System.out.println ("Document not valid type:"
							+ document.getContentType ());
					log.info ("Document not valid type:"
							+ document.getContentType ());

				}
				updateDocumentMaxWordFrequency ();

				/*
				 * if(getDocumentCount() == 15){if( setDocumentCount(0); }
				 */

				/*
				 * if (wordDocumentHitList.size () > 50000) {
				 * putAllWordDocumentHits (); }
				 */
			}
		} catch (InterruptedException e) {

			// System.out.println
			// ("Got exception from dequeue..thread  terminating...");

		}
		/*
		 * putAllWordDocumentHits ();
		 */
		// closeFile ();
	}

	public void openFile () {
		try {
			filepath = NodeUtils.getInstance ().getValue (
					Constants.DICTIONARY_FILE).concat (
					InetAddress.getLocalHost ().getHostAddress ()).concat (
					id + "");
		} catch (UnknownHostException e) {
			e.printStackTrace ();
		}

		file = new File (filepath);
		try {
			fw = new FileWriter (file);
		} catch (IOException e) {
			e.printStackTrace ();
		}

	}

	public void closeFile () {
		try {
			fw.flush ();
			fw.close ();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

	}

	public void indexHTML (Document document) {
		String url = document.getUrl ();

		String fileString = document.getContent ();
		// System.out.println (fileString);
		InputStream is = null;
		ByteArrayOutputStream byte1 = new ByteArrayOutputStream ();
		try {
			is = new ByteArrayInputStream (fileString.getBytes ("UTF-8"));
			Tidy tidy = new Tidy (); // obtain a new Tidy instance
			boolean xhtml = true;
			// tidy.setXHTML (xhtml); // set desired config options using tidy
			// setters

			tidy.setXmlOut (true);
			String error = "error.txt";
			tidy.setErrfile (error);
			tidy.setShowWarnings (false);

			tidy.parse (is, byte1);
			String s = byte1.toString ();
			// System.out.println ("Starting to print xml");
			// System.out.println (s);
			int indexHtml = s.indexOf ("<html");

			if (indexHtml != -1) {
				s = s.substring (indexHtml);
				// System.out.println (s);
			}
			is = new ByteArrayInputStream (s.getBytes ("UTF-8"));
			parse (is);
			// store.getAllWords ();
		} catch (UnsupportedEncodingException e) {
			System.out.println ("Got Unsupported Excpetion");
			log.error (e);
			// e.printStackTrace ();
		} catch (IOException e1) {
			System.out.println ("Got IOExcpetion");
			log.error (e1);
			// e1.printStackTrace ();
		}
	}

	public void indexHtml (Document document) {
		InputStream is = null;

		ByteArrayOutputStream errout = new ByteArrayOutputStream ();
		PrintWriter writer = new PrintWriter (errout, true);
		String content = document.getContent ();
		if (content == null)
			return;
		org.w3c.dom.Document doc = null;
		try {
			is = new ByteArrayInputStream (content.getBytes ("UTF-8"));
			Tidy tidy = new Tidy (); // obtain a new Tidy instance
			tidy.setShowWarnings (false);
			// tidy.setHideComments(true);
			// tidy.setShowErrors(0);
			// tidy.setErrfile("err.txt");
			tidy.setErrout (writer);
			doc = tidy.parseDOM (is, new FileOutputStream (new File (
					"filejtidy.txt")));

			parseHtml (doc);

		} catch (IOException e) {
			e.printStackTrace ();// TODO: handle exception
			System.out.println ("Caught IOException in parseDom");
			log.error ("SAXEXCEPTION: URL: " + currentDocument.getUrl ());
			log.error (e);
		} catch (StringIndexOutOfBoundsException ste) {
			log.error (ste);
			System.out
					.println ("Caught StringIndexOutOfBoundsExceptionContents in parseDom");

		}
	}

	public void parseHtml (org.w3c.dom.Document document) {
		{
			numDocsParsed = numDocsParsed + 1;

			Node rootElement = null;
			rootElement = document.getDocumentElement ();
			parseText (rootElement);

		}
	}

	public void parseText (Node node) {
		if (node.getNodeType () == node.TEXT_NODE) {
			String text = node.getNodeValue ();
			String[] values = pattern.split (text);
			for (String value : values) {
				// if (value.startsWith ("http://")) {
				// httpParse (value);
				// continue;
				// }
				wordPosition = counter.next ();
				value = value.trim ();
				if (value.length () < 2)
					continue;
				processWord (value);
			}
			return;
		}

		if (node.getNodeType () == node.ELEMENT_NODE) {
			String value = node.getNodeName ();
			if (value.equalsIgnoreCase ("b")) {
				setBoldFlag (true);
			}

			if (value.equalsIgnoreCase ("title")) {
				setTitleFlag (true);
			}
		}

		NodeList nodeChildren = null;
		nodeChildren = node.getChildNodes ();
		int size = nodeChildren.getLength ();

		if (size == 0) {
			setBoldFlag (false);
			setTitleFlag (false);
			return;
		}

		for (int i = 0; i < size; i++) {
			parseText (nodeChildren.item (i));
		}
		setBoldFlag (false);
		setTitleFlag (false);
	}

	/**
	 * 
	 * This method takes a document and breaks it into words.It puts the words
	 * in the database.
	 * 
	 */
	public void indexXML (Document document) {
		String fileString = document.getContent ();
		InputStream is = null;
		try {
			is = new ByteArrayInputStream (fileString.getBytes ("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			System.out.println ("Got UnsupportedExcpetion");
			log.error (e);
			// e.printStackTrace ();
		}
		parse (is);
		numDocsParsed = numDocsParsed + 1;

		// store.getAllWords ();
		// System.out.println ("No of words :" + store.sizeWords ());
	}

	public void indexPlainText (Document document) {
		String contents = document.getContent ();
		String[] values = pattern.split (contents);
		System.out.println ("Parsed PlainText document");

		for (String value : values) {
			wordPosition = counter.next ();
			value = value.trim ();
			if (value.length () < 2)
				continue;

			// if (value.startsWith ("http://")) {
			// httpParse (value);
			// continue;
			// }
			processWord (value);
		}

	}

	public void parse (InputStream is) {
		h.setIndexer (this);
		try {
			SAXParser parser = SAXParserFactory.newInstance ().newSAXParser ();
			// File file = new File("./webpages/MiddleEast.xml");
			// parser.parse(file,h);
			parser.parse (is, h);

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			// numDocsNotParsed =numDocsNotParsed+1;

			System.out.println ("Got ParserConfiguredExcpetion");
			log.error (e);
			// e.printStackTrace ();

		} catch (SAXException e) {
			// TODO Auto-generated catch block

			System.out.println ("Got SAXExcpetion");
			log.error ("SAXEXCEPTION: URL: " + currentDocument.getUrl ());
			log.error (e);
			// e.printStackTrace ();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			// numDocsNotParsed =numDocsNotParsed+1;

			System.out.println ("Got IOExcpetion");
			log.error (e);
			// e.printStackTrace ();

		}

	}

	public void startElementHandler (String qName, Attributes attributes) {
		// System.out.println("StartElement:"+qName+":");

	}

	public void endElementHandler (String qName) {
		// numDocsParsed =numDocsParsed+1;

		// System.out.println("EndElement:"+qName+":");

	}

	public void endDocumentHandler () {

		// System.out.println("EndElement:"+qName+":");

	}

	public void characterHandler (String contents) {
		if (contents.matches ("\\s+"))
			return;
		if (contents.equals (""))
			return;
		// System.out.println("Chracters:"+contents+":");
		String[] values = pattern.split (contents);// ("\\s+");
		String stemWord;
		for (String value : values) {
			wordPosition = counter.next ();
			value = value.trim ();
			if (value.length () < 2)
				continue;

			// if (value.startsWith ("http://")) {
			// httpParse (value);
			// continue;
			// }
			processWord (value);
		}

	}

	public void httpParse (String value) {
		value = value.substring ("http://".length ());
		if (value.contains (".html?")) {
			int index = value.indexOf (".html?");
			value = value.substring (0, index);
		}
		String[] values = value.split ("/");
		for (String string : values) {
			processWord (string);
		}
	}

	public void processWord (String wordstr) {
		if (wordstr.length () > 16)
			return; // we do not store words which are greater tah 20
		// characters.
		if (wordstr.matches ("\\s+"))
			return;
		if (wordstr.equals (""))
			return;
		if (wordstr.length () < 2)
			return;

		wordstr = wordstr.toLowerCase ();
		Matcher m = wordPattern.matcher (wordstr);
		if (!m.matches ())
			return;

		// try {
		// // fw.write(wordstr+" ");
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace ();
		// }
		if (StopWords.isStopWord (wordstr))
			return;
		char[] chararray = wordstr.toCharArray ();
		int len = chararray.length;
		stemmer.add (chararray, len);
		stemmer.stem ();
		String stemWord = stemmer.toString ();

		stemWord = stemWord.trim ();
		if (stemWord.length () < 2)
			return; // added to check you do not have zero length strings or
		// words after stemming.
		if (words.containsKey (stemWord)) {
			int count = words.get (stemWord);
			count = count + 1;
			words.put (stemWord, count);
		} else {
			words.put (stemWord, 1);
		}

		Word word = addWord (stemWord);
		addWordDocumentHit (word);

	}

	public void addWordDocumentHit (Word word) {
		// System.out.println("Add wordDocument");
		String wordstr = word.getWordValue ();
		Hit hit = new Hit ();
		hit.setPosition (wordPosition);
		if (isBoldFlag ())
			hit.setBold (true);
		if (isTitleFlag ())
			hit.setTitle (true);
		if (wordstr.length () > 1) {
			char firstchar = wordstr.charAt (0);
			if (Character.isLetter (firstchar)
					&& Character.isUpperCase (firstchar))
				hit.setCapital (true);
		}

		WordDocumentHit wordDocumentHit = new WordDocumentHit ();
		String docId = currentDocument.getId ();
		wordDocumentHit.setDocumentFk (docId);
		String wordId = word.getId ();
		wordDocumentHit.setWordFk (wordId);
		wordDocumentHit.setHit (hit);
		// System.out.println("Storing worddocumentHit in database");
		store.putWordDocumentHit (wordDocumentHit);// commented to test..
		// wordDocumentHitList.add (wordDocumentHit); // commented to test

		printCount = printCount + 1;
		if (printCount == 10000) {
			printCount = 0;
			System.out.println ("Stored worddocumentHit for word: " + wordstr
					+ " in database");
			log.info ("Stored worddocumentHit for word: " + wordstr
					+ "  in database");
		}
	}

	public Word addWord (String stemWord) {

		String id = new String (Utility.hash (stemWord));
		Word word = null;
		// check if word is there in database if not there store the word
		if (!store.containsWord (id)) {
			word = new Word ();
			word.setId (id);
			word.setWordValue (stemWord);
			store.putWord (word);
			return word;
		} else {
			word = store.getWord (id);
			return word;
		}
	}

	public int getNumDocsParsed () {
		return numDocsParsed;
	}

	public void updateDocumentMaxWordFrequency () {
		Collection<Integer> c = words.values ();
		if (c.size () > 0) {
			int max = (Integer) Collections.max (c);
			// System.out.println("Max frequency:"+max);
			currentDocument.setMaxWordFrequency (max);
			store.putDocument (currentDocument);
		} else {
			currentDocument.setMaxWordFrequency (0); // if no words then max
			// word frequency
			// zero...
			store.putDocument (currentDocument);

		}
	}

	/*
	 * public void putAllWordDocumentHits () { System.out.println
	 * ("Starting to put wordDocumentHits in database..."); System.out.println
	 * ("wordDocumentHits size: " + wordDocumentHitList.size ());
	 * 
	 * for (WordDocumentHit wordDocumentHit : wordDocumentHitList) {
	 * store.putWordDocumentHit (wordDocumentHit);// commented to test.. }
	 * 
	 * int count = getDocumentCount (); setDocumentCount (0);
	 * 
	 * System.out.println (wordDocumentHitList.size () +
	 * ": wordDocumentHits in  " + count + ": documents put in database !!!");
	 * wordDocumentHitList = new ArrayList<WordDocumentHit> ();
	 * 
	 * }
	 */
	public int getDocumentCount () {
		return documentCount;
	}

	public void setDocumentCount (int documentCount) {
		this.documentCount = documentCount;
	}

}

/*
 * 
 * 
 * //String stemWordInitial = StopWords.stemInitial (wordstr); //if
 * (stemWordInitial == null) // return;
 * 
 * 
 * public void parseBoldText(Node node){ if(node.getNodeType() ==
 * node.TEXT_NODE){ String text = node.getNodeValue(); String[] values =
 * pattern.split(text); for (String value : values) { // if (value.startsWith
 * ("http://")) { // httpParse (value); // continue; // } return; } }
 * 
 * 
 * NodeList nodeChildren = null; nodeChildren = node.getChildNodes(); int size =
 * nodeChildren.getLength();
 * 
 * if(size == 0) return;
 * 
 * for(int i = 0; i< size;i++){ parseText(nodeChildren.item(i)); }
 * 
 * }
 * 
 * 
 * public void updateBold(org.w3c.dom.Document doc){ NodeList nodeList =
 * doc.getElementsByTagName ("b"); for (int i= 0; i < nodeList.getLength ();
 * i++) { Node node = nodeList.item (i); parseBoldText(node); }
 * 
 * }
 */
