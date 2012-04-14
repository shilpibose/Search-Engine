package edu.upenn.cis555.restserver.service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.crawler.bean.DocumentResult;
import edu.upenn.cis555.indexer.DocumentHits;
import edu.upenn.cis555.indexer.LexiconEntry;
import edu.upenn.cis555.indexer.Word;
import edu.upenn.cis555.node.NodeManager;
import edu.upenn.cis555.node.utils.Spelling;
import edu.upenn.cis555.node.utils.Stemmer;
import edu.upenn.cis555.node.utils.StopWords;
import edu.upenn.cis555.pagerank.PageRank;

/**
 * Singleton class to act as a mediator between the REST Server and Pastry Node
 */
public class ServiceManager {

	private static double TF_IDF_SCORE = 100;

	private static double PAGE_RANK_SCORE = 1;

	private final Logger log = Logger.getLogger (ServiceManager.class);

	private static ServiceManager singleInstance;

	private ServiceManager () {

	}

	protected String processWord (String word) {
		if (word.length () > 16)
			return null;
		if (word.matches ("\\s+"))
			return null;
		if (word.equals (""))
			return null;
		if (word.length () < 2)
			return null;

		word = word.toLowerCase ();

		if (StopWords.isStopWord (word))
			return null;
		char[] chararray = word.toCharArray ();
		int len = chararray.length;

		Stemmer stemmer = new Stemmer ();
		stemmer.add (chararray, len);
		stemmer.stem ();
		String stemWord = stemmer.toString ();

		stemWord = stemWord.trim ();
		if (stemWord.length () < 2)
			return null;

		return stemWord;
	}

	/**
	 * Class containing the query string to which the result will be filled in
	 */
	class SearchRequest {

		private Hashtable<String, String> stemmedToOrig = new Hashtable<String, String> ();

		private Hashtable<String, LexiconEntry> searchList = new Hashtable<String, LexiconEntry> ();

		private Set<String> searchSet = new HashSet<String> ();

		public SearchRequest (String str) {
			String[] elements = str.split (Constants.SPLIT_EXPR);
			System.err.println ("New query: " + str + " with "
					+ elements.length);

			for (String s : elements) {
				System.err.println ("Elements in query" + s);
				if (s.length () != 0) {
					String processedWord = processWord (s);
					if (processedWord != null) {
						stemmedToOrig.put (processedWord, s);
						searchSet.add (processedWord);
					}
				}
			}
		}

		public Hashtable<String, LexiconEntry> getResult () {
			Hashtable<String, LexiconEntry> result = new Hashtable<String, LexiconEntry> ();
			for (String s : searchList.keySet ()) {
				result.put (stemmedToOrig.get (s), searchList.get (s));
			}
			return result;
		}

		public Set<String> getSearchWords () {
			return searchSet;
		}

		public void setResultFor (String searchKey, LexiconEntry result) {
			searchSet.remove (searchKey);
			if (result != null)
				searchList.put (searchKey, result);
		}

		public boolean areAllResultsIn () {
			return searchSet.isEmpty ();
		}
	}

	/** Map to book keep the requests and responses */
	protected Map<String, ArrayList<SearchRequest>> requestMap = new HashMap<String, ArrayList<SearchRequest>> ();

	/**
	 * A REST request handler thread calls this method and waits on the
	 * SearchRequest object it creates. After the result is received, this
	 * thread will be notified that the result has been put into the SearchQuery
	 * object
	 * 
	 * @param searchString
	 * @return
	 * @throws InterruptedException
	 */
	protected SearchRequest getResult (String searchString)
			throws InterruptedException {
		SearchRequest searchRequest = new SearchRequest (searchString);
		synchronized (requestMap) {
			for (String s : searchRequest.getSearchWords ()) {
				System.err.println ("Sending search request for " + s);
				ArrayList<SearchRequest> requests = requestMap.get (s);
				if (requests != null) {
					System.err.println ("There is already a req for " + s);
					requests.add (searchRequest);
				} else {
					ArrayList<SearchRequest> requestList = new ArrayList<SearchRequest> ();
					requestList.add (searchRequest);
					requestMap.put (s, requestList);
					System.err
							.println ("Calling Indexer to send the document list for "
									+ s);
					NodeManager.getInstance ().getIndexer ().getIndexerApp ()
							.sendResultReqMessage (s);
				}
			}
		}

		/* Wait for the result to be filled in */
		synchronized (searchRequest) {
			searchRequest.wait ();
		}
		return searchRequest;
	}

	/**
	 * P2PCache calls this method to deliver the result, which will issue to all
	 * the requests waiting for it.
	 * 
	 * @param searchString
	 * @param videoFeed
	 */
	public void deliverResult (String searchString, LexiconEntry lexEntry) {
		System.err
				.println ("Delivering the result to the requested thread for "
						+ searchString);
		synchronized (requestMap) {
			ArrayList<SearchRequest> requests = requestMap.get (searchString);
			if (requests == null) {
				System.err.println ("No requests waiting for the result?! "
						+ searchString);
			} else {
				System.err.println ("There are " + requests.size ()
						+ " requests waiting for the result");

				boolean allNotified = true;
				for (SearchRequest r : requests) {
					r.setResultFor (searchString, lexEntry);
					if (r.areAllResultsIn ()) {
						synchronized (r) {
							System.err.println ("All results are in for "
									+ r.getSearchWords ()
									+ ", notifying the thread");
							r.notify ();
							allNotified = true & allNotified;
						}
					} else {
						allNotified = false;
					}
				}

				// if (allNotified) {
				System.err.println ("Search String: " + searchString
						+ " cleared");
				requestMap.remove (searchString);
				// }
			}
		}
	}

	public String getSearchResults (String searchString, int batchNumber,
			String method) throws InterruptedException {
		System.err.println ("Got request for " + searchString + ", "
				+ batchNumber + ", " + method);

		SearchRequest searchStrResult = getResult (searchString);
		Hashtable<String, LexiconEntry> result = searchStrResult.getResult ();
		// If there was no match for any word, send empty result
		if (result.size () == 0) {
			return getSearchResultsXML (new ArrayList<DocumentResult> (),
					searchString);
		}

		Hashtable<DocumentHits, Double> hits = new FaginsAlgorithm (result)
				.calculateResult ();

		ArrayList<ResultScore> resultScores = new ArrayList<ResultScore> ();

		PageRank pagerank = null;
		System.err.println ("Method:" + method);
		if (method.equals (Constants.METHOD_HADOOP)) {
			pagerank = NodeManager.getInstance ().getPageRankHadoop ();
		} else if (method.equals (Constants.METHOD_DISTRIBUTED)) {
			pagerank = NodeManager.getInstance ().getPageRankDistributed ();
		}

		for (DocumentHits d : hits.keySet ()) {
			double tfIdfScore = hits.get (d) * TF_IDF_SCORE;
			double pageRankScore = pagerank.getPageRankFor (d.getUrl ())
					* PAGE_RANK_SCORE;
			double score = tfIdfScore + pageRankScore;

			System.err.println (d.getDocumentId () + ", " + d.getUrl ());
			System.err.println ("TF/IDF Score: " + tfIdfScore
					+ ", PageRank Score: " + pageRankScore);
			System.err.println ("\n\n");

			resultScores.add (new ResultScore (d.getUrl (), score));
		}
		System.err.println ("\n\n\n");

		Collections.sort (resultScores, new ResultScoreComparator ());
		for (ResultScore r : resultScores) {
			System.err.println (r.getDocURL () + " = " + r.getScore ());
		}

		System.err
				.println ("Sending back " + resultScores.size () + " results");

		int startIndex = 0;
		int endIndex = 0;

		startIndex = (batchNumber - 1) * Constants.NUM_RESULTS;
		endIndex = (batchNumber) * Constants.NUM_RESULTS;

		if (resultScores.size () <= startIndex) {
			return getSearchResultsXML (new ArrayList<DocumentResult> (),
					searchString);
		}

		if (resultScores.size () < endIndex) {
			endIndex = resultScores.size ();
		}

		return getSearchResultsXML (DocumentSnippetManager.getInstance ()
				.getDocumentSnippets (searchString,
						resultScores.subList (startIndex, endIndex)),
				searchString);
	}

	public synchronized static ServiceManager getInstance () {
		if (singleInstance == null) {
			singleInstance = new ServiceManager ();
		}
		return singleInstance;
	}

	public String getSearchResultsXML (ArrayList<DocumentResult> results,
			String keyword) {
		StringBuffer content = new StringBuffer (
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		content.append ("<results>");
		content.append ("<keyword>" + keyword + "</keyword>");
		content.append ("<suggestion>"
				+ Spelling.getInstance ().correct (keyword) + "</suggestion>");

		for (DocumentResult doc : results) {
			try {
				String url = doc.getUrl ();
				if (url != null)
					content.append ("<link>"
							+ URLEncoder.encode (encodeString (url), "UTF-8")
							+ "</link>");

				String title = doc.getTitle ();
				if (title == null) {
					title = "";
				}
				content.append ("<title>"
						+ URLEncoder.encode (encodeString (title), "UTF-8")
						+ "</title>");

				String snippet = doc.getSnippet ();
				if (snippet == null) {
					snippet = "";
				}
				content.append ("<snippet>"
						+ URLEncoder.encode (encodeString (snippet), "UTF-8")
						+ "</snippet>");

				int hits = doc.getHits ();
				content.append ("<hits>" + hits + "</hits>");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace ();
			}
		}
		content.append ("</results>");

		String resultStr = null;
		try {
			resultStr = new String (content.toString ().getBytes ("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace ();
		}
		return resultStr;
	}

	public String encodeString (String s) {
		s = s.replace ("&amp;", "&");
		return s;
	}
}
