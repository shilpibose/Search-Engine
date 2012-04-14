package edu.upenn.cis555.restserver.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis555.crawler.bean.DocumentResult;
import edu.upenn.cis555.node.NodeManager;

public class DocumentSnippetManager {

	private static DocumentSnippetManager singleInstance;

	private DocumentSnippetManager () {

	}

	class DocumentSnippetRequest {

		private Hashtable<String, DocumentResult> docList = new Hashtable<String, DocumentResult> ();

		private Set<String> retrieveSet = new HashSet<String> ();

		public DocumentSnippetRequest (List<ResultScore> results) {
			for (ResultScore s : results) {
				retrieveSet.add (s.getDocURL ());
			}
		}

		public Hashtable<String, DocumentResult> getResult () {
			return docList;
		}

		public Set<String> getDocumentSet () {
			return retrieveSet;
		}

		public void setResultFor (String searchKey, DocumentResult result) {
			retrieveSet.remove (searchKey);
			if (result != null)
				docList.put (searchKey, result);
		}

		public boolean areAllResultsIn () {
			return retrieveSet.isEmpty ();
		}
	}

	/** Map to book keep the requests and responses */
	protected Map<String, ArrayList<DocumentSnippetRequest>> requestMap = new HashMap<String, ArrayList<DocumentSnippetRequest>> ();

	public ArrayList<DocumentResult> getDocumentSnippets (String searchString,
			List<ResultScore> results) {
		DocumentSnippetRequest docRequest = new DocumentSnippetRequest (results);

		synchronized (requestMap) {
			for (String s : docRequest.getDocumentSet ()) {
				System.err
						.println ("Sending Document Snippet request for " + s);
				ArrayList<DocumentSnippetRequest> requests = requestMap.get (s);
				if (requests != null) {
					requests.add (docRequest);
					System.err
							.println ("Request already present. Adding to it "
									+ s);
				} else {
					ArrayList<DocumentSnippetRequest> requestList = new ArrayList<DocumentSnippetRequest> ();
					requestList.add (docRequest);
					requestMap.put (s, requestList);
					System.err
							.println ("Calling Crawler to send the document list for "
									+ s);
					NodeManager.getInstance ().getCrawler ().getCrawlerApp ()
							.wrapperFetchSnippet (s, searchString);
				}
			}
		}

		/* Wait for the result to be filled in */
		synchronized (docRequest) {
			try {
				docRequest.wait ();
			} catch (InterruptedException e) {
				e.printStackTrace ();
			}
		}

		Hashtable<String, DocumentResult> result = docRequest.getResult ();
		ArrayList<DocumentResult> docResults = new ArrayList<DocumentResult> (
				result.values ());
		return docResults;
	}

	public void deliverResult (String searchString, DocumentResult docEntry) {
		System.err
				.println ("Delivering the Document Snippet result to the requested thread for "
						+ searchString);
		synchronized (requestMap) {
			ArrayList<DocumentSnippetRequest> requests = requestMap
					.get (searchString);
			if (requests == null) {
				System.err
						.println ("No requests waiting for the Document Snippet result?!");
			} else {
				System.err.println ("There are " + requests.size ()
						+ " requests waiting for the Document Snippet result");

				for (DocumentSnippetRequest r : requests) {
					r.setResultFor (searchString, docEntry);
					if (r.areAllResultsIn ()) {
						synchronized (r) {
							System.err
									.println ("All Document Snippet results are in for "
											+ r.getDocumentSet ()
											+ ", notifying the thread");
							r.notify ();
						}
					}
				}
				requestMap.remove (searchString);
			}
		}
	}

	public synchronized static DocumentSnippetManager getInstance () {
		if (singleInstance == null) {
			singleInstance = new DocumentSnippetManager ();
		}
		return singleInstance;
	}
}
