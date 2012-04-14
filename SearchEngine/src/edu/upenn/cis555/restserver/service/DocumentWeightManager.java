package edu.upenn.cis555.restserver.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.upenn.cis555.node.NodeManager;

/**
 * Singleton class to act as a mediator between the REST Server and Pastry Node
 */
public class DocumentWeightManager {

	private final Logger log = Logger.getLogger (DocumentWeightManager.class);

	private static DocumentWeightManager singleInstance;

	/**
	 * Class containing the query string to which the result will be filled in
	 */
	class DocumentWeightRequest {
		private String url;
		private double result;

		public DocumentWeightRequest (String str) {
			url = str;
		}

		public double getResult () {
			return result;
		}

		public void setResult (double res) {
			result = res;
		}
	}

	/** Map to book keep the requests and responses */
	private Map<String, ArrayList<DocumentWeightRequest>> requestMap = new HashMap<String, ArrayList<DocumentWeightRequest>> ();

	private DocumentWeightManager () {

	}

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
	public double getResult (String url) {
		DocumentWeightRequest documentWeightRequest = new DocumentWeightRequest (
				url);
		synchronized (requestMap) {
			ArrayList<DocumentWeightRequest> requests = requestMap.get (url);
			if (requests != null) {
				requests.add (documentWeightRequest);
			} else {
				ArrayList<DocumentWeightRequest> requestList = new ArrayList<DocumentWeightRequest> ();
				requestList.add (documentWeightRequest);
				requestMap.put (url, requestList);
				NodeManager.getInstance ().getCrawler ().getCrawlerApp ()
						.wrapperWeightSendMessage (url);
			}
		}

		/* Wait for the result to be filled in */
		synchronized (documentWeightRequest) {
			try {
				documentWeightRequest.wait ();
			} catch (InterruptedException e) {
				e.printStackTrace ();
			}
		}
		return documentWeightRequest.getResult ();
	}

	/**
	 * P2PCache calls this method to deliver the result, which will issue to all
	 * the requests waiting for it.
	 * 
	 * @param searchString
	 * @param videoFeed
	 */
	public void deliverResult (String searchString, double weight) {
		log.info ("Delivering the Document Weight result to the requested thread");
		synchronized (requestMap) {
			ArrayList<DocumentWeightRequest> requests = requestMap
					.get (searchString);
			if (requests == null) {
				log.info ("No requests waiting for the Document Weight result?!");
			} else {
				log.info ("There are " + requests.size ()
						+ " Document Weight requests waiting for the result");
				for (DocumentWeightRequest r : requests) {
					r.setResult (weight);
					synchronized (r) {
						r.notify ();
					}
				}
				requestMap.remove (searchString);
			}
		}
	}

	public synchronized static DocumentWeightManager getInstance () {
		if (singleInstance == null) {
			singleInstance = new DocumentWeightManager ();
		}
		return singleInstance;
	}
}
