package edu.upenn.cis555.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Class representing the Event Queue of HTTP Connection requests
 */
public class CrawlerIndexer {

	private static final Logger log = Logger.getLogger (CrawlerIndexer.class);

	protected List<String> docIdQueue = new ArrayList<String> ();

	private static CrawlerIndexer singleInstance;

	private CrawlerIndexer () {

	}

	/**
	 * Enqueue a new request from a client
	 */
	public void enqueue (String docId) {
		synchronized (docIdQueue) {
			docIdQueue.add (docId);
			log.debug ("Enqueued request to the message queue");
			log.debug("Notifying a thread of a new request");
			docIdQueue.notify ();
			log.debug("Notification successful");
		}
	}

	/**
	 * Dequeue a request from the front of the Queue. If empty, the calling
	 * thread waits on the Queue object
	 */
	public String dequeue () throws InterruptedException {
		synchronized (docIdQueue) {
			while (docIdQueue.size () == 0) {
				docIdQueue.wait ();
			}
			log.debug ("Dequeuing a connection request");
			return docIdQueue.remove (0);
		}
	}

	public int getQueueLength () {
		synchronized (docIdQueue) {
			return docIdQueue.size ();
		}
	}

	public synchronized static CrawlerIndexer getInstance () {
		if (singleInstance == null) {
			singleInstance = new CrawlerIndexer ();
		}
		return singleInstance;
	}
}
