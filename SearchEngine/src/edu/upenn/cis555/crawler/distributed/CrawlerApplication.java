package edu.upenn.cis555.crawler.distributed;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;

import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.bean.DocumentBase;
import edu.upenn.cis555.crawler.bean.DocumentResult;
import edu.upenn.cis555.crawler.bean.SelectQueue;
import edu.upenn.cis555.crawler.distributed.MyMessage.messageType;
import edu.upenn.cis555.crawler.distributed.constants.CrawlerConstants;
import edu.upenn.cis555.crawler.distributed.util.DatabaseUtils;
import edu.upenn.cis555.crawler.distributed.util.ServerUtils;
import edu.upenn.cis555.node.NodeManager;
import edu.upenn.cis555.node.utils.NodeUtils;
import edu.upenn.cis555.pagerank.distributed.DocumentPageRankManager;
import edu.upenn.cis555.restserver.service.DocumentSnippetManager;
import edu.upenn.cis555.restserver.service.DocumentWeightManager;

public class CrawlerApplication extends Thread implements Application {

	private final Logger log = Logger.getLogger (CrawlerApplication.class);

	Node node;
	Endpoint endpoint;
	ServerSocket server;
	EntityStore store;
	DatabaseUtils dbUtils;
	Master queueCreator;
	Socket clientSocket;
	ArrayList<String> seedURL;
	boolean bootStrap;
	boolean shutDown;
	int createdNodeCount = 0;
	private Random generator;
	Boolean flagfirsttime;
	private long currentQueueID;
	public Long crawledDocuments;

	public long getCurrentQueueID () {
		return currentQueueID;
	}

	public void setCurrentQueueID (long currentQueueID) {
		this.currentQueueID = currentQueueID;
	}

	public boolean isRingComplete () {
		return ringComplete;
	}

	public void setRingComplete (boolean ringComplete) {
		this.ringComplete = ringComplete;
	}

	int ringSize;
	boolean ringComplete;
	InetSocketAddress boot;

	// public CrawlerApplication(seedUrl)
	public CrawlerApplication (Node node, EntityStore store,
			ArrayList<String> seedURL) {
		this.crawledDocuments = 0L;
		this.flagfirsttime = new Boolean (true);

		// log.debug("hello");
		this.node = node;
		// log.debug("hello  12");
		this.endpoint = this.node.buildEndpoint (this,
				"P2P Cache Server Application");
		// log.debug("hello 23");
		this.endpoint.register ();
		// log.debug("hello 34");
		this.store = store;
		this.dbUtils = new DatabaseUtils ();
		this.generator = new Random ();

		this.shutDown = false;

		this.seedURL = seedURL;
	}

	public boolean isShutDown () {
		return shutDown;
	}

	public void setShutDown (boolean shutDown) {
		this.shutDown = shutDown;
	}

	public boolean isBootStrap () {
		return bootStrap;
	}

	public void shutdown () {
		shutDown = true;
	}

	public void run () {
		queueCreator = new Master ("", Integer.parseInt (NodeUtils
				.getInstance ().getValue (Constants.MAX_NUM_OF_URLS)), store,
				Integer.parseInt (NodeUtils.getInstance ().getValue (
						Constants.MAX_FILE_SIZE)), this);
		queueCreator.start ();

		for (String link : seedURL) {
			System.err.println ("Seed URL: " + link);
			this.wrapperSendMessage ("" + " " + link);
			// Thread.sleep(5000);
			// this.frontEndWorker[(int)this.getCurrentQueueID()].add(link);
		}

		while (true) {
			try {
				Thread.sleep (10000);

				log.debug ("ShutDown " + shutDown);
				if (shutDown) {
					log.debug ("Application end ...");
					log.debug ("Application end ...");
					queueCreator.shutdown ();
					queueCreator.join ();
					break;
				}
			} catch (InterruptedException e) {
				e.printStackTrace ();
			}
		}
		log.debug ("Application ended - after while");
	}

	public int getCrawledDocuments () {
		return DatabaseUtils.retrieveCountObjects (store);
	}

	public void setCrawledDocuments (Long crawledDocuments) {
		this.crawledDocuments = crawledDocuments;
	}

	public void calculatePriority () {
		this.setCurrentQueueID ((long) generator
				.nextInt (CrawlerConstants.PRIORITY_MAX));
		// this.setCurrentQueueID(timestamp%(CrawlerConstants.PRIORITY_MAX-1));
	}

	@Override
	public void deliver (Id id, Message msg) {
		MyMessage newMessage = (MyMessage) msg;
		log.debug ("Received Message " + newMessage.getContent () + " from "
				+ newMessage.getFrom ());
		if (newMessage.wantResponse) {
			MyMessage reply = new MyMessage (this.node.getLocalNodeHandle (),
					"Message Received", MyMessage.messageType.ACK);
			reply.wantResponse = false;
			endpoint.route (null, reply, newMessage.getFrom ());
		}

		if (newMessage.getSelectedMessageType ().equals (
				messageType.GETINCOMING)) {
			Document inCrawled;
			try {
				inCrawled = DatabaseUtils.retrieveCrawledObject (this.store,
						Hash.hashSha1 (newMessage.getContent ()));
				System.err.println ("Crawled: " + inCrawled);
				MyMessage replyIncoming = new MyMessage (this.node
						.getLocalNodeHandle (), inCrawled,
						MyMessage.messageType.RESULTINCOMING);
				replyIncoming.wantResponse = false;
				endpoint.route (null, replyIncoming, newMessage.getFrom ());
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace ();
			}
		}
		if (newMessage.getSelectedMessageType ().equals (
				messageType.RESULTINCOMING)) {
			System.err.println (newMessage.getResultContent ());
			NodeManager.getInstance ().getPageRankHadoop ().deliverResult (
					newMessage.getResultContent ().getUrl (),
					newMessage.getResultContent ());
		}

		if (newMessage.getSelectedMessageType ().equals (messageType.QUERY)) {

			log.debug ("flagfirsttime+" + flagfirsttime);
			if (flagfirsttime) {
				this.calculatePriority ();
				Document newCrawled = new Document ();
				newCrawled.setOutgoingLinks (new HashSet<String> ());
				newCrawled.setHitcount (0);
				newCrawled.setUrl (newMessage.getContent ().split ("\\s+")[1]);
				HashSet<String> incomingLinks = new HashSet<String> ();
				incomingLinks.add (newMessage.getContent ().split ("\\s+")[0]);
				newCrawled.setIncomingLinks (incomingLinks);
				// log.debug("Master Thread :"+queueCreator.getName());

				log.debug ("The first queue");
				LinkedBlockingQueue<Document> firstQueue = queueCreator
						.getBackEndQueues ()[0];
				log.debug ("Before Insert");

				firstQueue.add (newCrawled);
				log.debug ("After Insert");
				try {
					queueCreator.getHostToQueue ().put (
							new URL (newCrawled.getUrl ()).getHost (),
							firstQueue);

					SelectQueue firstSelectedQueue = new SelectQueue ();
					firstSelectedQueue.setHost (new URL (newCrawled.getUrl ())
							.getHost ());
					firstSelectedQueue.setTimeInMillis (System
							.currentTimeMillis ());
					firstSelectedQueue.setHostQueue (firstQueue);
					queueCreator.getFrontEndQueue ()[0].add (newCrawled);

					queueCreator.getqMaster ().add (firstSelectedQueue);
					firstQueue.add (newCrawled);

					// Logger.debug("Query :"+newCrawled.getUrl()+" From :"+
					// newMessage.getFrom()+" To:"+ newMessage.getTo());
					log.debug ("I am in "
							+ this.getNode ().getLocalNodeHandle ());
					log.debug ("Query :" + newCrawled.getUrl () + " From :"
							+ newMessage.getFrom () + " To:"
							+ newMessage.getTo ());
					synchronized (this.flagfirsttime) {
						this.flagfirsttime = false;
					}
				} catch (MalformedURLException e) {

					e.printStackTrace ();
				}
			} else {
				log.debug ("NOt the first time");
				this.calculatePriority ();
				Document newCrawled = new Document ();
				newCrawled.setUrl (newMessage.getContent ().split ("\\s+")[1]);
				HashSet<String> incomingLinks = new HashSet<String> ();
				incomingLinks.add (newMessage.getContent ().split ("\\s+")[0]);
				newCrawled.setIncomingLinks (incomingLinks);
				// log.debug("Master Thread :"+queueCreator.getName());
				queueCreator.getFrontEndQueue ()[(int) this
						.getCurrentQueueID ()].add (newCrawled);
				log.debug ("I am in " + this.getNode ().getLocalNodeHandle ());
				log.debug ("Query :" + newCrawled.getUrl () + " From :"
						+ newMessage.getFrom () + " To:" + newMessage.getTo ());
			}
		}
		if (newMessage.getSelectedMessageType ().equals (messageType.FETCH)) {
			Document Crawled;
			try {
				Crawled = DatabaseUtils.retrieveCrawledObject (this.store, Hash
						.hashSha1 (newMessage.getContent ().split ("\\s+")[0]));

				DocumentResult result = new DocumentResult ();
				result.setUrl (newMessage.getContent ().split ("\\s+")[0]);
				result.setTitle (GenerateSnippet.getTitle (Crawled
						.getContent ()));
				result.setHits (Crawled.getHitcount ());
				result.setSnippet (GenerateSnippet.extractSnippet (Crawled
						.getContent (),
						newMessage.getContent ().split ("\\s+")[1]));
				System.err.println ("Sending snippet: <" + result + ">");

				MyMessage sendSnippet = new MyMessage (this.node
						.getLocalNodeHandle (), result,
						MyMessage.messageType.FETCH_RESULT);

				sendSnippet.wantResponse = false;
				endpoint.route (null, sendSnippet, newMessage.getFrom ());
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace ();
			}
		}
		if (newMessage.getSelectedMessageType ().equals (messageType.WEIGHT)) {
			try {
				Document crawled = DatabaseUtils.retrieveCrawledObject (
						this.store, Hash.hashSha1 (newMessage.getContent ()));

				DocumentBase baseDoc = new DocumentBase ();
				baseDoc.setDj (crawled.getDj ());
				baseDoc.setUrl (newMessage.getContent ());
				MyMessage sendSnippet = new MyMessage (this.node
						.getLocalNodeHandle (), baseDoc,
						MyMessage.messageType.WEIGHT_RESULT);
				sendSnippet.wantResponse = false;
				endpoint.route (null, sendSnippet, newMessage.getFrom ());
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace ();
			}
		}
		if (newMessage.getSelectedMessageType ().equals (
				messageType.FETCH_PAGERANK)) {
			try {
				Document Crawled = DatabaseUtils.retrieveCrawledObject (store,
						Hash.hashSha1 (newMessage.getContent ()));
				double pagerank = Crawled.getPagerank ();
				MyMessage sendPageRank = new MyMessage (this.node
						.getLocalNodeHandle (), newMessage.getContent () + " "
						+ String.valueOf (pagerank),
						MyMessage.messageType.RETURN_PAGERANK);
				sendPageRank.setWantResponse (false);
				endpoint.route (null, sendPageRank, newMessage.getFrom ());
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace ();
			}

		}
		if (newMessage.getSelectedMessageType ().equals (
				messageType.RETURN_PAGERANK)) {
			String url = newMessage.getContent ().split (" ")[0];
			String rank = newMessage.getContent ().split (" ")[1];
			DocumentPageRankManager.getInstance ().deliverResult (url,
					Double.parseDouble (rank));
		}
		if (newMessage.getSelectedMessageType ().equals (
				messageType.FETCH_RESULT)) {
			MyMessage sendSnippet = (MyMessage) msg;
			System.err.println ("Snippet: " + sendSnippet.getResult ());
			DocumentSnippetManager.getInstance ().deliverResult (
					sendSnippet.getResult ().getUrl (),
					sendSnippet.getResult ());
		}
		if (newMessage.getSelectedMessageType ().equals (
				messageType.WEIGHT_RESULT)) {
			MyMessage weightMessage = (MyMessage) msg;
			DocumentWeightManager.getInstance ().deliverResult (
					weightMessage.getDocBase ().getUrl (),
					weightMessage.getDocBase ().getDj ());
		}
	}

	public Node getNode () {
		return node;
	}

	public void setNode (Node node) {
		this.node = node;
	}

	@Override
	public boolean forward (RouteMessage arg0) {
		// this method will always return true
		return true;
	}

	@Override
	public void update (NodeHandle arg0, boolean arg1) {

	}

	public void wrapperSendMessage (String message) {
		log.debug ("SendMessageWrapper");
		String link = message.split ("\\s+")[1];
		try {
			URL linkURL = new URL (link);
			String host = linkURL.getHost ();

			MyMessage queryMessage = new MyMessage (this.endpoint
					.getLocalNodeHandle (), message, messageType.QUERY);
			queryMessage.setWantResponse (false);
			log.debug ("Destination ID: " + ServerUtils.getIdFromBytes (host));
			this.sendMessage (ServerUtils.getIdFromBytes (host), queryMessage);
		} catch (MalformedURLException e) {
			e.printStackTrace ();
		}
	}

	public void wrapperIncomingSendMessage (String url) {
		log.debug ("SendMessageWrapperIncoming");

		try {
			URL linkURL = new URL (url);
			String host = linkURL.getHost ();

			MyMessage queryMessage = new MyMessage (this.endpoint
					.getLocalNodeHandle (), url, messageType.GETINCOMING);
			queryMessage.setWantResponse (true);
			log.debug ("Destination ID for host: " + host + " is "
					+ ServerUtils.getIdFromBytes (host));
			this.sendMessage (ServerUtils.getIdFromBytes (host), queryMessage);
		} catch (MalformedURLException e) {
			e.printStackTrace ();
		}
	}

	public void sendMessage (Id idToSendTo, MyMessage message) {
		log.debug ("Send Message");
		endpoint.route (idToSendTo, message, null);
	}

	public void wrapperWeightSendMessage (String url) {
		log.info ("SendDocumentWeight");
		try {
			URL linkURL = new URL (url);
			String host = linkURL.getHost ();

			MyMessage queryMessage = new MyMessage (this.endpoint
					.getLocalNodeHandle (), url, messageType.WEIGHT);
			queryMessage.setWantResponse (true);
			log.info ("Destination ID: " + ServerUtils.getIdFromBytes (host));
			this.sendMessage (ServerUtils.getIdFromBytes (host), queryMessage);
		} catch (MalformedURLException e) {
			e.printStackTrace ();
		}
	}

	public void wrapperFetchSnippet (String url, String keyword) {
		log.debug ("SendMessageSnippet");

		try {
			URL linkURL = new URL (url);
			String host = linkURL.getHost ();

			MyMessage queryMessage = new MyMessage (this.endpoint
					.getLocalNodeHandle (), url + " " + keyword,
					messageType.FETCH);
			queryMessage.setWantResponse (true);
			log.debug ("Destination ID: " + ServerUtils.getIdFromBytes (host));
			this.sendMessage (ServerUtils.getIdFromBytes (host), queryMessage);
		} catch (MalformedURLException e) {
			e.printStackTrace ();
		}
	}

	public void wrapperFetchPageRank (String url) {
		URL linkURL;
		try {
			linkURL = new URL (url);
			String host = linkURL.getHost ();
			MyMessage msg = new MyMessage (this.endpoint.getLocalNodeHandle (),
					url, messageType.FETCH_PAGERANK);
			msg.setWantResponse (true);
			this.sendMessage (ServerUtils.getIdFromBytes (host), msg);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}

	}

}
