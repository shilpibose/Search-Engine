package edu.upenn.cis555.crawler.distributed;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.impl.RefreshException;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.common.CrawlerIndexer;
import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.bean.Robots;
import edu.upenn.cis555.crawler.bean.SelectQueue;
import edu.upenn.cis555.crawler.distributed.constants.ClientConstants;
import edu.upenn.cis555.crawler.distributed.constants.CrawlerConstants;
import edu.upenn.cis555.crawler.distributed.util.DatabaseUtils;
import edu.upenn.cis555.node.NodeManager;
import edu.upenn.cis555.node.utils.Hash;
import edu.upenn.cis555.node.utils.NodeUtils;

public class Worker extends Thread {
	private final Logger log = Logger.getLogger (Worker.class);
	private Hashtable<String, LinkedBlockingQueue<Document>> workerHostToQueue;
	private PriorityBlockingQueue<SelectQueue> qSlave;
	private LinkedBlockingQueue[] frontEndWorker;
	private LinkedBlockingQueue[] backEndQueuesWorker;
	private MyHttpClient client;
	private ClientConstants constants = new ClientConstants();
	private EntityStore store;
	
	private HashSet<String> outlinks;
	private boolean shutDown;

	private ArrayList<String> disallowedLinks;
	private String actualURL;


	public CrawlerApplication getApplication() {
		return application;
	}

	public void setApplication(CrawlerApplication application) {
		this.application = application;
	}

	private CrawlerApplication application;

	public Hashtable<String, LinkedBlockingQueue<Document>> getWorkerHostToQueue() {
		return workerHostToQueue;
	}

	public void setWorkerHostToQueue(
			Hashtable<String, LinkedBlockingQueue<Document>> workerHostToQueue) {
		this.workerHostToQueue = workerHostToQueue;
	}

	public PriorityBlockingQueue<SelectQueue> getqSlave() {
		return qSlave;
	}

	public void setqSlave(PriorityBlockingQueue<SelectQueue> qSlave) {
		this.qSlave = qSlave;
	}

	public LinkedBlockingQueue[] getFrontEndWorker() {
		return frontEndWorker;
	}

	public void setFrontEndWorker(LinkedBlockingQueue[] frontEndWorker) {
		this.frontEndWorker = frontEndWorker;
	}

	public LinkedBlockingQueue[] getBackEndQueuesWorker() {
		return backEndQueuesWorker;
	}

	public void setBackEndQueuesWorker(LinkedBlockingQueue[] backEndQueuesWorker) {
		this.backEndQueuesWorker = backEndQueuesWorker;
	}

	public MyHttpClient getClient() {
		return client;
	}

	public void setClient(MyHttpClient client) {
		this.client = client;
	}

	public ClientConstants getConstants() {
		return constants;
	}

	public void setConstants(ClientConstants constants) {
		this.constants = constants;
	}

	public DatabaseUtils getDbUtils() {
		return dbUtils;
	}

	public void setDbUtils(DatabaseUtils dbUtils) {
		this.dbUtils = dbUtils;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public int getMaximumSize() {
		return maximumSize;
	}

	public void setMaximumSize(int maximumSize) {
		this.maximumSize = maximumSize;
	}

	public Document getCurrentCrawled() {
		return currentCrawled;
	}

	public void setCurrentCrawled(Document currentCrawled) {
		this.currentCrawled = currentCrawled;
	}

	

	public Random getGenerator() {
		return generator;
	}

	public void setGenerator(Random generator) {
		this.generator = generator;
	}

	public Hashtable<Worker, Boolean> getTerminate() {
		return terminate;
	}

	public void setTerminate(Hashtable<Worker, Boolean> terminate) {
		this.terminate = terminate;
	}

	public Boolean getFlagFirstTime() {
		return flagFirstTime;
	}

	public void setFlagFirstTime(Boolean flagFirstTime) {
		this.flagFirstTime = flagFirstTime;
	}

	public Boolean getCached() {
		return cached;
	}

	public void setCached(Boolean cached) {
		this.cached = cached;
	}

	/*
	 * public MyDatabaseEnvironment getEnv() { return env; }
	 */

	/*
	 * public void setEnv(MyDatabaseEnvironment env) { this.env = env; }
	 */
	private DatabaseUtils dbUtils;
	private File file;
	private int maximumSize;
	private Document currentCrawled;

	private Hashtable<String, ArrayList<URL>> disallow;
	
	private long currentQueueID;
	private Random generator;
	private Hashtable<Worker, Boolean> terminate;
	private Boolean flagFirstTime;
	private Boolean cached = true;

	public long getCurrentQueueID() {
		return currentQueueID;
	}

	public void setCurrentQueueID(long currentQueueID) {
		this.currentQueueID = currentQueueID;
	}

	public Worker(HashMap<String, LinkedBlockingQueue<URL>> hostToQueue) {
		// TODO Auto-generated constructor stub
	}

	public Worker(Hashtable<String, LinkedBlockingQueue<Document>> hostToQueue,
			PriorityBlockingQueue<SelectQueue> tempSlave,
			LinkedBlockingQueue[] frontend, LinkedBlockingQueue[] backend,
			EntityStore store, DatabaseUtils dbUtils, File file, int max,
			Hashtable<String, ArrayList<URL>> disallow,
			Hashtable<Worker, Boolean> terminateMe, Boolean flagFirstTime,
			CrawlerApplication application) {
		this.workerHostToQueue = hostToQueue;
		this.qSlave = tempSlave;
		this.frontEndWorker = frontend;
		this.backEndQueuesWorker = backend;
		this.store = store;
		this.dbUtils = dbUtils;
		this.file = file;
		this.maximumSize = max;
		this.disallow = disallow;
		this.generator = new Random();
		this.terminate = terminateMe;
		this.flagFirstTime = flagFirstTime;
		this.application = application;


	}


	public void calculatePriority() {

		this.setCurrentQueueID((long) generator
				.nextInt(CrawlerConstants.PRIORITY_MAX));
		// this.setCurrentQueueID(timestamp%(CrawlerConstants.PRIORITY_MAX-1));
	}

	public void populateBackEndQueues(SelectQueue queue) {
		boolean ifEnter = true;
		boolean currentQueue = false;
		ArrayList<Integer> priorityList = new ArrayList<Integer>();
		ArrayList<URL> removeURL = new ArrayList<URL>();
		LinkedBlockingQueue<Document> backend = this.workerHostToQueue
				.get(queue.getHost());
		int priority = CrawlerConstants.PRIORITY_MAX - 1;
		while (priority > 0) {
			for (Document nextURL : (LinkedBlockingQueue<Document>) this.frontEndWorker[priority]) {
				URL newURL = null;
				try {
					newURL = new URL(nextURL.getUrl());
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (newURL.getHost().equals(queue.getHost())) {
					if (!currentQueue) {
						backend.add(nextURL);
						removeURL.add(newURL);
						currentQueue = true;
						priorityList.add(priority);
					}

				} else {
					if (!currentQueue
							&& this.workerHostToQueue.containsKey(newURL
									.getHost())) {
						LinkedBlockingQueue<Document> otherQueue = this.workerHostToQueue
								.get(newURL.getHost());
						otherQueue.add(nextURL);
						removeURL.add(newURL);
						priorityList.add(priority);
					} else {
						if (!currentQueue) {
							backend.add(nextURL);
							removeURL.add(newURL);
							priorityList.add(priority);

							this.workerHostToQueue.remove(queue.getHost());
							this.workerHostToQueue.put(newURL.getHost(),
									backend);
							queue.setHost(newURL.getHost());
							queue.setTimeInMillis(System.currentTimeMillis());
							currentQueue = true;
						}

					}

				}

			}

			priority--;

		}
		for (URL remove : removeURL) {
			Document newRemove = new Document();
			newRemove.setUrl(remove.toString());
			for (Integer id : priorityList) {
				if (this.frontEndWorker[id].contains(newRemove)) {
					this.frontEndWorker[id].remove(newRemove);
				}
			}
		}

	}

	public boolean checkRobots(URL url) {
		Robots newRobot = DatabaseUtils.retrieveRobotsText(this.store, url.getHost());
		if(newRobot ==null){
			return true;
		}else{
		/*if (this.getDisallowedLinks()!=null) {
			ArrayList<String> links = this.getDisallowedLinks();
			for (String disallowedLink : links) {
				if (!disallowedLink.toString().endsWith("/")) {
					if (url.toString().endsWith("/")) {
						if (url.toString().substring(0,
								url.toString().length() - 1).equals(
								disallowedLink.toString())) {
							return false;
						}
					} else {
						if (url.toString().equals(disallowedLink.toString())) {
							return false;
						}
					}
				} else {
					if (url.toString().endsWith("/")) {
						if (url.toString().equals(disallowedLink.toString())) {
							return false;
						}
					} else {
						if (url.toString()
								.equals(
										disallowedLink.toString().substring(
												0,
												disallowedLink.toString()
														.length() - 1))) {
							return false;
						}

					}
				}

			}
		}
		return true;*/
		 String urlPath = url.getPath ();
	        if (newRobot.isUserAgentMatched()) {
	            log.info ("Looking into disallowed ones for this agent");
	            for (String disallowedPath : newRobot.getDisallowedDirsForUA()) {
	                if (urlPath.toLowerCase ().startsWith (
	                        disallowedPath.toLowerCase ())) {
	                    log.info ("Path " + urlPath + " not allowed to crawl");
	                    return false;
	                }
	            }
	        } else if (newRobot.isStarMatched()) {
	            log.info ("Looking into disallowed ones for *");
	            for (String disallowedPath : newRobot.getDisallowedDirsForAll()) {
	                if (urlPath.toLowerCase ().startsWith (
	                        disallowedPath.toLowerCase ())) {
	                    log.info ("Path " + urlPath
	                            + " not allowed to crawl under * rules");
	                    return false;
	                }
	            }
	        } else {
	            log.info ("No relevant rules in robot.txt");
	        }
		}
	        return true;

	}


	public Hashtable<String, ArrayList<URL>> getDisallow() {
		return disallow;
	}

	public void setDisallow(Hashtable<String, ArrayList<URL>> disallow) {
		this.disallow = disallow;
	}

	public void populateBackEndQueues() {
		ArrayList<URL> removeURL = new ArrayList<URL>();
		int queueID = (int) this.getCurrentQueueID();
		for (Document nextURL : (LinkedBlockingQueue<Document>) (this.frontEndWorker[queueID])) {
			URL newURL = null;
			try {
				newURL = new URL(nextURL.getUrl());
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			int index = -1;
			boolean entry = true;
			int i = 0;
			if (this.workerHostToQueue.containsKey(newURL.getHost())) {
				LinkedBlockingQueue<Document> backend = this.workerHostToQueue
						.get(newURL.getHost());
				backend.add(nextURL);
				removeURL.add(newURL);
			} else {
				for (i = 0; i < Integer.parseInt(NodeUtils.getInstance()
						.getValue(Constants.NUM_CRAWLER_THREADS)) * 3; i++) {
					if (this.backEndQueuesWorker[i].isEmpty() && entry) {
						index = i;
						entry = false;
					}
				}

				if (index < 0) {
					continue;
				}
				LinkedBlockingQueue<Document> newBackEndQ = backEndQueuesWorker[index];
				newBackEndQ.add(nextURL);
				removeURL.add(newURL);
				this.workerHostToQueue.put(newURL.getHost(), newBackEndQ);
				SelectQueue newQ = new SelectQueue();
				newQ.setHostQueue(newBackEndQ);
				newQ.setTimeInMillis(System.currentTimeMillis());
				newQ.setHost(newURL.getHost());
				this.qSlave.put(newQ);
			}
		}

		for (URL remove : removeURL) {
			Document newRemove = new Document();
			newRemove.setUrl(remove.toString());
			if (this.frontEndWorker[queueID].contains(newRemove)) {
				this.frontEndWorker[queueID].remove(newRemove);
			}
		}
	}

	public boolean checkURLHash(Document newCrawled) {
		try {
			log.debug("Hash database");
			/*
			 * this.env= new MyDatabaseEnvironment();
			 * this.env.setup(this.file,false);
			 */
			String urlstring = newCrawled.getUrl();
			currentCrawled = DatabaseUtils.retrieveCrawledObject(this.store,
					Hash.hashSha1(urlstring));
			if (currentCrawled != null) {
				/* this.env.close(); */
				if (this.client.getContentType().equals("http")
						&& !this.lastModifiedCheck(this.client
								.getLastModifiedTime())) {
					HashSet<String> links = currentCrawled.getIncomingLinks();
					for (String nextLink : newCrawled.getIncomingLinks()) {
						links.add(nextLink);
					}
					currentCrawled
							.setHitcount(currentCrawled.getHitcount() + 1);
					currentCrawled.setIncomingLinks(links);
					DatabaseUtils.insertCrawledObject(currentCrawled,
							this.store);
				}
				return false;
			} else {
				/* this.env.close(); */
				return true;
			}

		} catch (NoSuchAlgorithmException e) {
			/* this.env.close(); */
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}

	public boolean lastModifiedCheck(Long lastModified) {
		if (lastModified > currentCrawled.getLastVisitedTime()) {
			return true;
		} else {
			return false;
		}
	}

	public boolean checkContentHash(Document newCrawled) {
		log.debug("Content Hash db");
		/*
		 * this.env = new MyDatabaseEnvironment(); this.env.setup(this.file,
		 * false);
		 */
		currentCrawled = DatabaseUtils.retrieveCrawledFromContent(this.store,
				newCrawled.getShaContent());
		/* this.env.close(); */
		if (currentCrawled != null) {

			HashSet<String> links = currentCrawled.getIncomingLinks();

			for (String nextLink : newCrawled.getIncomingLinks()) {
				links.add(nextLink);
			}
			currentCrawled.setHitcount(currentCrawled.getHitcount() + 1);
			currentCrawled.setIncomingLinks(links);

			DatabaseUtils.insertCrawledObject(currentCrawled, this.store);

			return false;
		} else {
			return true;
		}
	}

	public void insertDatabase(Document newCrawledURL) {
		/*
		 * this.env = new MyDatabaseEnvironment();
		 * this.env.setup(this.file,false);
		 */
		DatabaseUtils.insertCrawledObject(newCrawledURL, this.store);
		/* this.env.close(); */
	}
	
	public boolean checkContentHash(String payload, Document temp) {
		try {
			if (Hash.hashSha1(payload).equals(temp.getShaContent())) {
				return false;
			} else {
				return true;
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	public boolean checkRobotsDB(String host){
		Robots robotsText=DatabaseUtils.retrieveRobotsText(this.store, host);
		if(robotsText!=null){
			
			
			return true;
		}else{
			return false;
		}
	}
	public void shutDown() {
		shutDown = true;
	}

	public void run() {
		boolean seedCheck = true;
		// boolean flagFirstTime = true;
		SelectQueue nextQueueSelected = null;
		while (Master.count > 0) /* && this.terminateMe()) */{

			try {
				URL url;
				// log.debug(this.getName()+" ghumio");
				/*
				 * if(this.backEndEmpty() && this.frontEndEmpty()){
				 * log.debug(this.getName() + "false" +
				 * this.getState().toString()); this.terminate.put(this, false);
				 * continue; }else{
				 * 
				 * this.terminate.put(this, true); }
				 */
				if (!this.qSlave.isEmpty() && !shutDown) {
					this.outlinks= new HashSet<String>();
					

					log.debug(this.getName() + "not empty"
							+ this.getState().toString());

					if (flagFirstTime) {
						// this.crawlCachedLinks();
						this.frontEndWorker[0].clear();
					}
					log.debug(this.getName() + "block qslave"
							+ this.getState().toString());

					// nextQueueSelected = www.google.comthis.qSlave.take();
					nextQueueSelected = this.qSlave.take();
					nextQueueSelected.setTimeInMillis((System
							.currentTimeMillis() - nextQueueSelected
							.getTimeInMillis())
							* 10 + System.currentTimeMillis());
					this.qSlave.add(nextQueueSelected);
					log.debug(this.getName() + "block qslave out"
							+ this.getState().toString());
					Document newCrawledRun;
					LinkedBlockingQueue<Document> nextQueueHandle = nextQueueSelected
							.getHostQueue();
					if (!nextQueueHandle.isEmpty()) {
						log.debug(this.getName()
								+ "block backend queue from qslav"
								+ this.getState().toString());
						newCrawledRun = nextQueueHandle.take();
						url = new URL(newCrawledRun.getUrl());

						log.debug(this.getName()
								+ "block backend queue from qslav out"
								+ this.getState().toString());

					} else {

						this.populateBackEndQueues(nextQueueSelected);
						log.debug(this.getName()
								+ "block backend queue from qslav 2"
								+ this.getState().toString());
						newCrawledRun = nextQueueHandle.take();
						url = new URL(newCrawledRun.getUrl());
						log.debug(this.getName()
								+ "block backend queue from qslav 2 out"
								+ this.getState().toString());

					}

					this.client = new MyHttpClient(url);
					if (url.toString().startsWith("https")) {
						/*
						 * nextQueueSelected.setTimeInMillis((System.currentTimeMillis
						 * ()-nextQueueSelected.getTimeInMillis())*10
						 * +System.currentTimeMillis());
						 * this.qSlave.add(nextQueueSelected);
						 */
						continue;
					}
				
					if (!this.checkRobotsDB(url.getHost())) {
						// for robots.txt
						Robots newRobot = this.client
								.retrieveDisallowedLinks();
						newRobot.setHost(url.getHost());
						
						DatabaseUtils.insertRobots(newRobot,this.store);
					}
					if (!checkRobots(url)) {
						log.info("Disallowed URL" + url.toString());
						System.out.println("Disallowed URL" + url.toString());
						//System.out.println("Robots" + url);
						/*
						 * nextQueueSelected.setTimeIn
						 * Millis((System.currentTimeMillis
						 * ()-nextQueueSelected.getTimeInMillis())*10
						 * +System.currentTimeMillis());
						 * this.qSlave.add(nextQueueSelected);
						 */
						continue;
					}
					actualURL = new String(url.toString());
					//System.out.println("Actual URL "+ actualURL);
					//System.out.println("OLD URL :"+url.toString());
				
					
					
					this.client.sendHeadRequest();
					
					if(this.client.getResponseCode() !=200){
						//System.out.println("Response code" + client.getResponseCode());
						continue;
					}
					// this.client.sendGetMessage();
					if (!(this.client.getContentLength() == 0)
							&& this.client.getContentLength() < this.maximumSize) {
						currentCrawled = null;
						//log.info("URL "+ this.client.getURL());
						//System.out.println("URL "+ this.client.getURL());
						this.client.sendGetMessage();
						newCrawledRun.setShaContent(Hash.hashSha1(this.client
								.getContent()));
						// when the url is not an html document
						if (!this.client.getContentType().endsWith("html") && !(this.client.getContentType().endsWith("xml"))) {
							System.out.println("Document neither html or xml");

							/*
							 * nextQueueSelected.setTimeInMillis((System.currentTimeMillis
							 * ()-nextQueueSelected.getTimeInMillis())*10
							 * +System.currentTimeMillis());
							 * this.qSlave.add(nextQueueSelected);
							 */
							if(this.client.getContentType().equals("text/plain")){
							if (!this.checkURLHash(newCrawledRun)
									|| !this.checkContentHash(newCrawledRun)) {
								// add to the incoming links

								continue;
							} else {
								log.info(nextQueueHandle.hashCode()
										+ " "
										+ application.getNode()
												.getLocalNodeHandle()
												.toString() + " "
										+ url.toString() + " Host "
										+ nextQueueSelected.getHost()
										+ " BackGroundQueue"
										+ nextQueueHandle.hashCode());
								/*log.debug(nextQueueHandle.hashCode()
										+ " Node :"
										+ application.getNode()
												.getLocalNodeHandle()
												.toString() + " "
										+ url.toString() + " Host "
										+ nextQueueSelected.getHost()
										+ " BackGroundQueue"
										+ nextQueueHandle.hashCode());*/
								currentCrawled = new Document();
								/* currentCrawledContent = new CrawledContent(); */
								currentCrawled.setUrl(actualURL);
								currentCrawled.setId(Hash.hashSha1(actualURL));
								currentCrawled.setContentLength(this.client
										.getContentLength());
								currentCrawled.setContentType(this.client
										.getContentType());
								currentCrawled.setIncomingLinks(newCrawledRun
										.getIncomingLinks());
								HashSet<String> outgoingLinks = new HashSet<String>();
								currentCrawled.setOutgoingLinks(outgoingLinks);
								if(currentCrawled.getOutgoingLinks().size()==0){
									currentCrawled.setDeadLink(true);
								}else{
									currentCrawled.setDeadLink(false);
								}
								currentCrawled.setShaContent(Hash
										.hashSha1(this.client.getContent()));
								currentCrawled.setContent(this.client
										.getContent());
								currentCrawled.setHitcount(1);
								currentCrawled.setPagerank(1);
								/*
								 * currentCrawledContent.setShaContent(Hash.hashSha1
								 * (this.client.getContent()));
								 * currentCrawledContent
								 * .setContent(this.client.getContent());
								 */
								currentCrawled.setLastVisitedTime(System
										.currentTimeMillis());

								log.debug("First database call 1");
								this.insertDatabase(currentCrawled);
								log.debug("Into Database :"
										+ currentCrawled.getUrl());

								// Logger.debug("Into Database :"+
								// currentCrawled.getUrl());

								/*
								 * this.env= new MyDatabaseEnvironment();
								 * this.env.setup(file, false);
								 */

								/* this.env.close(); */
								CrawlerIndexer.getInstance().enqueue(
										currentCrawled.getId());
								synchronized (application.crawledDocuments) {
									application.crawledDocuments++;
								}
								synchronized (Master.count) {

									Master.count--;
								}
							}

							continue;
							}
						}
						// when url is an html document
						if (!this.checkContentHash(newCrawledRun)) {
							// if the content is same skip this
							/*
							 * nextQueueSelected.setTimeInMillis((System.currentTimeMillis
							 * ()-nextQueueSelected.getTimeInMillis())*10
							 * +System.currentTimeMillis());
							 * this.qSlave.add(nextQueueSelected);
							 */
							continue;
						}
						if (!this.checkURLHash(newCrawledRun)) {
							// if the url is encountered for second time
							if (!this.lastModifiedCheck(this.client
									.getLastModifiedTime())) {
								// if the last modified time is in the past
								/*
								 * if(flagFirstTime){ seedCheck=false; break; }
								 */
								/*
								 * nextQueueSelected.setTimeInMillis((System.currentTimeMillis
								 * ()-nextQueueSelected.getTimeInMillis())*10
								 * +System.currentTimeMillis());
								 * this.qSlave.add(nextQueueSelected);
								 */
								continue;

							} else {
								outlinks = new HashSet<String>();
								outlinks = this.client.retrieveURLLinks();
								
								Document dbCrawled = DatabaseUtils
										.retrieveCrawledObject(store,
												newCrawledRun.getUrl());
								HashSet<String> links = dbCrawled
										.getIncomingLinks();
								if (dbCrawled.getIncomingLinks() == null) {
									dbCrawled
											.setIncomingLinks(new HashSet<String>());
								}
								if (newCrawledRun.getIncomingLinks() == null) {

									newCrawledRun
											.setIncomingLinks(new HashSet<String>());
								}
								for (String nextLink : newCrawledRun
										.getIncomingLinks()) {
									links.add(nextLink);
								}
								log.info(nextQueueHandle.hashCode()
										+ " "
										+ application.getNode()
												.getLocalNodeHandle()
												.toString() + " "
										+ url.toString() + " Host "
										+ nextQueueSelected.getHost()
										+ " BackGroundQueue"
										+ nextQueueHandle.hashCode());
							/*	log.debug(nextQueueHandle.hashCode()
										+ " Node : "
										+ application.getNode()
												.getLocalNodeHandle()
												.toString() + " "
										+ url.toString() + " Host "
										+ nextQueueSelected.getHost()
										+ " BackGroundQueue"
										+ nextQueueHandle.hashCode());*/
								currentCrawled = new Document();
								/* currentCrawledContent = new CrawledContent(); */
								currentCrawled.setUrl(actualURL);
								currentCrawled.setId(Hash.hashSha1(actualURL));
								currentCrawled.setContentLength(this.client
										.getContentLength());
								currentCrawled.setContentType(this.client
										.getContentType());
								currentCrawled.setIncomingLinks(links);
								currentCrawled.setHitcount(currentCrawled
										.getHitcount() + 1);
								currentCrawled.setPagerank(1);
								currentCrawled.setOutgoingLinks(outlinks);
								if(currentCrawled.getOutgoingLinks().size()==0){
									currentCrawled.setDeadLink(true);
								}else{
									currentCrawled.setDeadLink(false);
								}
								currentCrawled.setShaContent(Hash
										.hashSha1(this.client.getContent()));
								currentCrawled.setContent(this.client
										.getContent());
								/*
								 * currentCrawledContent.setShaContent(Hash.hashSha1
								 * (this.client.getContent()));
								 * currentCrawledContent
								 * .setContent(this.client.getContent());
								 */
								currentCrawled.setLastVisitedTime(System
										.currentTimeMillis());
								log.debug("First database call 2");
								this.insertDatabase(currentCrawled);
								log.debug("Into Database :"
										+ currentCrawled.getUrl());
								// Logger.debug("Into Database :"+
								// currentCrawled.getUrl());
								CrawlerIndexer.getInstance().enqueue(
										currentCrawled.getId());
								synchronized (application.crawledDocuments) {
									application.crawledDocuments++;
								}
								synchronized (Master.count) {
									Master.count--;
								}

							}
						} else {
							outlinks = new HashSet<String>();
							outlinks = this.client.retrieveURLLinks();

							HashSet<String> links = new HashSet<String>();
							if (newCrawledRun.getIncomingLinks() == null) {

								newCrawledRun
										.setIncomingLinks(new HashSet<String>());
							}
							// links null
							for (String nextLink : newCrawledRun
									.getIncomingLinks()) {
								links.add(nextLink);
							}
							log.info(nextQueueHandle.hashCode()
									+ " "
									+ application.getNode()
											.getLocalNodeHandle().toString()
									+ " " + url.toString() + " Host "
									+ nextQueueSelected.getHost()
									+ " BackGroundQueue"
									+ nextQueueHandle.hashCode());
							/*log.debug(nextQueueHandle.hashCode()
									+ " Node :"
									+ application.getNode()
											.getLocalNodeHandle().toString()
									+ " " + url.toString() + " Host "
									+ nextQueueSelected.getHost()
									+ " BackGroundQueue"
									+ nextQueueHandle.hashCode());*/
							currentCrawled = new Document();
							/* currentCrawledContent = new CrawledContent(); */
							currentCrawled.setUrl(actualURL);
							currentCrawled.setId(Hash.hashSha1(actualURL));
							currentCrawled.setContentLength(this.client
									.getContentLength());
							currentCrawled.setContentType(this.client
									.getContentType());
							currentCrawled.setIncomingLinks(links);
							currentCrawled.setOutgoingLinks(outlinks);
							if(currentCrawled.getOutgoingLinks().size()==0){
								currentCrawled.setDeadLink(true);
							}else{
								currentCrawled.setDeadLink(false);
							}
							currentCrawled.setHitcount(1);
							currentCrawled.setPagerank(1);
							currentCrawled.setShaContent(Hash
									.hashSha1(this.client.getContent()));
							currentCrawled.setContent(this.client.getContent());
							/*
							 * currentCrawledContent.setShaContent(Hash.hashSha1(
							 * this.client.getContent()));
							 * currentCrawledContent.
							 * setContent(this.client.getContent());
							 */
							currentCrawled.setLastVisitedTime(System
									.currentTimeMillis());
							log.debug("First database call 3");
							this.insertDatabase(currentCrawled);
							log.debug("Into Database :"
									+ currentCrawled.getUrl());
							// Logger.debug("Into Database :"+
							// currentCrawled.getUrl());
							CrawlerIndexer.getInstance().enqueue(
									currentCrawled.getId());
							synchronized (application.crawledDocuments) {
								application.crawledDocuments++;
							}
							synchronized (Master.count) {
								Master.count--;
							}
						}

						// this.calculatePriority(this.client.getLastModifiedTime());
						for (String link : outlinks) {
							//System.out.println("Sent Link" +link);
							NodeManager.getInstance().getQueueStats().canSend();
							this.application.wrapperSendMessage(actualURL
									+ " " + link);
							
							// Thread.sleep(5000);
							// this.frontEndWorker[(int)this.getCurrentQueueID()].add(link);
						}
						/*
						 * nextQueueSelected.setTimeInMillis((System.currentTimeMillis
						 * ()-nextQueueSelected.getTimeInMillis())*10
						 * +System.currentTimeMillis());
						 * this.qSlave.add(nextQueueSelected);
						 */
						if (flagFirstTime) {
							this.populateBackEndQueues();
							synchronized (this.flagFirstTime) {
								flagFirstTime = false;
							}
							/*
							 * synchronized(Master.count){ Master.count--; }
							 */
							continue;
						}
						if (nextQueueHandle.isEmpty()) {
							this.populateBackEndQueues(nextQueueSelected);
						}

					}// not needed to terminate if the first time doesnt satisfy
					/*
					 * else{ if(flagFirstTime){ seedCheck=false; //TODO notify
					 * other threads break; }
					 * nextQueueSelected.setTimeInMillis((
					 * System.currentTimeMillis
					 * ()-nextQueueSelected.getTimeInMillis())*10
					 * +System.currentTimeMillis());
					 * this.qSlave.add(nextQueueSelected); }
					 */
					/*
					 * synchronized(Master.count){ Master.count--; }
					 */
//				} else {
//				log.debug("Worker thread " + this.getName()
//							+ " has stopped working");
//					log.debug("Worker thread " + this.getName()
//							+ " has stopped working");
//					break;
				}
			} catch (RefreshException e1) {
				log.info("Refresh");
			} catch (InterruptedException e) {
				log.info("Worker " + this.getName() + " has interrupted");
				log.info("interrupt");
				break;
			} catch (Exception e) {
				log.debug("in Exception catch" + this.getName());
				/*
				 * synchronized(Master.count){ Master.count--; }
				 */
				/*
				 * nextQueueSelected.setTimeInMillis((System.currentTimeMillis()-
				 * nextQueueSelected.getTimeInMillis())*10
				 * +System.currentTimeMillis());
				 * this.qSlave.add(nextQueueSelected);
				 */
				log.debug(e.toString());
			}
		}
		log.info("Worker " + this.getName() + " has terminated");
		log.info(this.getName() + "terminated"
				+ this.getState().toString());
	}

	public void setDisallowedLinks(ArrayList<String> disallowedLinks) {
		this.disallowedLinks = disallowedLinks;
	}

	public ArrayList<String> getDisallowedLinks() {
		return disallowedLinks;
	}

}
