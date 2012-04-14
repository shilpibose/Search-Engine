package edu.upenn.cis555.crawler.distributed;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.common.CrawlerIndexer;
import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.bean.SelectQueue;
import edu.upenn.cis555.crawler.distributed.constants.CrawlerConstants;
import edu.upenn.cis555.crawler.distributed.util.DatabaseUtils;
import edu.upenn.cis555.node.utils.NodeUtils;

public class Master extends Thread {
	private final Logger log = Logger.getLogger (Master.class);
	private String seed;
	private boolean isShutDown;
	public static Integer count;
	private Hashtable<String, LinkedBlockingQueue<Document>> hostToQueue;
	private Hashtable<String, ArrayList<URL>> disallow;
	private PriorityBlockingQueue<SelectQueue> qMaster;
	private LinkedBlockingQueue[] frontEndQueue;
	private Hashtable<Worker, Boolean> terminate;
	private LinkedBlockingQueue[] backEndQueues;
	private String filePath;
	private File dbFolder;
	private EntityStore store;
	private DatabaseUtils dbUtils;
	private int maximumSize;
	private Worker[] threadPool;
	private Boolean flagFirstTime;
	private Boolean cached;
	private Hashtable<String, String> dnsCache;

	private CrawlerApplication application;

	public Master (String entry, Integer cnt, EntityStore store, int max,
			CrawlerApplication crawler) {
		this.store = store;

		/* this.filePath=filePath; */
		this.maximumSize = max;
		/*
		 * dbUtils= new DatabaseUtils(); dbFolder = new File(this.filePath);
		 */

		this.seed = entry;

		Master.count = cnt;
		this.hostToQueue = new Hashtable<String, LinkedBlockingQueue<Document>> ();
		this.qMaster = new PriorityBlockingQueue<SelectQueue> ();
		this.disallow = new Hashtable<String, ArrayList<URL>> ();
		this.backEndQueues = new LinkedBlockingQueue[Integer
				.parseInt (NodeUtils.getInstance ().getValue (
						Constants.NUM_CRAWLER_THREADS)) * 3];
		this.frontEndQueue = new LinkedBlockingQueue[CrawlerConstants.PRIORITY_MAX];
		this.threadPool = new Worker[Integer.parseInt (NodeUtils.getInstance ()
				.getValue (Constants.NUM_CRAWLER_THREADS))];
		this.terminate = new Hashtable<Worker, Boolean> ();
		this.flagFirstTime = true;
		this.dnsCache = new Hashtable<String, String> ();
		this.application = crawler;
		this.isShutDown = false;
		for (int i = 0; i < Integer.parseInt (NodeUtils.getInstance ()
				.getValue (Constants.NUM_CRAWLER_THREADS)) * 3; i++) {
			backEndQueues[i] = new LinkedBlockingQueue<Document> ();

		}
		for (int i = 0; i < CrawlerConstants.PRIORITY_MAX; i++) {
			frontEndQueue[i] = new LinkedBlockingQueue<Document> ();
		}

		/* try{ */

		URL seedURL;

		/* if(seed.equals("")){ */
		LinkedBlockingQueue<Document> firstQueue = this.backEndQueues[0];
		this.hostToQueue.put ("", firstQueue);

		// Crawled newCrawled = new Crawled();
		// newCrawled.setUrl("");
		// firstQueue.add(newCrawled);
		// this.hostToQueue.put("", firstQueue);
		// SelectQueue firstSelectedQueue = new SelectQueue();
		// firstSelectedQueue.setHost("");
		// firstSelectedQueue.setTimeInMillis(System.currentTimeMillis());
		// firstSelectedQueue.setHostQueue(firstQueue);
		// this.frontEndQueue[0].add(newCrawled);
		// this.qMaster.add(firstSelectedQueue);
		/*
		 * }else{
		 */
		/*
		 * while(iter.hasNext()){ seedURL = new URL(iter.next());
		 * 
		 * LinkedBlockingQueue<Crawled> firstQueue = this.backEndQueues[0];
		 * Crawled newCrawled = new Crawled();
		 * newCrawled.setUrl(seedURL.toString());
		 * newCrawled.setIncomingLinks(new ArrayList<String>());
		 * newCrawled.setOutgoingLinks(new ArrayList<String>());
		 * newCrawled.setHitcount(0); firstQueue.add(newCrawled);
		 * this.hostToQueue.put(seedURL.getHost(), firstQueue); SelectQueue
		 * firstSelectedQueue = new SelectQueue();
		 * firstSelectedQueue.setHost(seedURL.getHost());
		 * firstSelectedQueue.setTimeInMillis(System.currentTimeMillis());
		 * firstSelectedQueue.setHostQueue(firstQueue);
		 * this.frontEndQueue[0].add(newCrawled);
		 * this.qMaster.add(firstSelectedQueue); }
		 */
		// this.crawlCachedLinks();
		/*
		 * for(int i=0; i<CrawlerConstants.THREAD_POOL_SIZE ; i++){
		 * threadPool[i] = new Worker(this.hostToQueue, this.qMaster,
		 * this.frontEndQueue, this.backEndQueues,this.env, this.dbUtils,
		 * this.dbFolder,this.maximumSize, this.disallow,this.terminate,
		 * this.flagFirstTime, this.nodeFactory, this.application);
		 * 
		 * this.terminate.put(threadPool[i], true); threadPool[i].start(); }
		 */

		/*
		 * }catch (MalformedURLException e1) { // TODO Auto-generated catch
		 * block e1.printStackTrace(); }
		 */

	}

	public String getSeed () {
		return seed;
	}

	public void setSeed (String seed) {
		this.seed = seed;
	}

	public static Integer getCount () {
		return count;
	}

	public static void setCount (Integer count) {
		Master.count = count;
	}

	public Hashtable<String, LinkedBlockingQueue<Document>> getHostToQueue () {
		return hostToQueue;
	}

	public void setHostToQueue (
			Hashtable<String, LinkedBlockingQueue<Document>> hostToQueue) {
		this.hostToQueue = hostToQueue;
	}

	public Hashtable<String, ArrayList<URL>> getDisallow () {
		return disallow;
	}

	public void setDisallow (Hashtable<String, ArrayList<URL>> disallow) {
		this.disallow = disallow;
	}

	public PriorityBlockingQueue<SelectQueue> getqMaster () {
		return qMaster;
	}

	public void setqMaster (PriorityBlockingQueue<SelectQueue> qMaster) {
		this.qMaster = qMaster;
	}

	public LinkedBlockingQueue[] getFrontEndQueue () {
		return frontEndQueue;
	}

	public void setFrontEndQueue (LinkedBlockingQueue[] frontEndQueue) {
		this.frontEndQueue = frontEndQueue;
	}

	public Hashtable<Worker, Boolean> getTerminate () {
		return terminate;
	}

	public void setTerminate (Hashtable<Worker, Boolean> terminate) {
		this.terminate = terminate;
	}

	public LinkedBlockingQueue[] getBackEndQueues () {
		return backEndQueues;
	}

	public void setBackEndQueues (LinkedBlockingQueue[] backEndQueues) {
		this.backEndQueues = backEndQueues;
	}

	public String getFilePath () {
		return filePath;
	}

	public void setFilePath (String filePath) {
		this.filePath = filePath;
	}

	public File getDbFolder () {
		return dbFolder;
	}

	public void setDbFolder (File dbFolder) {
		this.dbFolder = dbFolder;
	}

	public DatabaseUtils getDbUtils () {
		return dbUtils;
	}

	public void setDbUtils (DatabaseUtils dbUtils) {
		this.dbUtils = dbUtils;
	}

	public int getMaximumSize () {
		return maximumSize;
	}

	public void setMaximumSize (int maximumSize) {
		this.maximumSize = maximumSize;
	}

	public Worker[] getThreadPool () {
		return threadPool;
	}

	public void setThreadPool (Worker[] threadPool) {
		this.threadPool = threadPool;
	}

	public Boolean getFlagFirstTime () {
		return flagFirstTime;
	}

	public void setFlagFirstTime (Boolean flagFirstTime) {
		this.flagFirstTime = flagFirstTime;
	}

	public Boolean getCached () {
		return cached;
	}

	public void setCached (Boolean cached) {
		this.cached = cached;
	}

	/*
	 * public void crawlCachedLinks(){
	 * 
	 * 
	 * 
	 * 
	 * this.env= new MyDatabaseEnvironment();
	 * this.env.setup(this.dbFolder,false); ArrayList<Crawled> listCrawled =
	 * DatabaseUtils.retrieveAllCrawledObjects(env); ArrayList<Channel>
	 * listChannel = DatabaseUtils.displayChannels(env); this.env.close();
	 * if(!listCrawled.isEmpty()){ for(Crawled url : listCrawled){
	 * Logger.debug("Cached link "+url.getUrl()); URL urlnext = null; try {
	 * urlnext = new URL(url.getUrl());
	 * 
	 * MyHttpClient client = new MyHttpClient(urlnext); client.sendGetMessage();
	 * if(url.getContentType().endsWith("xml")){ for(Channel channel :
	 * listChannel){
	 * 
	 * ArrayList<String> xpath = channel.getXpathRules(); xpathEval = new
	 * XPathEngine(xpath);
	 * 
	 * if(xpathEval.evaluate(url.getUrl())){ ChannelXML newXML = new
	 * ChannelXML(); newXML.setChannelID(channel.getChannelID());
	 * newXML.setXmlContent(client.getContent());
	 * newXML.setXmlURL(url.getUrl());
	 * newXML.setCrawledTime(ServerUtils.getCurrentDate
	 * ("yyyy-MM-dd'T'hh:mm:ss")); try {
	 * newXML.setId(channel.getChannelID().concat(Hash.hashSha1(url.getUrl())));
	 * } catch (NoSuchAlgorithmException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } this.insertDatabaseXML(newXML); }
	 * 
	 * } } }catch (MalformedURLException e1) { // TODO Auto-generated catch
	 * block e1.printStackTrace(); } } }
	 * 
	 * 
	 * 
	 * }
	 */
	public void shutdown () {
		isShutDown = true;
	}

	public void run () {
		ArrayList<Document> listCrawled = DatabaseUtils
				.retrieveAllCrawledObjects (this.store);
		for (Document nextDoc : listCrawled) {
			synchronized (application.crawledDocuments) {
				application.crawledDocuments++;
			}
			CrawlerIndexer.getInstance ().enqueue (nextDoc.getId ());
		}
		log.info ("Master " + this.getName () + " has started");
		for (int i = 0; i < Integer.parseInt (NodeUtils.getInstance ()
				.getValue (Constants.NUM_CRAWLER_THREADS)); i++) {
			threadPool[i] = new Worker (this.hostToQueue, this.qMaster,
					this.frontEndQueue, this.backEndQueues, this.store,
					this.dbUtils, this.dbFolder, this.maximumSize,
					this.disallow, this.terminate, this.flagFirstTime,
					this.application);

			this.terminate.put (threadPool[i], true);
			threadPool[i].start ();
			log.info ("Worker" + threadPool[i].getName () + " has started");
		}
		while (true) {
			try {
				Thread.sleep (10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace ();
			}
			// System.out.println("Master shutdown "+ isShutDown);
			if (isShutDown) {
				for (int i = 0; i < Integer.parseInt (NodeUtils.getInstance ()
						.getValue (Constants.NUM_CRAWLER_THREADS)); i++) {
					threadPool[i].shutDown ();
					if (threadPool[i].getState () == Thread.State.WAITING) {
						threadPool[i].shutDown();
						threadPool[i].interrupt ();
					}
				}

				for (int i = 0; i < Integer.parseInt (NodeUtils.getInstance ()
						.getValue (Constants.NUM_CRAWLER_THREADS)); i++) {
					try {
						threadPool[i].join ();
					} catch (InterruptedException e) {
						e.printStackTrace ();
					}
				}

				log.info ("Master Thread " + this.getName () + " has stopped");
				System.out.println ("Master Thread " + this.getName ()
						+ " has stopped");
				break;
			}
		}
	}
}
