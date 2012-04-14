package edu.upenn.cis555.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseExistsException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

import rice.pastry.PastryNode;

import edu.upenn.cis555.command.CommandType;
import edu.upenn.cis555.common.ClientSocket;
import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.common.CrawlerIndexer;
import edu.upenn.cis555.crawler.distributed.P2PCrawler;
import edu.upenn.cis555.crawler.distributed.util.DatabaseUtils;
import edu.upenn.cis555.indexer.Index;
import edu.upenn.cis555.node.utils.NodeUtils;
import edu.upenn.cis555.pagerank.distributed.PageRankDist;
import edu.upenn.cis555.pagerank.hadoop.PageRankHadoop;
import edu.upenn.cis555.restserver.RestServer;

public class NodeManager {

	private static final Logger log = Logger.getLogger (NodeManager.class);

	private static NodeManager singleInstance;

	protected ClientSocket clientSocket;

	protected NodeFactory nodeFactory;

	protected PastryNode node;

	protected QueueStats queueStats;

	protected Environment dbEnv;

	protected EntityStore store;

	// Crawler Object
	protected P2PCrawler crawler;

	protected long totalDocsCrawled = 0;

	// Indexer Object
	protected Index indexer;

	// Pagerank Object
	protected PageRankHadoop pagerankHadoop;

	protected PageRankDist pagerankDist;

	protected boolean nodeRestarted = true;

	public void initDb () throws UnknownHostException {
		String dbDir = NodeUtils.getInstance ().getValue (Constants.DB_DIR)
				+ "_" + InetAddress.getLocalHost ().getHostAddress ();
		if (dbDir == null) {
			throw new DatabaseNotFoundException ("Unknown DB Store");
		}

		File envDir = new File (dbDir);
		if ( (envDir.exists ()) && ! (envDir.isDirectory ())) {
			throw new DatabaseExistsException (
					"A non directory of same name exists");
		}

		if (!envDir.exists ()) {
			envDir.mkdir ();
		}
		EnvironmentConfig myEnvConfig = new EnvironmentConfig ();
		myEnvConfig.setReadOnly (false);
		myEnvConfig.setAllowCreate (true);
		myEnvConfig.setTransactional (true);
		dbEnv = new Environment (envDir, myEnvConfig);

		StoreConfig storeConfig = new StoreConfig ();
		storeConfig.setReadOnly (false);
		storeConfig.setTransactional (true);
		storeConfig.setAllowCreate (true);
		store = new EntityStore (dbEnv, Constants.DB_STORE_NAME, storeConfig);
	}

	private NodeManager () throws FileNotFoundException, IOException {
		String bootStrapNodeAddress = NodeUtils.getInstance ().getValue (
				Constants.BOOTSTRAP_ADDRESS);
		int bootStrapNodePort = Integer.parseInt (NodeUtils.getInstance ()
				.getValue (Constants.BOOTSTRAP_PORT));
		nodeFactory = new NodeFactory (bootStrapNodePort,
				new InetSocketAddress (bootStrapNodeAddress, bootStrapNodePort));
		// Initialize the Pastry Node
		node = nodeFactory.getNode ();
		System.err.println ("Initialized Pastry Node with ID: "
				+ node.getId ().toStringFull ());

		initDb ();
		System.err.println ("Created the database");
	}

	public QueueStats getQueueStats () {
		return queueStats;
	}

	public long getTotalDocs () {
		return totalDocsCrawled;
	}

	public P2PCrawler getCrawler () {
		return crawler;
	}

	public Index getIndexer () {
		return indexer;
	}

	public PageRankHadoop getPageRankHadoop () {
		return pagerankHadoop;
	}

	public PageRankDist getPageRankDistributed () {
		return pagerankDist;
	}

	protected void connectToController () throws UnknownHostException,
			IOException {
		String serverAddress = NodeUtils.getInstance ().getValue (
				Constants.CONTROLLER_ADDRESS);
		int serverPort = Integer.parseInt (NodeUtils.getInstance ().getValue (
				Constants.CONTROLLER_PORT));

		Socket socket = new Socket (serverAddress, serverPort);
		clientSocket = new ClientSocket (socket);
		System.err.println ("Connected to Controller");
	}

	protected void instantiateTasks () throws IOException {
		// Instantiate crawler objectrestServer =
		String seedsUrlStr = NodeUtils.getInstance ().getValue (
				Constants.SEED_URLS);
		String[] seedUrls = seedsUrlStr.split ("[ ]+");
		ArrayList<String> seedUrlList = new ArrayList<String> (Arrays
				.asList (seedUrls));
		System.err
				.println ("Received " + seedUrlList.size () + " URLs to seed");

		queueStats = new QueueStats (node);

		crawler = new P2PCrawler (seedUrlList, store, node);

		// Instantiate Indexer object
		indexer = new Index (store, node);

		pagerankDist = new PageRankDist (store, node);

		pagerankHadoop = new PageRankHadoop (store);

		queueStats.start ();

		clientSocket.writeLine (CommandType.NODE_UP.toString ());
		System.err.println ("Node is up and ready to receive commands");
	}

	public String receivedCommand (String line) {
		String[] components = line.split ("[ \t]+");
		return components[0];
	}

	public long receivedCommandArg (String line) {
		String[] components = line.split ("[ \t]+");
		return Long.parseLong (components[1]);
	}

	protected void manageInstance () throws IOException, InterruptedException {
		connectToController ();
		instantiateTasks ();

		do {
			String line = clientSocket.readLine ();
			String command = receivedCommand (line);
			System.err.println ("Received command: " + command);

			if (command.equals (CommandType.CRAWL_INDEX_START.toString ())) {
				// Node is being started fresh
				nodeRestarted = false;

				// Start Crawler
				crawler.startCrawling ();
				System.err.println ("Started Crawler");

				indexer.startLocalIndexing ();
				System.err.println ("Started Indexer");

			} else if (command.equals (CommandType.CRAWL_INDEX_END.toString ())) {
				// Stop the Crawler
				long numDocsCrawled = crawler.stopCrawling ();
				System.err.println ("Stopped Crawler");

				while (true) {
					if (CrawlerIndexer.getInstance ().getQueueLength () == 0) {
						break;
					}
					Thread.sleep (10 * 1000);
				}

				numDocsCrawled = DatabaseUtils.retrieveCountObjects (store);
				System.err.println ("Number of documents Crawled: "
						+ numDocsCrawled);

				// Send Local Num Docs Crawled
				indexer.quitLocalIndexing ();

				numDocsCrawled = DatabaseUtils.retrieveCountObjects (store);

				clientSocket.writeLine (CommandType.LOCAL_NUM_DOCS_CRAWLED
						.toString ()
						+ " " + numDocsCrawled);
				System.err.println ("Stopped Local Indexer");

			} else if (command.equals (CommandType.GLOBAL_NUM_DOCS_CRAWLED
					.toString ())) {
				// Save in a DB
				totalDocsCrawled = receivedCommandArg (line);
				System.err.println (totalDocsCrawled
						+ " have been crawled in ring");

			} else if (command.equals (CommandType.GLOBAL_INDEX_START
					.toString ())) {
				// Start reducing the indices

				indexer.startGlobalIndexing ();
				System.err.println ("Global Indexing Complete");

				clientSocket.writeLine (CommandType.GLOBAL_INDEX_COMPLETE
						.toString ());

			} else if (command.equals (CommandType.DOCUMENT_WEIGHT_CALC_START
					.toString ())) {

				indexer.startDocumentWeightCalculation ();
				System.err.println ("Document weight calculation complete");

				clientSocket
						.writeLine (CommandType.DOCUMENT_WEIGHT_CALC_COMPLETE
								.toString ());

			} else if (command.equals (CommandType.DISTRIBUTED_PAGE_RANK_START
					.toString ())) {
				// Start distributed page rank

				pagerankDist.startRanking (Integer.parseInt (NodeUtils
						.getInstance ().getValue (Constants.CONVERGENCE_TIME)));
				clientSocket
						.writeLine (CommandType.DISTRIBUTED_PAGE_RANK_COMPLETE
								.toString ());

			} else if (command
					.equals (CommandType.OPEN_REST_SERVER.toString ())) {
				// Load the Pagerank File now
				pagerankHadoop.loadPageRankFile ();

				RestServer.instantiate (Integer.parseInt (NodeUtils
						.getInstance ().getValue (Constants.REST_SERVER_PORT)));
				RestServer.getInstance ().startServer ();
				System.err.println ("Started Rest Server");

			} else if (command.equals (CommandType.CLOSE_REST_SERVER
					.toString ())) {
				if (RestServer.getInstance () != null) {
					RestServer.getInstance ().shutDownServer ();
					System.err.println ("Stopped Rest Server");
				}

			}
		} while (true);
	}

	public static NodeManager getInstance () {
		return singleInstance;
	}

	public static void main (String args[]) throws FileNotFoundException,
			IOException, InterruptedException {
		String propertiesFile = args[0];
		System.err.println ("Loading properties file: " + propertiesFile);

		NodeUtils.Instantiate (propertiesFile);
		singleInstance = new NodeManager ();
		singleInstance.manageInstance ();
	}

	@Override
	protected void finalize () throws Throwable {
		store.close ();
		dbEnv.close ();
		super.finalize ();
	}
}
