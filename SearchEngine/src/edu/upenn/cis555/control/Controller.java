package edu.upenn.cis555.control;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.upenn.cis555.command.Command;
import edu.upenn.cis555.command.CommandType;
import edu.upenn.cis555.command.GlobalNumDocsCrawledMessage;
import edu.upenn.cis555.common.ClientSocket;

public class Controller extends Thread {

	private static final Logger log = Logger.getLogger (Controller.class);

	protected ServerSocket listener;

	protected int serverPort;

	protected int numNodes;

	protected long crawlDuration;

	protected List<NodeCommHandler> nodeHandlers = new ArrayList<NodeCommHandler> ();

	protected int nodeJobStatus = 0;

	protected long crawlStats = 0;

	private static Controller singleInstance;

	public Controller (int serverPort, int numNodes, int crawlDuration) {
		this.serverPort = serverPort;
		this.numNodes = numNodes;
		this.crawlDuration = crawlDuration;
	}

	public synchronized void updateJobStatus () {
		nodeJobStatus++;
		System.err.println ("Updating Node Job Status: " + nodeJobStatus);
	}

	public synchronized void updateCrawlStats (int localStats) {
		crawlStats += localStats;
	}

	protected void connect () throws IOException, InterruptedException {
		listener = new ServerSocket (serverPort);
		do {
			Socket clientSocket = listener.accept ();
			NodeCommHandler handler = new NodeCommHandler (new ClientSocket (
					clientSocket));
			nodeHandlers.add (handler);
			handler.start ();

		} while (nodeHandlers.size () < numNodes);

		listener.close ();
		System.err.println ("Successfully launched all node handler threads");

		for (NodeCommHandler handler : nodeHandlers) {
			System.err.println ("Thread State of " + handler.getName () + " : "
					+ handler.getState ());
		}

		while (nodeJobStatus < numNodes) {
			Thread.sleep (10 * 1000);
		}
		nodeJobStatus = 0;
		System.err.println ("All nodes are up");
	}

	protected void crawl () throws InterruptedException {
		Command command = new Command (CommandType.CRAWL_INDEX_START);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}
		System.err.println ("Sent Crawl Start command to all nodes");

		Thread.sleep (crawlDuration * 1000);

		command = new Command (CommandType.CRAWL_INDEX_END);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}
		System.err.println ("Sending Crawl End to all nodes");

		while (nodeJobStatus < numNodes) {
			Thread.sleep (10 * 1000);
		}
		nodeJobStatus = 0;
		System.err.println ("All nodes are up");
	}

	protected void crawlStats () throws InterruptedException {
		Command command = new GlobalNumDocsCrawledMessage (crawlStats);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}
		System.err.println ("Sent Global Crawl Stats to all nodes");
	}

	protected void index () throws InterruptedException {
		Thread.sleep (10 * 60 * 1000);

		Command command = new Command (CommandType.GLOBAL_INDEX_START);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}
		System.err.println ("Sent Global Index Start to all nodes");

		while (nodeJobStatus < numNodes) {
			Thread.sleep (10 * 1000);
		}
		nodeJobStatus = 0;
		System.err.println ("Global Indexing is complete");
	}

	protected void docWeight () throws InterruptedException {
		Thread.sleep (10 * 60 * 1000);

		Command command = new Command (CommandType.DOCUMENT_WEIGHT_CALC_START);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}
		System.err.println ("Sent document weight start to all nodes");

		while (nodeJobStatus < numNodes) {
			Thread.sleep (10 * 1000);
		}
		nodeJobStatus = 0;
		System.err.println ("Document weight start is complete");
	}

	protected void pageRankDistributed () throws InterruptedException {
		nodeJobStatus = 0;
		Command command = new Command (CommandType.DISTRIBUTED_PAGE_RANK_START);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}
		System.err.println ("Page Rank process initiated");

		while (nodeJobStatus < numNodes) {
			Thread.sleep (10 * 1000);
		}
		nodeJobStatus = 0;
		System.err.println ("Page Rank is complete");
	}

	protected void pageRankHadoop () throws IOException {
		// listener = new ServerSocket (serverPort);
		// ClientSocket clientSocket = new ClientSocket (listener.accept ());
		// clientSocket.readLine ();
		// clientSocket = new ClientSocket (listener.accept ());
		// clientSocket.readLine ();
		System.err.println ("Enter any key after hadoop is complete");
		System.in.read ();
	}

	protected void openRestServer () {
		Command command = new Command (CommandType.OPEN_REST_SERVER);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}
		System.err.println ("Rest Server opened for requests");
	}

	public void run () {
		Command command = new Command (CommandType.CLOSE_REST_SERVER);
		for (NodeCommHandler handler : nodeHandlers) {
			handler.enqueue (command);
		}

		boolean allDone = false;
		while (!allDone) {
			allDone = true;
			for (NodeCommHandler handler : nodeHandlers) {
				allDone = allDone
						& (handler.getState () == Thread.State.TERMINATED);
			}
		}
	}

	public void startControl () throws IOException, InterruptedException {
		System.err.println ("Starting control from Controller");

		connect ();
		System.err.println ("Successfully connected to all nodes");

		if (crawlDuration != 0) {
			crawl ();
			System.err.println ("Crawl and local indexing are complete");

			crawlStats ();
			System.err.println ("Crawl stats received");

			index ();
			System.err.println ("Global indexing initiated");

			docWeight ();
			System.err.println ("Document weight calculation initiated");

			pageRankDistributed ();
			System.err.println ("Page Rank Completed");

			pageRankHadoop ();
		}
		openRestServer ();
		System.err.println ("Search engine ready to serve requests");

		boolean allDone = true;
		while (!allDone) {
			for (NodeCommHandler c : nodeHandlers) {
				Thread.State state = c.getState ();
				allDone = allDone & (state == Thread.State.TERMINATED);
			}
		}
	}

	public static Controller getInstance () {
		return singleInstance;
	}

	public static void main (String args[]) throws IOException,
			InterruptedException {
		int serverPort = Integer.parseInt (args[0]);
		int numNodes = Integer.parseInt (args[1]);

		Properties logProperties = new Properties ();
		try {
			logProperties.load (new FileInputStream ("conf/log4j.properties"));
			PropertyConfigurator.configure (logProperties);
			System.err.println ("Logging initialized.");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace ();
		} catch (IOException e1) {
			e1.printStackTrace ();
		}

		int crawlDuration = 0;
		if (args.length > 2) {
			crawlDuration = Integer.parseInt (args[2]);
		}
		singleInstance = new Controller (serverPort, numNodes, crawlDuration);
		Runtime.getRuntime ().addShutdownHook (singleInstance);
		singleInstance.startControl ();
	}
}
