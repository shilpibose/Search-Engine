package edu.upenn.cis555.restserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import edu.upenn.cis555.restserver.server.queue.RequestHandler;
import edu.upenn.cis555.restserver.server.queue.Worker;

/**
 * Abtract class representing a Server
 */
public abstract class BaseServer extends Thread {

	private final Logger log = Logger.getLogger (BaseServer.class);

	/** Allowed inactivity period on the socket */
	protected static long allowedInactivityTime = 30 * 1000;

	/** Number of worker threads handling the HTTP requests */
	protected static int maxThreads = 10;

	/** Handler listening on the Server socket */
	protected RequestHandler requestHandler;

	/** Member representing whether the server is running or not */
	protected boolean serverRunning;

	/** Worker thread status */
	protected Thread[] workerThreads = new Thread[maxThreads];

	/** Port at which the Server listens */
	protected int serverPort;

	/**
	 * Member representing the status after the server has been shutdown, with
	 * value = maxThreads denoting that server is ready to be shutdown
	 */
	protected int shutdownStatus;

	private Worker worker;

	/**
	 * Constructor taking a Worker object to initialize other Worker Threads
	 */
	public BaseServer (int serverPort, Worker worker) {
		this.serverPort = serverPort;
		serverRunning = false;
		shutdownStatus = 0;
		this.worker = worker;
	}

	/**
	 * Method returning the allowed inactivity time on a client socket
	 */
	public long getAllowedInactivityTime () {
		return allowedInactivityTime;
	}

	/**
	 * Method providing status whether the server is shutting down. This is used
	 * to decide whether to accept new HTTP requests
	 */
	public boolean isServerBeingShutDown () {
		return (shutdownStatus > 0);
	}

	/**
	 * Method which is called by Worker threads to update their shutdown status
	 */
	public synchronized void updateShutdownStatus () {
		shutdownStatus++;
	}

	protected static void printOut (String message) {
		System.err.println (message);
	}

	public void run () {
		shutDownServer ();
	}

	/**
	 * Method to start the server by initializing the Server socket handler and
	 * starting the given number of Worker threads
	 */
	protected void startServer () {
		if (!serverRunning) {
			printOut ("Server is being started at port " + serverPort + "\n");
			try {
				requestHandler = new RequestHandler (serverPort, maxThreads,
						worker.getEventQueue ());
				requestHandler.startServer ();
				log.info ("Starting server at port " + serverPort);
				serverRunning = true;

				/* Create worker threads */
				for (int iter = 0; iter < maxThreads; iter++) {
					workerThreads[iter] = worker.clone ();
					workerThreads[iter].start ();
				}
				log.info ("Created " + maxThreads + " number of threads");

			} catch (IOException e) {
				e.printStackTrace ();
			}
		} else {
			printOut ("Server is already running");
		}
	}

	/**
	 * Method to shutdown the server.
	 */
	protected void shutDownServer () {
		if (serverRunning) {
			printOut ("Server is being shutdown");
			/*
			 * Inform the Server socket listener thread to stop listening, and
			 * inform the Worker threads to stop accepting more requests
			 */
			requestHandler.stopServer ();

			/*
			 * Wait until all the Worker threads have reported of completing
			 * their ongoing requests
			 */
			while (shutdownStatus < maxThreads) {
				log.info ("Waiting for the threads to stop: " + shutdownStatus
						+ " =/= " + maxThreads);
				try {
					Thread.sleep (1 * 1000);
				} catch (InterruptedException e) {
					printOut ("Error while waiting for threads to stop. Exiting "
							+ e.getMessage ());
					e.printStackTrace ();
					System.exit (1);
				}
			}
			printOut ("Server has been shut down successfully");
			shutdownStatus = 0;
			serverRunning = false;
		} else {
			printOut ("Server is not running");
		}
	}

	protected void showThreadStatus () {
		for (int iter = 0; iter < maxThreads; iter++) {
			printOut ("Worker Thread " + iter + " Status : "
					+ (workerThreads[iter].getState ()));
		}
	}

	protected void showLog () {
		try {
			Process p = Runtime.getRuntime ().exec (
					"tail -100 cis555_server.log");

			BufferedReader stdInput = new BufferedReader (
					new InputStreamReader (p.getInputStream ()));

			String line;
			while ( (line = stdInput.readLine ()) != null) {
				printOut (line);
			}

		} catch (IOException e) {
			printOut ("Error while accessing log file: " + e.getMessage ());
		}
	}
}
