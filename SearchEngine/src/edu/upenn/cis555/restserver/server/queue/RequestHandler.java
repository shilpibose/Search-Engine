package edu.upenn.cis555.restserver.server.queue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * Class handling the task of listening to the Server socket
 */
public class RequestHandler extends Thread {

	private final Logger log = Logger.getLogger(RequestHandler.class);

	protected EventQueue eventQueue;

	/** Server listener socket */
	protected ServerSocket listener;

	/** Server port */
	protected int serverPort;

	/** Number of worker threads */
	protected int workerThreadCount;

	protected boolean serverBeingStopped;

	public RequestHandler(int portNumber, int workerThreadCount,
			EventQueue queue) throws IOException {
		serverPort = portNumber;
		this.workerThreadCount = workerThreadCount;
		listener = new ServerSocket(serverPort);
		eventQueue = queue;
	}

	public void run() {
		try {
			while (true) {
				Socket clientSocket = listener.accept();
				log.info("Received a connection to the server");
				/* Add the new connection to the Event Queue */
				eventQueue.enqueue(clientSocket);
			}
		} catch (IOException ioe) {
			log.info("Listener socket exception.");
		}
	}

	public void startServer() {
		start();
	}

	/**
	 * Initiates shutdown of the listener and worker threads
	 */
	public void stopServer() {
		/*
		 * Add a null message for every Worker thread to inform that Server is
		 * being shutdown
		 */
		for (int iter = 0; iter < workerThreadCount; iter++) {
			eventQueue.enqueue(null);
		}
		try {
			log.info("Closing server socket");
			listener.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}