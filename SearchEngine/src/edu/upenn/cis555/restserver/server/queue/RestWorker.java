package edu.upenn.cis555.restserver.server.queue;

import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Logger;

import edu.upenn.cis555.restserver.RestServer;
import edu.upenn.cis555.restserver.rest.RESTSessionHandler;

/**
 * Class representing a REST Worker
 */
public class RestWorker extends Worker {

	private final Logger log = Logger.getLogger (RestWorker.class);

	public RestWorker (EventQueue queue) {
		super (queue);
	}

	public void run () {
		while (true) {
			log.info ("Worker thread started");

			/* Dequeue a request from the Event Queue */
			Socket clientSocket = eventQueue.dequeue ();
			log.info ("Worker thread dequeued a request");

			/*
			 * If the request is null, which means the server is being shutdown.
			 * Close processing of this thread
			 */
			if (clientSocket == null) {
				RestServer.getInstance ().updateShutdownStatus ();
				break;
			}

			/* Create a new HTTP Session Handler to further process the request */
			try {
				log.info ("Handling a new request");
				new RESTSessionHandler (clientSocket);
			} catch (IOException e) {
				log.info ("Closing connection");
			}
		}
	}

	/**
	 * Clone method needed to create other REST worker threads in Server
	 */
	public RestWorker clone () {
		return new RestWorker (eventQueue);
	}
}
