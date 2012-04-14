package edu.upenn.cis555.restserver;

import edu.upenn.cis555.restserver.server.queue.EventQueue;
import edu.upenn.cis555.restserver.server.queue.RestWorker;

/**
 * Singleton class representing a HTTP REST Server
 */
public class RestServer extends BaseServer {

	private static RestServer singleInstance;

	/**
	 * Initialize the base class with REST Worker object
	 * 
	 * @param serverPort
	 */
	private RestServer (int serverPort) {
		super (serverPort, new RestWorker (new EventQueue ()));
	}

	public void startServer () {
		super.startServer ();
	}

	public void shutDownServer () {
		super.shutDownServer ();
	}

	public static RestServer getInstance () {
		return singleInstance;
	}

	public static void instantiate (int serverPort) {
		singleInstance = new RestServer (serverPort);
	}
}
