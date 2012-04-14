package edu.upenn.cis555.restserver.rest;

import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Logger;

import edu.upenn.cis555.common.ClientSocket;
import edu.upenn.cis555.restserver.RestServer;
import edu.upenn.cis555.restserver.common.HTTPRequest;
import edu.upenn.cis555.restserver.common.HTTPVersion;
import edu.upenn.cis555.restserver.common.InvalidHTTPRequestException;

/**
 * Class handling REST Requests on a given socket
 */
public class RESTSessionHandler {

	private final Logger log = Logger.getLogger (RESTSessionHandler.class);

	/** Socket on which REST Requests will be received */
	protected ClientSocket socket;

	public RESTSessionHandler (Socket clientSocket) throws IOException {
		socket = new ClientSocket (clientSocket);
		HTTPRequest request = null;
		boolean isPersistent = false;

		do {
			isPersistent = false;

			/* Read the first line of the request */
			String reqLine = socket.readLine ();
			try {
				log.info ("New request : " + reqLine);
				if (reqLine.length () == 0) {
					/*
					 * The first line itself is bad No knowledge of HTTP version
					 * being used Let the request timeout. Close the connection
					 * since we dont know how much of the data is remaining in
					 * the connection.
					 */
					log.error ("Received a blank line as the first line");
					throw new InvalidHTTPRequestException ("");
				}
				request = new HTTPRequest (reqLine);

				/*
				 * Create a new HTTP session for the received HTTP request to
				 * processing further depending on the version in the HTTP
				 * Request
				 */
				if (request.getVersion ().equals (HTTPVersion.VERSION_1_0)) {
					log.debug ("Request is of version 1.0");
					RESTSession_1_0 rest_1_0_Session = new RESTSession_1_0 (
							socket, request);
					rest_1_0_Session.handleRequest ();

				} else {
					log.debug ("Request is of version 1.1");
					RESTSession_1_1 rest_1_1_Session = new RESTSession_1_1 (
							socket, request);
					rest_1_1_Session.handleRequest ();

					/*
					 * If the HTTP connection is persistent, then continue
					 * awaiting request on this socket itself
					 */
					isPersistent = rest_1_1_Session.isPersistent ();
				}
			} catch (InvalidHTTPRequestException e) {
				log.debug ("Closing socket because of invalid HTTP Request");
				socket.close ();
			}
			/* If the server is being shutdown, do not accept any more requests */
		} while (isPersistent
				&& ! (RestServer.getInstance ().isServerBeingShutDown ()));

		log.debug ("Closing socket");
		/* Close the socket */
		socket.close ();
	}
}
