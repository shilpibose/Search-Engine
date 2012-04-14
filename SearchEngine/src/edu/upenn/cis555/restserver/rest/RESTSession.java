package edu.upenn.cis555.restserver.rest;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.log4j.Logger;

import edu.upenn.cis555.common.ClientSocket;
import edu.upenn.cis555.restserver.common.HTTPBaseSession;
import edu.upenn.cis555.restserver.common.HTTPRequest;
import edu.upenn.cis555.restserver.common.HTTPRequestType;
import edu.upenn.cis555.restserver.common.HTTPVersion;
import edu.upenn.cis555.restserver.common.InvalidHTTPRequestException;
import edu.upenn.cis555.restserver.server.utils.ServerUtils;
import edu.upenn.cis555.restserver.service.ServiceManager;

/**
 * Abstract class containing the common elements of the HTTP 1.0 and 1.1 Server
 */
public abstract class RESTSession extends HTTPBaseSession {

	private final Logger log = Logger.getLogger (RESTSession.class);

	public RESTSession (ClientSocket clientSocket, HTTPRequest httpRequest,
			HTTPVersion requestVersion) {
		super (clientSocket, httpRequest, requestVersion);
	}

	/**
	 * Check whether the requested method is implemented, otherwise return 501
	 */
	protected void validateHTTPMethod () throws InvalidHTTPRequestException {
		if (request.getRequestType () != HTTPRequestType.GET) {
			send501Response ();
			throw new InvalidHTTPRequestException ("");
		}
	}

	/**
	 * Method to respond for a BAD request with custom message added in the body
	 */
	protected void respondToBadRequest (HTTPRequest request, String message)
			throws InvalidHTTPRequestException {
		socket.writeLine ("HTTP/" + version + " 400 BAD REQUEST");
		socket.writeLine ("Date: " + ServerUtils.timeNow ());
		socket.writeLine ("Content-Type: text/html");

		String errorMessage = new String ("<html>" + "<body>" + "<h1>"
				+ "400 BAD REQUEST" + "</h1>" + "<hr/>" + message + "</hr>"
				+ "</body>" + "</html>");
		socket.writeLine ("Content-Length: " + errorMessage.length ());
		socket.writeLine ("");
		socket.writeLine (errorMessage);
		throw new InvalidHTTPRequestException ("");
	}

	/**
	 * Method to respond when Servlet Fails
	 */
	protected void respondToFailure (HTTPRequest request, String message)
			throws InvalidHTTPRequestException {
		socket.writeLine ("HTTP/" + version + " 500 SERVER ERROR");
		socket.writeLine ("Date: " + ServerUtils.timeNow ());
		socket.writeLine ("Content-Type: text/html");

		String errorMessage = new String ("<html>" + "<body>" + "<h1>"
				+ "500 SERVER ERROR" + "</h1>" + "<hr/>" + message + "</hr>"
				+ "</body>" + "</html>");
		socket.writeLine ("Content-Length: " + errorMessage.length ());
		socket.writeLine ("");
		socket.writeLine (errorMessage);
		throw new InvalidHTTPRequestException ("");
	}

	protected void send501Response () {
		socket.writeLine ("HTTP/" + version + " 501 NOT IMPLEMENTED");
		socket.writeLine ("Date: " + ServerUtils.timeNow ());
		socket.writeLine ("Content-Type: text/html");

		String errorMessage = new String (
				"<html><body><h1>501 NOT IMPLEMENTED</h1></body></html>");
		socket.writeLine ("Content-Length: " + errorMessage.length ());
		socket.writeLine ("");
		socket.writeLine (errorMessage);
		socket.writeLine ("");
	}

	public abstract void handleRequest () throws IOException,
			InvalidHTTPRequestException;

	protected abstract void processHeaders () throws IOException,
			InvalidHTTPRequestException, NumberFormatException;

	/**
	 * Method to generate and send the response to the client
	 * 
	 * @throws InvalidHTTPRequestException
	 */
	protected void generateResponse () throws IOException,
			InvalidHTTPRequestException {
		String requestArgument = new String (request.getRequestArgument ());
		requestArgument = URLDecoder.decode (requestArgument, "UTF-8");

		String[] components = requestArgument.split ("[?]");
		if (components.length <= 1) {
			respondToBadRequest (request, "Unknown REST request");
		}

		String afterQuestionMark = requestArgument.split ("[?]")[1];
		String[] parameterPairs = afterQuestionMark.split ("&");

		String batchNumberStr = null;
		String searchString = null;
		String method = null;
		for (int i = 0; i < parameterPairs.length; i++) {
			String[] valuePair = parameterPairs[i].split ("=");
			if (valuePair[0].equals ("b")) {
				batchNumberStr = valuePair[1];
			} else if (valuePair[0].equals ("m")) {
				method = valuePair[1];
			} else if (valuePair[0].equals ("q")) {
				searchString = valuePair[1];
			}
		}

		try {
			String result = ServiceManager.getInstance ().getSearchResults (
					searchString, Integer.parseInt (batchNumberStr), method);
			System.err.println (result.length () + ": " + result);
			socket.writeLine ("HTTP/1.1 200 OK");
			socket.writeLine ("Date: " + ServerUtils.timeNow ());
			socket.writeLine ("Server: CIS555/0.1");
			socket.writeLine ("Content-Type: application/xml");
			socket.writeLine ("Content-Length: " + result.length ());
			socket.writeLine ("");
			socket.writeLine (result);

		} catch (InterruptedException e) {
			socket.writeLine ("HTTP/1.1 500 Server Error");
			socket.writeLine ("Date: " + ServerUtils.timeNow ());
			socket.writeLine ("Server: CIS555/0.1");
			socket.writeLine ("Content-Type: text/html");

			String errorMessage = new String ("<html>" + "<body>" + "<h1>"
					+ "500 Server Error" + "</h1>" + "</body>" + "</html>");
			socket.writeLine ("Content-Length: " + errorMessage.length ());
			socket.writeLine ("");
			socket.writeLine (errorMessage);
		}
	}
}
