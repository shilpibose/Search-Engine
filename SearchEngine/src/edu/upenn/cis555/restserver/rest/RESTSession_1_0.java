package edu.upenn.cis555.restserver.rest;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.upenn.cis555.common.ClientSocket;
import edu.upenn.cis555.restserver.common.HTTPHeader;
import edu.upenn.cis555.restserver.common.HTTPRequest;
import edu.upenn.cis555.restserver.common.HTTPVersion;
import edu.upenn.cis555.restserver.common.InvalidHTTPRequestException;

/**
 * Class to process and generate response for a HTTP 1.0 request
 */
public class RESTSession_1_0 extends RESTSession {

	private final Logger log = Logger.getLogger (RESTSession_1_0.class);

	/**
	 * Constructor initializing the base class object
	 */
	public RESTSession_1_0 (ClientSocket clientSocket, HTTPRequest httpRequest) {
		super (clientSocket, httpRequest, HTTPVersion.VERSION_1_0);
	}

	/**
	 * Method to validate and generate response for the request
	 * 
	 * @throws ServletException
	 */
	public void handleRequest () throws IOException,
			InvalidHTTPRequestException {

		/* Check whether the server handles the requested method */
		validateHTTPMethod ();
		log.info ("Validated the headers");

		try {
			/* Read the headers and the content send in the request */
			processHeaders ();
		} catch (InvalidHTTPRequestException e) {
			respondToBadRequest (request, e.getMessage ());
		} catch (NumberFormatException e) {
			respondToBadRequest (request, e.getMessage ());
		}

		/* Generate response */
		generateResponse ();
	}

	/**
	 * Method to read the headers and the content
	 */
	protected void processHeaders () throws IOException,
			InvalidHTTPRequestException, NumberFormatException {

		readHeaders ();
		String headerValueStr = request
				.getHeaderValueFor (HTTPHeader.CONTENT_LENGTH);
		if (headerValueStr != null) {
			handleContent (headerValueStr);
		}
	}
}
