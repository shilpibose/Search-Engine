package edu.upenn.cis555.restserver.common;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import edu.upenn.cis555.common.ClientSocket;

/**
 * Abstract class containing the common elements of the Web and REST Server
 */
public abstract class HTTPBaseSession {
	private final Logger log = Logger.getLogger (HTTPBaseSession.class);

	protected ClientSocket socket;

	/* The request which this HTTP Session is handling */
	protected HTTPRequest request;

	/* HTTP version of the request */
	protected String version;

	public HTTPBaseSession (ClientSocket clientSocket, HTTPRequest httpRequest,
			HTTPVersion requestVersion) {
		socket = clientSocket;
		request = httpRequest;

		/* Initialize the version string to construct appropriate responses */
		if (requestVersion == HTTPVersion.VERSION_1_0) {
			version = new String ("1.0");
		} else if (requestVersion == HTTPVersion.VERSION_1_1) {
			version = new String ("1.1");
		}
	}

	/**
	 * Check whether the requested method is implemented, otherwise return 501
	 */
	protected abstract void validateHTTPMethod ()
			throws InvalidHTTPRequestException;

	/**
	 * Method to read the HTTP headers in the request
	 */
	protected void readHeaders () throws IOException,
			InvalidHTTPRequestException {
		/* Read the headers in the HTTP Request */
		StringBuffer buffer = new StringBuffer ();
		String reqLine = socket.readLine ();
		while (reqLine.length () != 0) {
			log.info (reqLine);
			buffer.append (reqLine);
			if ( (reqLine.charAt (0) != ' ') && (reqLine.charAt (0) != '\t')) {
				request.addHeader (buffer.toString ());
				buffer.delete (0, reqLine.length ());
			}
			reqLine = socket.readLine ();
		}
	}

	public abstract void handleRequest () throws IOException,
			InvalidHTTPRequestException;

	protected void handleContent (String contentLengthStr) throws IOException {
		/* Read all the content sent */
		int contentLength = Integer.parseInt (contentLengthStr);
		byte[] contentBuffer = new byte[contentLength];
		for (int iter = 0; iter < contentLength; iter++) {
			contentBuffer[iter] = new Integer (socket.readChar ()).byteValue ();
		}
		request
				.setContentBufferedReader (new BufferedReader (
						new InputStreamReader (new ByteArrayInputStream (
								contentBuffer))));
	}

	/**
	 * Method to generate and send the response to the client
	 */
	protected abstract void generateResponse () throws IOException,
			InvalidHTTPRequestException;
}
