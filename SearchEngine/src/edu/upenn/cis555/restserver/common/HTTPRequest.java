package edu.upenn.cis555.restserver.common;

import java.io.BufferedReader;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.upenn.cis555.restserver.server.utils.RegExp;

/**
 * Class representing the HTTP Request
 */
public class HTTPRequest {

	private final Logger log = Logger.getLogger (HTTPRequest.class);

	/* HTTP Request Type - GET, POST ... */
	protected HTTPRequestType requestType;

	/* Request file with GET or PORT in the request */
	protected String requestArgument;

	/* Map of headers from header to HTTPheader received in the HTTP Request */
	protected Hashtable<String, HTTPHeader> requestHeaders;

	/* HTTP version of the request */
	protected HTTPVersion version;

	/* Input Stream */
	protected BufferedReader contentReader;

	/**
	 * Constructor method initializing the request type, requested file
	 */
	public HTTPRequest (String firstLine) throws InvalidHTTPRequestException {
		requestHeaders = new Hashtable<String, HTTPHeader> ();

		String[] requestElements = firstLine.trim ().split (RegExp.SPACE);
		if (requestElements.length < 3) {
			throw new InvalidHTTPRequestException ("");
		}

		/* Process to remove white spaces with header and value */
		int iter = 0;
		for (String s : requestElements) {
			requestElements[iter++] = s.trim ();
		}

		log.info ("Request elements " + requestElements[0] + " "
				+ requestElements[1] + " " + requestElements[2]);
		if (requestElements[0].equalsIgnoreCase ("GET")) {
			requestType = HTTPRequestType.GET;
		} else if (requestElements[0].equalsIgnoreCase ("POST")) {
			requestType = HTTPRequestType.POST;
		} else if (requestElements[0].equalsIgnoreCase ("HEAD")) {
			requestType = HTTPRequestType.HEAD;
		} else if (requestElements[0].equalsIgnoreCase ("PUT")) {
			requestType = HTTPRequestType.PUT;
		} else if (requestElements[0].equalsIgnoreCase ("DELETE")) {
			requestType = HTTPRequestType.DELETE;
		} else if (requestElements[0].equalsIgnoreCase ("OPTIONS")) {
			requestType = HTTPRequestType.OPTIONS;
		} else if (requestElements[0].equalsIgnoreCase ("TRACE")) {
			requestType = HTTPRequestType.TRACE;
		} else {
			throw new InvalidHTTPRequestException ("");
		}

		if (requestElements[2].equalsIgnoreCase ("HTTP/1.0")) {
			version = HTTPVersion.VERSION_1_0;
		} else if (requestElements[2].equalsIgnoreCase ("HTTP/1.1")) {
			version = HTTPVersion.VERSION_1_1;
		} else {
			throw new InvalidHTTPRequestException ("");
		}

		/* Check whether the requested file contains the URL itself */
		if (requestElements[1].startsWith ("http://")) {
			requestElements[1] = requestElements[1]
					.substring (requestElements[1].indexOf ("/", 7));
		}
		requestArgument = requestElements[1];
	}

	public HTTPRequest (HTTPRequest anotherRequest) {
		requestType = anotherRequest.requestType;
		requestArgument = anotherRequest.requestArgument;
		requestHeaders = anotherRequest.requestHeaders;
		version = anotherRequest.version;
		contentReader = anotherRequest.contentReader;
	}

	/**
	 * Method to add header while processing through the HTTP request
	 */
	public void addHeader (String headerString)
			throws InvalidHTTPRequestException {
		String[] headerElements = headerString.split (RegExp.COLON, 2);
		if (headerElements.length < 2) {
			throw new InvalidHTTPRequestException ("");
		}

		/* Process to remove white spaces with header and value */
		int iter = 0;
		for (String s : headerElements) {
			headerElements[iter++] = s.trim ();
		}

		/* Add the header to the hash table */
		log.info ("Adding header " + headerElements[0].toLowerCase ()
				+ " with value " + headerElements[1]);

		HTTPHeader header = requestHeaders.get (headerElements[0]
				.toLowerCase ());
		if (header == null) {
			requestHeaders.put (headerElements[0].toLowerCase (),
					new HTTPHeader (headerElements[0], headerElements[1]));
		} else {
			String newValue = header.getValue () + ", " + headerElements[1];
			requestHeaders.put (headerElements[0].toLowerCase (),
					new HTTPHeader (headerElements[0], newValue));
		}
	}

	/**
	 * Method to set header while processing through the HTTP request
	 */
	public void setHeader (String headerString)
			throws InvalidHTTPRequestException {
		String[] headerElements = headerString.split (RegExp.COLON, 2);
		if (headerElements.length < 2) {
			throw new InvalidHTTPRequestException ("");
		}

		/* Process to remove white spaces with header and value */
		int iter = 0;
		for (String s : headerElements) {
			headerElements[iter++] = s.trim ();
		}

		/* Add the header to the hash table */
		log.info ("Setting header " + headerElements[0].toLowerCase ()
				+ " with value " + headerElements[1]);

		requestHeaders.put (headerElements[0].toLowerCase (), new HTTPHeader (
				headerElements[0], headerElements[1]));
	}

	public void setContentBufferedReader (BufferedReader contentReader) {
		this.contentReader = contentReader;
	}

	/**
	 * Method to retrieve value for a given header string
	 */
	public String getHeaderValueFor (String header) {
		HTTPHeader result = requestHeaders.get (header);
		if (result == null)
			return null;
		else
			return result.getValue ();
	}

	public Map<String, HTTPHeader> getAllHeaders () {
		return requestHeaders;
	}

	public HTTPRequestType getRequestType () {
		return requestType;
	}

	public String getRequestArgument () {
		return requestArgument;
	}

	public HTTPVersion getVersion () {
		return version;
	}
}
