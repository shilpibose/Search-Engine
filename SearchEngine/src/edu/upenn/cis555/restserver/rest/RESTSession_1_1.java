package edu.upenn.cis555.restserver.rest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import edu.upenn.cis555.common.ClientSocket;
import edu.upenn.cis555.restserver.common.HTTPHeader;
import edu.upenn.cis555.restserver.common.HTTPRequest;
import edu.upenn.cis555.restserver.common.HTTPVersion;
import edu.upenn.cis555.restserver.common.InvalidHTTPRequestException;
import edu.upenn.cis555.restserver.server.utils.RegExp;

/**
 * Class to process and generate response for a HTTP 1.1 request
 */
public class RESTSession_1_1 extends RESTSession {

	private final Logger log = Logger.getLogger (RESTSession_1_1.class);

	/**
	 * Constructor initializing the base class object
	 */
	public RESTSession_1_1 (ClientSocket clientSocket, HTTPRequest httpRequest) {
		super (clientSocket, httpRequest, HTTPVersion.VERSION_1_1);
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
		log.debug ("Validated the HTTP Method");

		try {
			/* Read the headers and the content send in the request */
			processHeaders ();
			log.debug ("Processed headers");

		} catch (InvalidHTTPRequestException e) {
			respondToBadRequest (request, e.getMessage ());
		} catch (NumberFormatException e) {
			respondToBadRequest (request, e.getMessage ());
		}

		try {
			/* Check for the mandatory headers in the request */
			validateHeaders ();
			log.debug ("Validated the headers");

			/* Generate response */
			generateResponse ();
			log.debug ("Sent the response");

		} catch (InvalidHTTPRequestException e) {
			respondToBadRequest (request, e.getMessage ());
		}
	}

	/**
	 * Method to read the headers and the content
	 */
	protected void processHeaders () throws IOException,
			InvalidHTTPRequestException, NumberFormatException {

		readHeaders ();
		log.info ("Completed reading the headers of the request");

		/* Either we get Content-Length or Chunked-Transfer-Encoding */

		String headerValueStr = request
				.getHeaderValueFor (HTTPHeader.CONTENT_LENGTH);
		if (headerValueStr != null) {
			log.debug ("Handling content in the message");
			handleContent (headerValueStr);
		} else {
			handleChunkedEncoding ();
		}
	}

	/**
	 * Read the chunks in chunked encoding and put them into stream
	 */
	protected void handleChunkedEncoding () throws InvalidHTTPRequestException,
			IOException {
		/* Handle chunked transfer-encoding */
		String reqLine;
		String transferEncoding = request
				.getHeaderValueFor (HTTPHeader.TRANSFER_ENCODING);

		if (transferEncoding != null && transferEncoding.equals ("chunked")) {
			byte[] contentBuffer = new byte[0];
			int contentBufferLength = 0;

			while (true) {
				reqLine = socket.readLine ();
				if (reqLine.length () == 0) {
					throw new InvalidHTTPRequestException (
							"Chunks are missing in the requests");
				}

				/* Read the chunk length and parse it with radix 16 */
				int chunkLength = Integer.parseInt (reqLine
						.split (RegExp.SEMI_COLON)[0].trim (), 16);
				if (chunkLength == 0) {
					break;
				}
				/* Expand the buffer after reading chunk length */
				contentBufferLength = contentBuffer.length;
				contentBuffer = expandContentBuffer (contentBuffer, chunkLength);

				/* Read the data of length got from previous step */
				for (int iter = 0; iter < chunkLength; iter++) {
					contentBuffer[contentBufferLength + iter] = new Integer (
							socket.readChar ()).byteValue ();
				}
				log.info ("Read " + chunkLength + " long data in this chunk. "
						+ (contentBufferLength + chunkLength) + " so far");

				socket.readLine ();
			}

			request.setHeader (HTTPHeader.CONTENT_LENGTH + ":"
					+ contentBuffer.length);
			request.setContentBufferedReader (new BufferedReader (
					new InputStreamReader (new ByteArrayInputStream (
							contentBuffer))));

			/* Read the rest of the footers if present */
			readHeaders ();
		}
	}

	/**
	 * Expands the buffer required when handling chunked encoding
	 */
	protected byte[] expandContentBuffer (byte[] contentBuffer, int length) {
		byte[] newByteBuffer = new byte[contentBuffer.length + length];
		for (int iter = 0; iter < contentBuffer.length; iter++) {
			newByteBuffer[iter] = contentBuffer[iter];
		}
		return newByteBuffer;
	}

	/**
	 * Method returning whether the connection through which the request was
	 * received is going to be persistent or not.
	 */
	public boolean isPersistent () {
		String headerValueStr = request.getHeaderValueFor ("connection");

		/* Default setting in HTTP 1.1 is persistent */
		if ( (null != headerValueStr)
				&& (headerValueStr.equalsIgnoreCase ("close"))) {
			return false;
		}
		log.debug ("Connection is persistent");
		return true;
	}

	/**
	 * Method to check whether the mandatory host header is present in request
	 */
	protected void validateHeaders () throws InvalidHTTPRequestException {
		if (null == request.getHeaderValueFor ("host")) {
			log
					.error ("Did not find the host header which is mandatory in 1.1");
			throw new InvalidHTTPRequestException ("No Host: header received. "
					+ "HTTP 1.1 requests must include the Host: header.");
		}
	}
}
