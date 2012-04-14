package edu.upenn.cis555.restserver.common;

import org.apache.log4j.Logger;

public class InvalidHTTPRequestException extends Exception {
	
	private final Logger log = Logger.getLogger (InvalidHTTPRequestException.class);
	
	private static final long serialVersionUID = 1L;

	public InvalidHTTPRequestException (String msg) {
		super (msg);
	}
}
