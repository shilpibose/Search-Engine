package edu.upenn.cis555.restserver.common;

import org.apache.log4j.Logger;

/**
 * HTTP Header class with header name and value
 */
public class HTTPHeader {

	public static final String CONTENT_LENGTH = "content-length";

	public static final String CONTENT_TYPE = "content-type";

	public static final String COOKIE = "cookie";

	public static final String TRANSFER_ENCODING = "transfer-encoding";
	
	public static final String IF_MODIFIED_SINCE = "if-modified-since";
	
	public static final String IF_UNMODIFIED_SINCE = "if-unmodified-since";

	public static final String SET_COOKIE = "Set-Cookie";

	private final Logger log = Logger.getLogger (HTTPHeader.class);

	protected String header;
	protected String value;

	public HTTPHeader (String header, String value) {
		this.header = new String (header);
		this.value = new String (value);
	}

	public String getValue () {
		return value;
	}
}
