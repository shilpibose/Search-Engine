package edu.upenn.cis555.crawler.distributed.constants;

import java.util.ArrayList;

public class ClientConstants {

	private static int HTTP_VERSION=505;
	private static int HTTP_UNSUPPORTED_TYPE=415;
	private static int HTTP_UNAUTHORIZED = 401;
	private static int HTTP_OK = 200;
	private static int HTTP_BAD_METHOD = 405;
	private static int HTTP_ACCEPTED=202;
	private static int HTTP_BAD_REQUEST = 400;
	private final ArrayList<String> XML_MIME_TYPE = new ArrayList<String>();
	
	
	public ClientConstants(){
		XML_MIME_TYPE.add("text/html");
		XML_MIME_TYPE.add("text/xml");
		XML_MIME_TYPE.add("application/xml");
		XML_MIME_TYPE.add("application/xhtml+xml");
		XML_MIME_TYPE.add("application/atom+xml");
		XML_MIME_TYPE.add("application/xslt+xml");
		XML_MIME_TYPE.add("application/mathml+xml");
		XML_MIME_TYPE.add("application/rss+xml");
	}


	public ArrayList<String> getXML_MIME_TYPE() {
		return XML_MIME_TYPE;
	}
	
}
