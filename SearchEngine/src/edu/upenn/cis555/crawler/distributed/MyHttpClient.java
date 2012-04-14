package edu.upenn.cis555.crawler.distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.tidy.Tidy;

import edu.upenn.cis555.crawler.bean.Robots;

public class MyHttpClient {
	private final Logger log = Logger.getLogger (Master.class);
	String urlString;
	URL url;
	URL modifiedURL;
	private static Hashtable<String, String> dnsCache = new Hashtable<String,String>();
	HttpURLConnection conn;
	String contentType;
	Long lastModifiedTime;

	
	public int getResponseCode() {
		return responseCode;
	}

	public URL getModifiedURL() {
		return modifiedURL;
	}

	public void setModifiedURL(URL modifiedURL) {
		this.modifiedURL = modifiedURL;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	String content;
	String language;
	int responseCode;
	public String getContent () {
		return content;
	}
	public URL getURL(){
		return url;
	}
	public void setURL(URL url){
		this.url = url;
	}
	public void setContent (String content) {
		this.content = content;
	}

	public Document getDocWebPage () {
		return docWebPage;
	}

	public void setDocWebPage (Document docWebPage) {
		this.docWebPage = docWebPage;
	}

	int contentLength;
	Document docWebPage;

	public MyHttpClient (URL url2) {
		url = url2;
		
	}

	public Long getLastModifiedTime () {
		return lastModifiedTime;
	}
	
	public void setLastModifiedTime (Long lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

	public String getContentType () {
		return contentType;
	}

	public void setContentType (String tempContentType) {
		if (tempContentType.indexOf (";") == -1) {
			this.contentType = tempContentType;
		} else {
			this.contentType = tempContentType.substring (0, tempContentType
					.indexOf (";"));
		}
	}

	public int getContentLength () {
		return contentLength;
	}

	public void setContentLength (int contentLength) {
		this.contentLength = contentLength;
	}
	public void checkCache(){
		if(dnsCache.containsKey(url.getHost())){
			try {
				this.modifiedURL=(new URL(url.toString().replaceAll(url.getHost(), this.dnsCache.get(url.getHost()))));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}else{
			try {
				dnsCache.put(url.getHost(), InetAddress.getByName(url.getHost()).getHostAddress());
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				this.modifiedURL =(new URL(url.toString().replaceAll(url.getHost(), this.dnsCache.get(url.getHost()))));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void sendHeadRequest () throws Exception {
		checkCache();
		conn = (HttpURLConnection) modifiedURL.openConnection ();
		conn.setDoInput (true);
		conn.setRequestMethod ("HEAD");
		conn.setInstanceFollowRedirects(true);
		//conn.setRequestProperty("Accept-Language", "en");
		conn.connect ();
		
		this.setContentType (conn.getContentType ().split(";")[0]);
		
		this.setContentLength (conn.getContentLength ());
		this.setLastModifiedTime (conn.getLastModified ());
		this.setLanguage(conn.getHeaderField("Content-Language"));
		this.setResponseCode(conn.getResponseCode());
		conn.disconnect ();
		
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public void sendGetMessage () {
		try {
			String content = "";
			String input = null;
			Tidy tidy = new Tidy ();
			conn = (HttpURLConnection) modifiedURL.openConnection ();
			conn.setRequestMethod ("GET");
			conn.setDoInput (true);
			conn.setDoOutput (true);
		
			BufferedReader in = new BufferedReader (new InputStreamReader (conn
					.getInputStream ()));
			StringWriter out = new StringWriter ();
			/*
			 * conn.connect(); this.setContentType(conn.getContentType());
			 * this.setContentLength(conn.getContentLength());
			 * this.setDocWebPage(tidy.parseDOM(in,out));
			 * 
			 * 
			 * conn.disconnect(); conn = (HttpURLConnection)
			 * url.openConnection(); conn.setRequestMethod("GET");
			 * conn.setDoInput(true); conn.setDoOutput(true);
			 */
			conn.connect ();
			BufferedReader read = new BufferedReader (new InputStreamReader (
					conn.getInputStream ()));
			while ( (input = read.readLine ()) != null) {
				content = content.concat (input);
			}
			this.setContent (content);
			this.setLanguage(conn.getHeaderField("Content-Language"));
			conn.disconnect ();

		} catch (ProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
	}

	public Robots retrieveDisallowedLinks () {
		String input = null;
		Robots result = null;
		boolean robotsDiscard = false;
		ArrayList<String> links = new ArrayList<String> ();
		try {
			//	
			URL hostUrl;
			String urlhttp;
			if (url.getHost ().startsWith ("http")) {
				if (url.getHost ().endsWith ("/")) {
					urlhttp = url.getHost ();
					hostUrl = new URL (url.getHost ().concat ("robots.txt"));
				} else {
					urlhttp = url.getHost ();
					hostUrl = new URL (url.getHost ().concat ("/robots.txt"));
				}
			} else {
				if (url.getHost ().endsWith ("/")) {
					urlhttp = "http://".concat (url.getHost ());
					hostUrl = new URL ("http://".concat (url.getHost ())
							.concat ("robots.txt"));
				} else {
					urlhttp = "http://".concat (url.getHost ());
					hostUrl = new URL ("http://".concat (url.getHost ())
							.concat ("/robots.txt"));
				}
			}

			conn = (HttpURLConnection) hostUrl.openConnection ();

			conn.setRequestMethod ("GET");

			conn.setDoInput (true);
			conn.setDoOutput (true);
			conn.connect ();
			
			if(conn.getResponseCode()!= 200){
				conn.disconnect();
				return null;
				
			}else{
				
				
			BufferedReader read = new BufferedReader (new InputStreamReader (
					conn.getInputStream ()));
		/*	while ( (input = read.readLine ()) != null) {
				if (input.startsWith ("User-agent:")) {
					robotsDiscard = false;
					if (input.substring (input.indexOf (' ')).indexOf (';') == -1) {
						if (input.substring (input.indexOf (' ') + 1)
								.startsWith ("*")) {
							robotsDiscard = true;
						} else {
							robotsDiscard = false;
						}
					} else {
						robotsDiscard = false;
						String[] line = input.substring (
								input.indexOf (' ') + 1).split (";");
						for (String next : line) {
							if (next.equals ("*")) {
								robotsDiscard = true;
							}
						}
					}

				}
				if (input.startsWith ("Disallow:")) {
					String disallowedLink;
					if (robotsDiscard && ! (input.indexOf ("/") == -1)) {
						String restInput = input
								.substring (input.indexOf ("/"));
						if (urlhttp.endsWith ("/")) {

							disallowedLink = urlhttp.concat (restInput
									.substring (restInput.indexOf ('/') + 1));
						} else {
							disallowedLink = urlhttp.concat (restInput);
						}
						links.add (disallowedLink);
					}
				}
			}*/
			result = new Robots();
			String line = null;
	        String serverUserAgent = InetAddress.getLocalHost ().getHostName ();

	        boolean userAgentMatched = false;
	        ArrayList<String> disallowedDirsForUA = new ArrayList<String> ();

	        boolean starMatched = false;
	        ArrayList<String> disallowedDirsForAll = new ArrayList<String> ();

	        log.debug ("Reading robot.txt file");

	        while ( (line = read.readLine ()) != null) {
	            log.debug ("robot.txt: " + line);
	            line = line.trim ();
	            if ( (line.length () == 0) || (line.startsWith ("#"))) {
	                continue;
	            }

	            line = line.toLowerCase ();
	            String components[] = line.split (":");

	            if (components[0].trim ().equalsIgnoreCase ("User-agent")) {
	                log.debug ("Rules for User-agent: " + components[1]);

	                if (components[1].trim ().equalsIgnoreCase (serverUserAgent)) {
	                    log.debug ("Found record for this crawler");
	                    userAgentMatched = true;
	                    starMatched = false;
	                } else if (components[1].trim ().equalsIgnoreCase ("*")) {
	                    log.debug ("Found record for *");
	                    starMatched = true;
	                } 

	            } else if (components[0].trim ().equalsIgnoreCase ("Disallow")) {
	                if (components.length > 1) {
	                    if (starMatched) {
	                        disallowedDirsForAll.add (components[1].trim ());
	                        log.debug (components[1]
	                                + " disallowed for crawling for *");
	                    } else if (userAgentMatched) {
	                        disallowedDirsForUA.add (components[1].trim ());
	                        log.debug (components[1] + " disallowed for crawling");
	                    }
	                }
	            }
	            
	        }
	        result.setUserAgentMatched(userAgentMatched);
	        result.setStarMatched(starMatched);
	        result.setDisallowedDirsForUA(disallowedDirsForUA);
	        result.setDisallowedDirsForAll(disallowedDirsForAll);
			this.setContent (content);
			conn.disconnect ();
			}
			} catch (ProtocolException e) {
 			// TODO Auto-generated catch block

			e.printStackTrace ();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();

		}
		
		return result;
	}
	
	

	public HashSet<String> retrieveURLLinks () {
		/*
		 * ArrayList<String> links = new ArrayList<String>(); NodeList templinks
		 * = this.getDocWebPage().getElementsByTagName("a"); for(int i=0;
		 * i<templinks.getLength();i++){ Node link = templinks.item(i);
		 * 
		 * NamedNodeMap tempURL = link.getAttributes(); for(int j=0;
		 * j<tempURL.getLength();j++){
		 * 
		 * Node url = tempURL.item(j); if(url.getNodeName().equals("href")){
		 * String urlstring = url.getNodeValue();
		 * 
		 * 
		 * if( urlstring.equals("") || urlstring.equals("\r") ){ continue;
		 * }else{ try { if(!(urlstring.startsWith("http"))){
		 * if(this.url.toString().endsWith("/")){ if(urlstring.startsWith("/")){
		 * urlstring
		 * =this.url.toString().concat(urlstring.substring(0,urlstring.length
		 * ()-1)); }else{ urlstring = this.url.toString().concat(urlstring); }
		 * }else{ if(urlstring.startsWith("/")){ urlstring =
		 * this.url.toString().concat(urlstring); }else{
		 * urlstring=this.url.toString().concat("/").concat(urlstring); } } }
		 * //URL newURL = new URL(urlstring); URL newURL = new
		 * URL(this.url,urlstring); links.add(newURL.toString());
		 * 
		 * } catch (MalformedURLException e) { // TODO Auto-generated catch
		 * block System.out.println("Malformed URL Encountered"); continue; } }
		 * } } } return links;
		 */
		Set<String> links = new HashSet<String> ();

		Pattern pattern = Pattern.compile ("href\\s*=\\s*(\"|\')(.*?)(\"|\')\\s*",
				Pattern.CASE_INSENSITIVE);
		Pattern linkpattern = Pattern
				.compile ("(?<=(<link>))(.*?)(?=(</link>))");

		Matcher matcher = pattern.matcher (this.getContent ());
		Matcher linkmatcher = linkpattern.matcher (this.getContent ());
		// find and print all matches
		while (matcher.find ()) {
			//String w = matcher.group ();
			//String urlstring = w.split ("=")[1].trim();
			String urlstring = matcher.group(2);
			if (urlstring.equals ("") || urlstring.equals ("\r") || urlstring.contains("javascript:")) {
				continue;
			} else {
				URL newURL;
				try {
					newURL = new URL (this.url, urlstring);

					links.add (newURL.toString ());
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					log.debug (e.toString ());
				}
			}

		}
		while (linkmatcher.find ()) {
			String w2 = linkmatcher.group ();
			if (links.contains (w2)) {
				continue;
			} else {
				links.add (w2);
			}
		}
		return (HashSet<String>) links;
	}

}
