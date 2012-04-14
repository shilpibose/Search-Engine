package edu.upenn.cis555.crawler.distributed.util;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.upenn.cis555.node.utils.Hash;

import rice.p2p.commonapi.Id;
import rice.pastry.commonapi.PastryIdFactory;

public class ServerUtils {
	/**
     * String pattern checking, alternative of .contains method of String class.
     *
     * @param commandName, the string into which search operation will
     * be performed
     * @param tester, the string which will be searched in the 1st argument
     * @return true, iff, the string commandName has tester
     * */
	public static Boolean matchString(String commandName, String tester) {
	        // TODO Auto-generated method stub

	        try {
	        	
	            Pattern pCRLF = Pattern.compile(tester);
	            Matcher m = pCRLF.matcher(commandName);

	            if (m.find()) {
	                return true;


	            } else {
	                return false;

	            }
	        } catch (Exception e) {
	            // TODO Auto-generated catch block
	            return false;
	        }
	    }
	
	 public static String getCurrentDate(String dateFormat) {
		    Calendar cal = Calendar.getInstance();
		    cal.setTimeZone(TimeZone.getTimeZone("GMT"));
		    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		    return sdf.format(cal.getTime());

		  }
		public static Id getIdFromBytes(String host) {
          try {
			return rice.pastry.Id.build(Hash.hashSha1(host));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
			
		}
		
    }
	 public static String getDomain(String urlString) throws MalformedURLException, UnknownHostException{
		 String domain;
	
			 
			URL tempURL = new URL(urlString);
			domain = tempURL.getHost();
			InetAddress addr=InetAddress.getByName(domain);
			domain = addr.getHostName();
			return domain;
	 }
	 
}
