package edu.upenn.cis555.restserver.server.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Class containing utility functions on date, time and MIME type aiding in
 * processing HTTP requests
 */
public class ServerUtils {

	private static final Logger log = Logger.getLogger (ServerUtils.class);

	/**
	 * Returns Time in the format suggested in HTTP 1.1 protocol
	 */
	public static String timeNow () {
		return timeString (Calendar.getInstance ().getTimeInMillis ());
	}

	/**
	 * Returns Time in the format suggested in HTTP 1.1 protocol
	 */
	public static String timeString (long time) {
		SimpleDateFormat sdf = new SimpleDateFormat (
				"EEE, d MMM yyyy HH:mm:ss z");
		return sdf.format (new Date (time));
	}

	/**
	 * Returns Time in the format suggested for XML
	 */
	public static String xmlTimeString (long time) {
		SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
		return sdf.format (new Date (time));
	}

	public static Date whichFormatOfDate (String ifModifiedValue) {
		DateFormat df = null;
		Date ifModifiedDate;

		df = new SimpleDateFormat ("EEE, d MMM yyyy HH:mm:ss z");
		try {
			ifModifiedDate = df.parse (ifModifiedValue);
			return ifModifiedDate;
		} catch (ParseException e) {
		}

		df = new SimpleDateFormat ("EEEE, dd-MMM-yy HH:mm:ss z");
		try {
			ifModifiedDate = df.parse (ifModifiedValue);
			return ifModifiedDate;
		} catch (ParseException e) {
		}

		df = new SimpleDateFormat ("EEE MMM d HH:mm:ss yyyy");
		try {
			ifModifiedDate = df.parse (ifModifiedValue);
			return ifModifiedDate;
		} catch (ParseException e) {
		}

		log.error ("Matches none of the date formats");
		return null;
	}

	public static String getSHA (String text) {
		int value = 0;
		StringBuffer valStr = new StringBuffer ();
		MessageDigest md;

		try {
			md = MessageDigest.getInstance ("SHA-1");
			byte[] sha1hash = new byte[40];
			md.update (text.getBytes ("iso-8859-1"), 0, text.length ());
			sha1hash = md.digest ();

			for (int i = 0; i < sha1hash.length; i++) {
				value = (sha1hash[i] & 0x00F0);
				value = value >> 4;
				valStr = valStr.append (Integer.toHexString (value));
				value = (sha1hash[i] & 0x000F);
				valStr = valStr.append (Integer.toHexString (value));
			}
		} catch (NoSuchAlgorithmException e) {
		} catch (UnsupportedEncodingException e) {
		}
		return valStr.toString ();
	}

	public static void infoLog (Object message) {
		log.info (message);
	}

	public static void errorLog (Object message) {
		log.error (message);
	}
}
