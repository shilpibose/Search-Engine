package edu.upenn.cis555.node.utils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import edu.upenn.cis555.indexer.Index;

import rice.pastry.Id;

public class Utility {
	private static final Logger log = Logger.getLogger (Index.class);

	private static final String CHARSET = "iso-8859-1";
	private static MessageDigest md;
	static {
		try {
			md = MessageDigest.getInstance ("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
	}

	public static String hash (String text) {
	String hashvalue = null;
		try {
			hashvalue =  Hash.hashSha1 (text);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hashvalue;
	}

	
	public static Id getIdFromKey (String key) {
		String hex  = hash (key);
		Id id = Id.build(hex);
	//	Id id = Id.build (shaDigest);
		return id;
	}

}


/*



byte[] sha1hash = new byte[20];
try {
	md.update (text.getBytes (CHARSET));

sha1hash = md.digest ();
} 
catch(ArrayIndexOutOfBoundsException ae){
	System.out.println("ArrayIndexOutofBoundException for:"+text);
	log.error(ae);
	}

return sha1hash;
*/