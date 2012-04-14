package edu.upenn.cis555.node.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

		
		
	    public static String hashSha1 ( String url ) throws NoSuchAlgorithmException
	    {	
	    	String hashValue;
	    	MessageDigest md = MessageDigest.getInstance("SHA-1");
	    	byte [] hashStr = new byte[20];
	    	
	    	
	    	md.update(url.getBytes());
	    	hashStr = md.digest();
	    	BigInteger bigInt = new BigInteger ( hashStr );

	    	bigInt = bigInt.abs();
	    	hashValue = bigInt.toString(16);
	    	return hashValue;
	    	
	    	
	    } 
	    
	    public static  String hashSha1Content ( String payload ) throws NoSuchAlgorithmException
	    {
	    	MessageDigest md = MessageDigest.getInstance("SHA-1");
	    	byte [] hashStr = new byte[20];
	    	
	    	
	    	md.update(payload.getBytes());
	    	hashStr = md.digest();
	    	BigInteger bigInt = new BigInteger ( hashStr );

	    	bigInt = bigInt.abs();
	    	String hashValue = bigInt.toString(16);
	    	return hashValue;
	    	
	    	
	    } 
	    
	    
}
