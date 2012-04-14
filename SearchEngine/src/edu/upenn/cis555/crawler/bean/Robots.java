package edu.upenn.cis555.crawler.bean;

import java.io.Serializable;
import java.util.ArrayList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Robots implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4168215934277041472L;

	@PrimaryKey
	String host;
	boolean userAgentMatched;
	boolean starMatched;
	public boolean isUserAgentMatched() {
		return userAgentMatched;
	}

	public void setUserAgentMatched(boolean userAgentMatched) {
		this.userAgentMatched = userAgentMatched;
	}

	public boolean isStarMatched() {
		return starMatched;
	}

	public void setStarMatched(boolean starMatched) {
		this.starMatched = starMatched;
	}

	public ArrayList<String> getDisallowedDirsForAll() {
		return disallowedDirsForAll;
	}

	public void setDisallowedDirsForAll(ArrayList<String> disallowedDirsForAll) {
		this.disallowedDirsForAll = disallowedDirsForAll;
	}

	public ArrayList<String> getDisallowedDirsForUA() {
		return disallowedDirsForUA;
	}

	public void setDisallowedDirsForUA(ArrayList<String> disallowedDirsForUA) {
		this.disallowedDirsForUA = disallowedDirsForUA;
	}

	ArrayList<String> disallowedDirsForAll;
	ArrayList<String> disallowedDirsForUA;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}


	
	
	
}
