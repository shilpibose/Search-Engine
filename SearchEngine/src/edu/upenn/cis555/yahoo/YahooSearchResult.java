package edu.upenn.cis555.yahoo;

import java.util.ArrayList;

public class YahooSearchResult {

	String query;
	ArrayList<String> links;
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public ArrayList<String> getLinks() {
		return links;
	}
	public void setLinks(ArrayList<String> links) {
		this.links = links;
	}
	
}
