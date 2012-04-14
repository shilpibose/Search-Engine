package edu.upenn.cis555.crawler.bean;

import java.io.Serializable;

public class DocumentResult implements Serializable {

	String url;
	String title;
	String snippet;
	int hits;

	public int getHits () {
		return hits;
	}

	public void setHits (int hits) {
		this.hits = hits;
	}

	public String getUrl () {
		return url;
	}

	public void setUrl (String url) {
		this.url = url;
	}

	public String getTitle () {
		return title;
	}

	public void setTitle (String title) {
		this.title = title;
	}

	public String getSnippet () {
		return snippet;
	}

	public void setSnippet (String snippet) {
		this.snippet = snippet;
	}

	public String toString () {
		return snippet;
	}
}
