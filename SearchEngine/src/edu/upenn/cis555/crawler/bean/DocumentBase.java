package edu.upenn.cis555.crawler.bean;

import java.io.Serializable;

public class DocumentBase implements Serializable {
	String url;
	String id;

	double weight_dj;

	public String getUrl () {
		return url;
	}

	public void setUrl (String url) {
		this.url = url;
	}

	public String getId () {
		return id;
	}

	public void setId (String shaURL) {
		this.id = shaURL;
	}

	public void setDj (double dj) {
		weight_dj = dj;
	}

	public double getDj () {
		return weight_dj;
	}
}
