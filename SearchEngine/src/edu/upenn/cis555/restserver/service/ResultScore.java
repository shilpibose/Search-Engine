package edu.upenn.cis555.restserver.service;

public class ResultScore {

	protected String docURL;

	protected double score;

	public ResultScore (String docURL, double score) {
		this.docURL = docURL;
		this.score = score;
	}

	public String getDocURL () {
		return docURL;
	}

	public double getScore () {
		return score;
	}
}
