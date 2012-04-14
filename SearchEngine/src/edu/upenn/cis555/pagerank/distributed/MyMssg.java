package edu.upenn.cis555.pagerank.distributed;

import edu.upenn.cis555.crawler.bean.Document;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;

public class MyMssg implements Message {
	private static final long serialVersionUID = 1971427247855604027L;
	private NodeHandle from;
	private String content;
	private Document docContent;
	private messageType type;
	private NodeHandle to;
	private long timestamp;
	private double pagerank;
	private double oldRank;
	private double newRank;
	private String toUrl;

	public NodeHandle getTo () {
		return to;
	}

	public void setTo (NodeHandle to) {
		this.to = to;
	}

	public static long getSerialversionuid () {
		return serialVersionUID;
	}

	public enum messageType {
		QUERY, RESULT, UPDATE;
	}

	public MyMssg (NodeHandle from, String content, messageType type) {
		this.setFrom (from);
		this.setContent (content);
		this.setType (type);
	}

	public MyMssg (NodeHandle from,Document content, messageType type) {
		this.setFrom(from);
		this.docContent = content;
		this.type = type;
	}

	/*public MyMssg (NodeHandle from,String toUrl, String content,Double rank, messageType type) {
		this.setFrom (from);
		this.setContent (content);
		this.setType (type);
		this.setPagerank(rank);
		this.setToUrl(toUrl);
	} */
	
	public MyMssg (NodeHandle from, String toUrl, Double oldfactor,Double newFactor, messageType type) {
		this.setFrom (from);
		this.setType (type);
		this.setOldRank(oldfactor);
		this.setNewRank(newFactor);
		this.setToUrl(toUrl);
	} 
	
	@Override
	public int getPriority () {
		return 0;
	}

	public NodeHandle getFrom () {
		return from;
	}

	public void setFrom (NodeHandle from) {
		this.from = from;
	}

	public String getContent () {
		return content;
	}

	public void setContent (String content) {
		this.content = content;
	}

	public messageType getType () {
		return type;
	}

	public void setType (messageType selectedMessageType) {
		this.type = selectedMessageType;
	}

	public void setTimestamp (long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp () {
		return timestamp;
	}

	public void setDocContent (Document docContent) {
		this.docContent = docContent;
	}

	public Document getDocContent () {
		return docContent;
	}

	public void setPagerank(double pagerank) {
		this.pagerank = pagerank;
	}

	public double getPagerank() {
		return pagerank;
	}

	public void setToUrl(String toUrl) {
		this.toUrl = toUrl;
	}

	public String getToUrl() {
		return toUrl;
	}

	public void setOldRank(double oldRank) {
		this.oldRank = oldRank;
	}

	public double getOldRank() {
		return oldRank;
	}

	public void setNewRank(double newRank) {
		this.newRank = newRank;
	}

	public double getNewRank() {
		return newRank;
	}

}
