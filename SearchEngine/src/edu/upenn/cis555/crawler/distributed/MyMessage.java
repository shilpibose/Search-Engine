package edu.upenn.cis555.crawler.distributed;

import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.bean.DocumentBase;
import edu.upenn.cis555.crawler.bean.DocumentResult;

public class MyMessage implements Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1971427247855604027L;
	private NodeHandle from;
	private String content;
	private messageType selectedMessageType;
	private NodeHandle to;
	private int servletPort;
	private String servletIP;
	private Document resultContent;
	private DocumentBase documentWeight;
	private DocumentResult result;

	public NodeHandle getTo () {
		return to;
	}

	public void setTo (NodeHandle to) {
		this.to = to;
	}

	public int getServletPort () {
		return servletPort;
	}

	public void setServletPort (int servletPort) {
		this.servletPort = servletPort;
	}

	public String getServletIP () {
		return servletIP;
	}

	public void setServletIP (String servletIP) {
		this.servletIP = servletIP;
	}

	public static long getSerialversionuid () {
		return serialVersionUID;
	}

	public enum messageType {
		QUERY, ACK, GETINCOMING, RESULTINCOMING, WEIGHT, WEIGHT_RESULT, FETCH, FETCH_RESULT, FETCH_PAGERANK, RETURN_PAGERANK;
	}

	boolean wantResponse = true;

	public MyMessage (NodeHandle from, String content, messageType type) {
		this.from = from;
		this.content = content;
		this.selectedMessageType = type;
	}

	public MyMessage (NodeHandle from, Document content, messageType type) {
		this.from = from;
		this.resultContent = content;
		this.selectedMessageType = type;
	}

	public MyMessage (NodeHandle from, DocumentBase documentWeight,
			messageType type) {
		this.from = from;
		this.documentWeight = documentWeight;
		this.selectedMessageType = type;
	}

	public MyMessage (NodeHandle from, DocumentResult result, messageType type) {
		this.from = from;
		this.result = result;
		this.selectedMessageType = type;
	}

	@Override
	public int getPriority () {
		// TODO Auto-generated method stub
		return 0;
	}

	public DocumentResult getResult () {
		return result;
	}

	public void setResult (DocumentResult result) {
		this.result = result;
	}

	public Document getResultContent () {
		return resultContent;
	}

	public void setResultContent (Document content) {
		this.resultContent = content;
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

	public messageType getSelectedMessageType () {
		return selectedMessageType;
	}

	public void setSelectedMessageType (messageType selectedMessageType) {
		this.selectedMessageType = selectedMessageType;
	}

	public boolean isWantResponse () {
		return wantResponse;
	}

	public void setWantResponse (boolean wantResponse) {
		this.wantResponse = wantResponse;
	}

	public DocumentBase getDocBase () {
		return documentWeight;
	}
}
