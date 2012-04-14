package edu.upenn.cis555.crawler.bean;

import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;

public class SelectQueue implements Comparable<SelectQueue> {

	private LinkedBlockingQueue<Document> hostQueue;
	private Long timeInMillis;
	private String host;
	public void setHostQueue(LinkedBlockingQueue<Document> hostQueue) {
		this.hostQueue = hostQueue;
	}
	public LinkedBlockingQueue<Document> getHostQueue() {
		return hostQueue;
	}
	public void setTimeInMillis(Long timeInMillis) {
		this.timeInMillis = timeInMillis;
	}
	public Long getTimeInMillis() {
		return timeInMillis;
	}
	@Override
	public int compareTo(SelectQueue arg0) {
		// TODO Auto-generated method stub
		if(this.timeInMillis < arg0.getTimeInMillis()){
			return -1;
		}
		if(this.timeInMillis > arg0.getTimeInMillis()){
			return 1;
		}
		return 0;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getHost() {
		return host;
	}
}
