package edu.upenn.cis555.crawler.bean;

import java.io.Serializable;
import java.util.HashSet;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class Document implements Serializable {

	String url;

	@PrimaryKey
	String id;

	double weight_dj;

	HashSet<String> incomingLinks;
	@SecondaryKey (relate = Relationship.ONE_TO_ONE)
	String shaContent;
	String content;
	int hitcount;
	double pagerank;
	boolean deadLink;

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

	public boolean isDeadLink () {
		return deadLink;
	}

	public void setDeadLink (boolean deadLink) {
		this.deadLink = deadLink;
	}

	int maxWordFrequency; // this field used by the indexer

	public double getPagerank () {
		return pagerank;
	}

	public void setPagerank (double pagerank) {
		this.pagerank = pagerank;
	}

	public int getHitcount () {
		return hitcount;
	}

	public void setHitcount (int hitcount) {
		this.hitcount = hitcount;
	}

	public String getContent () {
		return content;
	}

	public void setContent (String content) {
		this.content = content;
	}

	Long lastVisitedTime;
	String contentType;
	int contentLength;

	HashSet<String> outgoingLinks;

	public HashSet<String> getIncomingLinks () {
		return incomingLinks;
	}

	public void setIncomingLinks (HashSet<String> incomingLinks) {
		this.incomingLinks = incomingLinks;
	}

	public HashSet<String> getOutgoingLinks () {
		return outgoingLinks;
	}

	public void setOutgoingLinks (HashSet<String> outgoingLinks) {
		this.outgoingLinks = outgoingLinks;
	}

	public String getContentType () {
		return contentType;
	}

	public void setContentType (String contentType) {
		this.contentType = contentType;
	}

	public int getContentLength () {
		return contentLength;
	}

	public void setContentLength (int contentLength) {
		this.contentLength = contentLength;
	}

	public String getShaContent () {
		return shaContent;
	}

	public void setShaContent (String shaContent) {
		this.shaContent = shaContent;
	}

	public Long getLastVisitedTime () {
		return lastVisitedTime;
	}

	public void setLastVisitedTime (Long lastVisitedTime) {
		this.lastVisitedTime = lastVisitedTime;
	}

	@Override
	public int hashCode () {
		final int prime = 31;
		int result = 1;
		result = prime * result + ( (url == null) ? 0 : url.hashCode ());
		return result;
	}

	@Override
	public boolean equals (Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass () != obj.getClass ())
			return false;
		Document other = (Document) obj;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals (other.url))
			return false;
		return true;
	}

	public int getMaxWordFrequency () {
		return maxWordFrequency;
	}

	public void setMaxWordFrequency (int maxWordFrequency) {
		this.maxWordFrequency = maxWordFrequency;
	}

}
