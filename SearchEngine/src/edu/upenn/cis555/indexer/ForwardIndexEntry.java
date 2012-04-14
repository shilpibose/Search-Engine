
package edu.upenn.cis555.indexer;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class ForwardIndexEntry {

	@PrimaryKey
	String documentId;
	String url;
	int nwords;
	int maxWordFrequency;
	ForwardBarrel forwardBarrel;
	
	
	public int getNwords() {
		return nwords;
	}
	public ForwardBarrel getForwardBarrel() {
		return forwardBarrel;
	}
	
	public void setNwords(int nwords) {
		this.nwords = nwords;
	}
	public void setForwardBarrel(ForwardBarrel forwardBarrel) {
		this.forwardBarrel = forwardBarrel;
	}
	public String getDocumentId() {
		return documentId;
	}
	public void setDocumentId(String documentId) {
		this.documentId = documentId;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public int getMaxWordFrequency() {
		return maxWordFrequency;
	}
	public void setMaxWordFrequency(int maxWordFrequency) {
		this.maxWordFrequency = maxWordFrequency;
	}
	
}
