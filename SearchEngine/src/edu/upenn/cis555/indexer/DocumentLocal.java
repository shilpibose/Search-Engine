package edu.upenn.cis555.indexer;

import java.io.Serializable;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
@Entity
public class DocumentLocal implements Serializable {

	@PrimaryKey
	private String documentId;
	private String url;
	double calcWeight;
	
	
	
	public String getDocumentId() {
		return documentId;
	}

	public void setDocumentId(String documentId) {
		this.documentId = documentId;
	}
	public double getCalcWeight() {
		return calcWeight;
	}

	public void setCalcWeight(double calcWeight) {
		this.calcWeight = calcWeight;
	}
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	
}
