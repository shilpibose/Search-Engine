
package edu.upenn.cis555.indexer;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class DocumentLocalReceived {

	@PrimaryKey(sequence="DocumentLocalReceived_Sequence")
	Integer id;
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	String documentId;
	String url;
	double calcWeight;
	
	
	public DocumentLocalReceived(DocumentLocal documentLocal) {
	this.documentId  = documentLocal.getDocumentId();
	this.url = documentLocal.getUrl();
	this.calcWeight = documentLocal.getCalcWeight();
	
	}
	public DocumentLocalReceived(){
		
	}
	
	public Integer getId() {
		return id;
	}
	public String getDocumentId() {
		return documentId;
	}
	public String getUrl() {
		return url;
	}
	public double getCalcWeight() {
		return calcWeight;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public void setDocumentId(String documentId) {
		this.documentId = documentId;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public void setCalcWeight(double calcWeight) {
		this.calcWeight = calcWeight;
	}
	
}
