
package edu.upenn.cis555.indexer;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;
@Entity

public class WordDocumentWeight {


	@PrimaryKey(sequence = "WordDocumentWeightSequence")
	private Integer wordDocumentWeightId;

	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	private String documentFk;
	
	private String url;
	double weight;
	
	
	public Integer getWordDocumentWeightId() {
		return wordDocumentWeightId;
	}
	public String getDocumentFk() {
		return documentFk;
	}
	public void setWordDocumentWeightId(Integer wordDocumentId) {
		this.wordDocumentWeightId = wordDocumentId;
	}
	public void setDocumentFk(String documentFk) {
		this.documentFk = documentFk;
	}
	
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
	
}
