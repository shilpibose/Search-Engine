
package edu.upenn.cis555.indexer;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;
@Entity

public class WordDocumentHit {


	@PrimaryKey(sequence = "WordDocumentHitSequence")
	private Integer wordDocumentHitId;

	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	private String wordFk;
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	private String documentFk;
	
	private Hit hit;
	
	
	public Hit getHit() {
		return hit;
	}
	public void setHit(Hit hit) {
		this.hit = hit;
	}
	public Integer getWordDocumentHitId() {
		return wordDocumentHitId;
	}
	public String getWordFk() {
		return wordFk;
	}
	public String getDocumentFk() {
		return documentFk;
	}
	public void setWordDocumentHitId(Integer wordDocumentId) {
		this.wordDocumentHitId = wordDocumentId;
	}
	public void setWordFk(String wordFk) {
		this.wordFk = wordFk;
	}
	public void setDocumentFk(String documentFk) {
		this.documentFk = documentFk;
	}
	
	
}
