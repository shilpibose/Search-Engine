
package edu.upenn.cis555.indexer;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class LexiconEntryLocalReceived {

	@PrimaryKey(sequence="LexiconEntryLocalReceived_Sequence")
	Integer id;
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	String wordId;
	String word;
	int ndocs;
	InvertedBarrel invertedBarrel;
	
	
	public LexiconEntryLocalReceived(LexiconEntryLocal lexiconEntryLocal) {
	this.wordId = lexiconEntryLocal.getWordId();
	this.word = lexiconEntryLocal.getWord();
	this.ndocs = lexiconEntryLocal.getNdocs();
	this.invertedBarrel = lexiconEntryLocal.getInvertedBarrel();
	
	}
	public LexiconEntryLocalReceived(){
		
	}
	public int getNdocs() {
		return ndocs;
	}
	public InvertedBarrel getInvertedBarrel() {
		return invertedBarrel;
	}
	
	public void setNdocs(int ndocs) {
		this.ndocs = ndocs;
	}
	public void setInvertedBarrel(InvertedBarrel invertedBarrel) {
		this.invertedBarrel = invertedBarrel;
	}
	public String getWordId() {
		return wordId;
	}
	public void setWordId(String wordId) {
		this.wordId = wordId;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	
}
