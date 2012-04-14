package edu.upenn.cis555.indexer;

import java.io.Serializable;
import java.util.ArrayList;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class InvertedBarrel implements Serializable {

	ArrayList<DocumentHits> documentHitsList;

	public ArrayList<DocumentHits> getDocumentHitsList () {
		return documentHitsList;
	}

	public void setDocumentHitsList (ArrayList<DocumentHits> documentHitsList) {
		this.documentHitsList = documentHitsList;
	}

	public InvertedBarrel () {
		documentHitsList = new ArrayList<DocumentHits> ();
	}

	public int size () {
		return documentHitsList.size ();
	}

	public void sumInvertedBarrel (InvertedBarrel invertedBarrel) {
		ArrayList<DocumentHits> otherDocumentHitsList = invertedBarrel
				.getDocumentHitsList ();
		documentHitsList.addAll (otherDocumentHitsList);

	}
}
