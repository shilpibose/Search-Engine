package edu.upenn.cis555.indexer;

import java.io.Serializable;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class DocumentHits implements Serializable {

	String documentId;
	String url;
	int maxWordFrequency;
	int nhits;
	double tf;
	HitsList hitsList;
	double weight;

	public String getDocumentId () {
		return documentId;
	}

	public int getNhits () {
		return nhits;
	}

	public HitsList getHitsList () {
		return hitsList;
	}

	public void setDocumentId (String documentId) {
		this.documentId = documentId;
	}

	public void setNhits (int nhits) {
		this.nhits = nhits;
	}

	public void setHitsList (HitsList hitsList) {
		this.hitsList = hitsList;
	}

	public int getMaxWordFrequency () {
		return maxWordFrequency;
	}

	public void setMaxWordFrequency (int maxWorFrequency) {
		this.maxWordFrequency = maxWorFrequency;
	}

	public String getUrl () {
		return url;
	}

	public void setUrl (String url) {
		this.url = url;
	}

	public double getWeight () {
		return weight;
	}

	public void setWeight (double weight) {
		this.weight = weight;
	}

	public double getTf () {
		return tf;
	}

	public void setTf (double tf) {
		this.tf = tf;
	}

	@Override
	public int hashCode () {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ( (documentId == null) ? 0 : documentId.hashCode ());
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
		DocumentHits other = (DocumentHits) obj;
		if (documentId == null) {
			if (other.documentId != null)
				return false;
		} else if (!documentId.equals (other.documentId))
			return false;
		return true;
	}

	public String toString () {
		return documentId + "\n";
	}
}
