package edu.upenn.cis555.crawler.bean;

import java.io.Serializable;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class PagerankDocument implements Serializable {

	@PrimaryKey
	String id;
	double pagerank;
	int outlinksCount;

	public int getOutlinksCount () {
		return outlinksCount;
	}

	public void setOutlinksCount (int outlinksCount) {
		this.outlinksCount = outlinksCount;
	}

	public String getId () {
		return id;
	}

	public void setId (String id) {
		this.id = id;
	}

	public double getPagerank () {
		return pagerank;
	}

	public void setPagerank (double pagerank) {
		this.pagerank = pagerank;
	}
}
