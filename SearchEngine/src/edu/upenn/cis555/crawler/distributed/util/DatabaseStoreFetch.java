package edu.upenn.cis555.crawler.distributed.util;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.bean.PagerankDocument;
import edu.upenn.cis555.crawler.bean.Robots;

public class DatabaseStoreFetch {

	private PrimaryIndex<String, Document> urlPrimaryIndex = null;
	private PrimaryIndex<String, Robots> robotsPrimaryIndex = null;
	private PrimaryIndex<String, PagerankDocument> pagerankPrimaryIndex = null;

	private SecondaryIndex<String, String, Document> crawledSecondaryIndex = null;

	public PrimaryIndex<String, Robots> getRobotsPrimaryIndex () {
		return robotsPrimaryIndex;
	}

	public void setRobotsPrimaryIndex (
			PrimaryIndex<String, Robots> robotsPrimaryIndex) {
		this.robotsPrimaryIndex = robotsPrimaryIndex;
	}

	public DatabaseStoreFetch (EntityStore dbTemp) {
		setUrlPrimaryIndex (dbTemp.getPrimaryIndex (String.class,
				Document.class));

		setCrawledSecondaryIndex (dbTemp.getSecondaryIndex (urlPrimaryIndex,
				String.class, "shaContent"));
		setRobotsPrimaryIndex (dbTemp.getPrimaryIndex (String.class,
				Robots.class));
		setPagerankPrimaryIndex(dbTemp.getPrimaryIndex(String.class, PagerankDocument.class));
	}

	public PrimaryIndex<String, PagerankDocument> getPagerankPrimaryIndex() {
		return pagerankPrimaryIndex;
	}

	public void setPagerankPrimaryIndex(
			PrimaryIndex<String, PagerankDocument> pagerankPrimaryIndex) {
		this.pagerankPrimaryIndex = pagerankPrimaryIndex;
	}

	public void setUrlPrimaryIndex (PrimaryIndex<String, Document> primaryIndex) {
		this.urlPrimaryIndex = primaryIndex;
	}

	public PrimaryIndex<String, Document> getUrlPrimaryIndex () {
		return urlPrimaryIndex;
	}

	public void setCrawledSecondaryIndex (
			SecondaryIndex<String, String, Document> crawledSecondaryIndex) {
		this.crawledSecondaryIndex = crawledSecondaryIndex;
	}

	public SecondaryIndex<String, String, Document> getCrawledSecondaryIndex () {
		return crawledSecondaryIndex;
	}

}
