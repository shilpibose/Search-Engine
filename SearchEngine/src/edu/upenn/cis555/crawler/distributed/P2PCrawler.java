package edu.upenn.cis555.crawler.distributed;

import java.util.ArrayList;

import rice.pastry.PastryNode;

import com.sleepycat.persist.EntityStore;

public class P2PCrawler {

	private ArrayList<String> listSeeds;
	private EntityStore dbStore;
	private CrawlerApplication pastryApplication;
	private PastryNode node;
	private long crawledDocuments;

	public P2PCrawler (ArrayList<String> seedURLs, EntityStore store,
			PastryNode node) {
		this.listSeeds = seedURLs;
		this.dbStore = store;
		this.node = node;
		pastryApplication = new CrawlerApplication (this.node, dbStore,
				this.listSeeds);
	}

	public void startCrawling () {
		pastryApplication.start ();
	}

	public long stopCrawling () {
		pastryApplication.shutdown ();
		WebGraphText linkGraph = new WebGraphText (this.dbStore);
		linkGraph.write ();
		crawledDocuments = pastryApplication.getCrawledDocuments ();

		return crawledDocuments;
	}

	public CrawlerApplication getCrawlerApp () {
		return pastryApplication;
	}
}
