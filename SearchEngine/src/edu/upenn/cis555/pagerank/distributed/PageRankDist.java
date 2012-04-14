package edu.upenn.cis555.pagerank.distributed;

import rice.p2p.commonapi.Node;
import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.pagerank.PageRank;

public class PageRankDist implements PageRank {

	private EntityStore dbStore;
	private RankerApplication pastryApp;
	private Node node;

	public PageRankDist (EntityStore store, Node node) {
		this.dbStore = store;
		this.node = node;
		pastryApp = new RankerApplication (this.node, dbStore);
	}

	public void startRanking (int timeLimit) {
		System.out.println ("PageRank Started");
		/*
		 * try { Thread.sleep(1000); } catch (InterruptedException e) {
		 * e.printStackTrace(); }
		 */
		pastryApp.start (timeLimit);
	}

	@Override
	public double getPageRankFor (String url) throws InterruptedException {
		double rank = DocumentPageRankManager.getInstance ().getResult (url);
		if (rank == 1.0d) {
			return 0.15;
		}
		return rank;
	}
}
