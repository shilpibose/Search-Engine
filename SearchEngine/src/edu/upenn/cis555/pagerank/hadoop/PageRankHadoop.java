package edu.upenn.cis555.pagerank.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.bean.PagerankDocument;
import edu.upenn.cis555.crawler.distributed.Hash;
import edu.upenn.cis555.crawler.distributed.util.DatabaseStoreFetch;
import edu.upenn.cis555.crawler.distributed.util.DatabaseUtils;
import edu.upenn.cis555.node.NodeManager;
import edu.upenn.cis555.node.utils.NodeUtils;
import edu.upenn.cis555.pagerank.PageRank;

public class PageRankHadoop implements PageRank {

	private static Logger log = Logger.getLogger (PageRankHadoop.class);

	protected EntityStore store;

	/**
	 * Class containing the query string to which the result will be filled in
	 */
	class InLinksRequest {

		protected String url;

		protected Document document;

		public InLinksRequest (String url) {
			this.url = url;
		}

		public Document getResult () {
			return document;
		}

		public void setResult (Document result) {
			document = result;
		}
	}

	/** Map to book keep the requests and responses */
	protected Map<String, ArrayList<InLinksRequest>> requestMap = new HashMap<String, ArrayList<InLinksRequest>> ();

	protected InLinksRequest getResult (String url) throws InterruptedException {
		InLinksRequest rankRequest = new InLinksRequest (url);
		synchronized (requestMap) {
			ArrayList<InLinksRequest> requests = requestMap.get (url);
			if (requests != null) {
				requests.add (rankRequest);
			} else {
				ArrayList<InLinksRequest> requestList = new ArrayList<InLinksRequest> ();
				requestList.add (rankRequest);
				requestMap.put (url, requestList);
				System.err.println ("Asking for rank for " + url);
				NodeManager.getInstance ().getCrawler ().getCrawlerApp ()
						.wrapperIncomingSendMessage (url);
			}
		}

		/* Wait for the result to be filled in */
		synchronized (rankRequest) {
			rankRequest.wait ();
		}
		return rankRequest;
	}

	public void deliverResult (String url, Document document) {
		System.err.println ("Delivering the result to the requested thread");
		synchronized (requestMap) {
			ArrayList<InLinksRequest> requests = requestMap.get (url);
			if (requests == null) {
				System.err.println ("No requests waiting for the result?! "
						+ url);
			} else {
				System.err.println ("There are " + requests.size ()
						+ " requests waiting for the result");

				for (InLinksRequest r : requests) {
					r.setResult (document);
					synchronized (r) {
						r.notify ();
					}
				}
				requestMap.remove (url);
			}
		}
	}

	public PageRankHadoop (EntityStore store) throws IOException {
		this.store = store;
	}

	protected void addToDB (String docId, String pageRankVal, String outLinks) {
		System.err.println ("Adding " + docId + " with " + pageRankVal + " "
				+ outLinks);
		PagerankDocument doc = new PagerankDocument ();
		doc.setId (docId);
		doc.setPagerank (Double.parseDouble (pageRankVal));
		doc.setOutlinksCount (Integer.parseInt (outLinks));
		DatabaseUtils.insertPagerank (this.store, doc);
	}

	protected double getRankFromDB (String url) {
		try {
			PagerankDocument doc = DatabaseUtils.retrievePagerank (this.store,
					Hash.hashSha1 (url));
			if (doc == null) {
				System.err.println ("Rank for " + url
						+ " is not there in DB, querying it");
				return 0;
			}
			System.err.println ("Document rank: " + doc.getPagerank ());
			return doc.getPagerank ();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace ();
		}
		return 0;
	}

	@Override
	public double getPageRankFor (String url) throws InterruptedException {
		System.err.println ("Getting page rank for: " + url);
		double rank = getRankFromDB (url);
		if (rank == 0) {
			System.err.println ("Rank for " + url
					+ " is not there in DB, querying");
			InLinksRequest result = getResult (url);
			Document doc = result.getResult ();

			try {
				if (doc.getIncomingLinks () == null) {
					rank = 0.15;
					addToDB (Hash.hashSha1 (url), rank + "", doc
							.getOutgoingLinks ().size ()
							+ "");
				} else {
					rank = calculatePageRank (doc.getIncomingLinks ());
					addToDB (Hash.hashSha1 (url), rank + "", doc
							.getOutgoingLinks ().size ()
							+ "");
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace ();
			}
		}
		return rank;
	}

	protected int getOutLinksFor (String url) {
		try {
			PagerankDocument doc = DatabaseUtils.retrievePagerank (this.store,
					Hash.hashSha1 (url));
			if (doc == null) {
				System.err.println ("Failed to get outlinks for " + url);
				return 0;
			}
			return doc.getOutlinksCount ();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace ();
		}
		return 0;
	}

	public double calculatePageRank (HashSet<String> links)
			throws InterruptedException {
		double score = 0;
		for (String s : links) {
			System.err.println ("Inlinks: " + s.length () + " = " + s);
			if (s.length () == 0)
				continue;
			score += getPageRankFor (s) / getOutLinksFor (s);
		}
		score += 0.15d;
		System.err.println ("Calculated page rank " + score);
		return score;
	}

	public void loadPageRankFile () throws IOException {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);
		DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
		EntityCursor<PagerankDocument> crawledCursor = dsf
				.getPagerankPrimaryIndex ().entities (txn, null);
		try {
			for (PagerankDocument p : crawledCursor) {
				crawledCursor.delete ();
			}
			crawledCursor.close ();
			crawledCursor = null;
			txn.commit ();
			txn = null;
		} catch (Exception e) {
			if (crawledCursor != null) {
				crawledCursor.close ();
			}
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}

		File file = new File (NodeUtils.getInstance ().getValue (
				Constants.PAGERANK_FILE_DIR), NodeUtils.getInstance ()
				.getValue (Constants.PAGERANK_FILENAME));

		if (! (file.exists ())) {
			System.err.println ("Pagerank file does not exist");
			return;
		}

		BufferedReader reader = new BufferedReader (new FileReader (file));
		String line = null;
		while ( (line = reader.readLine ()) != null) {
			String[] components = line.split ("[ \t]+");
			addToDB (components[0], components[1], components[2]);
		}
		System.err.println ("Loaded the PR file");
	}
}
