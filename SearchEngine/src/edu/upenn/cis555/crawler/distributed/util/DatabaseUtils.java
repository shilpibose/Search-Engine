package edu.upenn.cis555.crawler.distributed.util;

import java.util.ArrayList;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.bean.PagerankDocument;
import edu.upenn.cis555.crawler.bean.Robots;

public class DatabaseUtils {

	public static void insertCrawledObject (Document newCrawledURL,
			EntityStore store) {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);
		try{

			DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
			dsf.getUrlPrimaryIndex ().put (txn, newCrawledURL);
			txn.commit();
		}catch(Exception e){
			if(txn!=null){
				txn.abort();
				txn = null;
			}
		}

	}

	public static Document retrieveCrawledObject (EntityStore store, String url) {
		DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
		Document newCrawled = dsf.getUrlPrimaryIndex ().get (url);
		return newCrawled;
	}

	public static ArrayList<Document> retrieveAllCrawledObjects (
			EntityStore store) {

		ArrayList<Document> listCrawled = new ArrayList<Document> ();
		DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
		EntityCursor<Document> crawledCursor = dsf.getUrlPrimaryIndex ()
		.entities ();

		//int i=0;
		for (Document url : crawledCursor) {
			//System.out.println("Doc Mila "+ url.getUrl());
			//i++;
			listCrawled.add (url);
		}
		//System.out.println("I got "+i+" docs");
		crawledCursor.close ();
		return listCrawled;

	}

	public static int retrieveCountObjects (
			EntityStore store) {
		int count =0;
		DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
		EntityCursor<Document> crawledCursor = dsf.getUrlPrimaryIndex ()
		.entities ();

		//int i=0;
		for (Document url : crawledCursor) {
			//System.out.println("Doc Mila "+ url.getUrl());
			//i++;
			count++;
		}
		//System.out.println("I got "+i+" docs");
		crawledCursor.close ();
		return count;

	}


	public static Document retrieveCrawledFromContent (EntityStore store,
			String content) {
		DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
		Document newCrawled = dsf.getCrawledSecondaryIndex ().get (content);
		return newCrawled;

	}

	public static PagerankDocument retrievePagerank(EntityStore store, String id){
		DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
		PagerankDocument newPageRank = dsf.getPagerankPrimaryIndex().get(id);
		return newPageRank;
	}

	public static void insertPagerank(EntityStore store, PagerankDocument doc){
		DatabaseStoreFetch dsf = new DatabaseStoreFetch(store);
		dsf.getPagerankPrimaryIndex().put(null, doc);
	}
	public static void insertRobots (Robots newRobotsText, EntityStore store) {
		Transaction txn = store.getEnvironment ().beginTransaction (null, null);
		try{

			DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
			dsf.getRobotsPrimaryIndex ().put (null, newRobotsText);
			txn.commit();
			txn = null;
		}catch(Exception e){
			if (txn != null) {
				txn.abort ();
				txn = null;
			}
		}
	}

	public static Robots retrieveRobotsText (EntityStore store, String host) {
		DatabaseStoreFetch dsf = new DatabaseStoreFetch (store);
		Robots newRobots = dsf.getRobotsPrimaryIndex ().get (host);
		return newRobots;
	}

}
