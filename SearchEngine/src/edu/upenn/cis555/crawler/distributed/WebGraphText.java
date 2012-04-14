package edu.upenn.cis555.crawler.distributed;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.common.Constants;
import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.distributed.util.DatabaseUtils;
import edu.upenn.cis555.node.utils.NodeUtils;

public class WebGraphText {
	private final Logger log = Logger.getLogger (WebGraphText.class);
	EntityStore store;
	ArrayList<Document> allCrawled;

	WebGraphText (EntityStore store) {
		this.store = store;
		allCrawled = DatabaseUtils.retrieveAllCrawledObjects (store);

	}

	public void write () {
		try {
			File dir = new File (NodeUtils.getInstance ().getValue (
					Constants.PAGERANK_FILE_DIR));
			File file = new File (NodeUtils.getInstance ().getValue (
					Constants.PAGERANK_FILE_DIR), (NodeUtils.getInstance ()
							.getValue (Constants.PAGERANK_FILENAME))
							+ "_" + InetAddress.getLocalHost ().getHostAddress ());
			System.out.println ("FILE :" + file);

			if (!dir.exists ()) {
				boolean statusdir = dir.mkdir ();
				if (statusdir) {
					log.info ("Directory got created.");
				} else {
					log.info ("Directory never got created.");
				}
			}
			if (!file.exists ()) {

				boolean status = file.createNewFile ();
				if (status) {
					log.info ("File got created");
				} else {
					log.info ("File never got created");
				}

			}

			PrintWriter pw = new PrintWriter (file);
			Iterator<Document> iter = allCrawled.iterator ();
			log.info ("Writing to the text file");
			while (iter.hasNext ()) {
				boolean isSeed=false;
				boolean isSeedwithMultiple=false;
				boolean isSelf = false;
				Document nextDoc = iter.next ();
				HashSet<String> incoming = nextDoc.getIncomingLinks ();
				Iterator<String> iterIncoming = incoming.iterator ();

				while (iterIncoming.hasNext ()) {

					String url = iterIncoming.next ();

					// check for seed url
					if(url.equals("")){

						//pw.write (nextDoc.getUrl() + " " + nextDoc.getUrl ());
						if(incoming.size() == 1){
							isSeed = true;
							pw.write (nextDoc.getId() + " " + nextDoc.getId ());
							pw.write('\n');
							break;
						}else{
							isSeedwithMultiple = true;
						}

					}else{
						
						if(url.equals(nextDoc.getId())){
							//remove self links 
							isSelf = true;
							continue;
						}
						pw.write (Hash.hashSha1(url) + " " + nextDoc.getId ());
						pw.write ('\n');
					}



					
				}
				if(isSelf){
					incoming.remove(nextDoc.getId());
					nextDoc.setIncomingLinks(incoming);
					DatabaseUtils.insertCrawledObject(nextDoc, store);
				}
				if(isSeed){
					incoming.remove("");
					incoming.add(nextDoc.getId());
					nextDoc.setIncomingLinks(incoming);
					
					DatabaseUtils.insertCrawledObject(nextDoc, store);
				}
				if(isSeedwithMultiple){
					incoming.remove("");
					
					nextDoc.setIncomingLinks(incoming);
				
					DatabaseUtils.insertCrawledObject(nextDoc, store);
				}


			}
			pw.flush ();
			log.info ("Writing to the text file Finished");
			pw.close ();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
