package edu.upenn.cis555.pagerank.distributed;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;

import com.sleepycat.persist.EntityStore;

import edu.upenn.cis555.crawler.bean.Document;
import edu.upenn.cis555.crawler.distributed.Hash;
import edu.upenn.cis555.crawler.distributed.util.DatabaseUtils;
import edu.upenn.cis555.crawler.distributed.util.ServerUtils;
import edu.upenn.cis555.node.NodeManager;
import edu.upenn.cis555.pagerank.distributed.MyMssg.messageType;

class Inlink
{
	public String url;
	public int numOut;
	public double rank;
}

/*class Request
{
	public String url;
	public String inurl;
	public double pagerank;
	
	public Request(String url,String inurl,Double rank)
	{
		this.url=url;
		this.inurl=inurl;
		this.pagerank=rank;
	}
}*/

class Request
{
	public String url;
	public double oldRank;
	public double newRank;
		
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(newRank);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(oldRank);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Request other = (Request) obj;
		if (Double.doubleToLongBits(newRank) != Double
				.doubleToLongBits(other.newRank))
			return false;
		if (Double.doubleToLongBits(oldRank) != Double
				.doubleToLongBits(other.oldRank))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

	public Request(String url,Double oldRank,Double newRank)
	{
		this.url=url;
		this.oldRank=oldRank;
		this.newRank=newRank;
	}
}

public class RankerApplication implements Application{
	private final Logger log = Logger.getLogger (RankerApplication.class);
	Node node;
	Endpoint endpoint;
	EntityStore store;
	DatabaseUtils dbUtils;
	public double epsilon;
	public double dampFactor;
	public Map<String,Document> resultMap;
	//public Map<String,ArrayList<Inlink>> inlinkInfo;
	public LinkedBlockingQueue<Request> updateRequests;
	
	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public Endpoint getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(Endpoint endpoint) {
		this.endpoint = endpoint;
	}

	public EntityStore getStore() {
		return store;
	}

	public void setStore(EntityStore store) {
		this.store = store;
	}

	public DatabaseUtils getDbUtils() {
		return dbUtils;
	}

	public void setDbUtils(DatabaseUtils dbUtils) {
		this.dbUtils = dbUtils;
	}

	public RankerApplication(Node node,EntityStore store)
	{
		this.node=node;
		this.store=store;
		this.endpoint = this.node.buildEndpoint (this,
				"Pagerank Application");
		this.endpoint.register ();
		this.dbUtils = new DatabaseUtils ();
		this.epsilon=0.4;
		this.dampFactor=0.85;
		resultMap=Collections.synchronizedMap(new HashMap<String,Document>());
		//inlinkInfo=Collections.synchronizedMap(new HashMap<String,ArrayList<Inlink>>());
		updateRequests=new LinkedBlockingQueue<Request>();
	}

	@Override
	public void deliver(Id id, Message message)
	{
		MyMssg mssg=(MyMssg) message;
		//log.info("Got "+mssg.getType() + " message from "+mssg.getFrom()+"using thread "+Thread.currentThread().getId());
		switch(mssg.getType())
		{
			case QUERY:log.info(node.getLocalNodeHandle().getId()+" Received "+ mssg.getType() +" Message containing " + 
mssg.getContent() + " from " + 
mssg.getFrom().toString());
						Document search;
			try {
						search=DatabaseUtils.retrieveCrawledObject(store, Hash.hashSha1(mssg.getContent()));
						MyMssg reply=new MyMssg(node.getLocalNodeHandle(),search,messageType.RESULT);
						reply.setContent(mssg.getContent());
						reply.setTimestamp(mssg.getTimestamp());
						reply.setFrom(node.getLocalNodeHandle());
						log.info(node.getLocalNodeHandle().toString()+" Sent Result to "+ mssg.getFrom().getId().toStringFull());
						NodeManager.getInstance().getQueueStats().canSend();
						try {
							Thread.sleep(5);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						sendMessage(mssg.getFrom().getId(),reply);
						
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			break;
			case RESULT: log.info(node.getLocalNodeHandle().toString()+" Received result "+ mssg.getType());
				if(mssg.getDocContent()!=null)	
			    {
			    	log.info("Message containing " +mssg.getDocContent().getPagerank() + " from " + mssg.getFrom().toString());
			    	resultMap.put(mssg.getContent()+mssg.getTimestamp(), mssg.getDocContent());
			    	//System.out.println(node.getLocalNodeHandle().toString()+" Inserted "+mssg.getContent()+mssg.getTimestamp()+"into hashmap");
			    }
			    else
			    	log.info("Null doc recieved");
			    break;
			case UPDATE:
				/*log.info(node.getLocalNodeHandle().getId()+" Received "+ mssg.getType() +" Message containing " + 
mssg.getContent()+ " from " + mssg.getFrom().toString());*/
				//System.out.println()
				Request req=new Request(mssg.getToUrl(),mssg.getOldRank(),mssg.getNewRank());
			Document doc1;
			try {
				doc1 = DatabaseUtils.retrieveCrawledObject(store, Hash.hashSha1(req.url));
				if(doc1!=null)
				{
					if(!updateRequests.contains(req))
							updateRequests.add(req);
				}
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
				//System.out.println("Update received for "+ mssg.getToUrl());
				
				break;	
		}
	}

	@Override
	public boolean forward(RouteMessage arg0) {
		
		return true;
	}

	@Override
	public void update(NodeHandle arg0, boolean arg1) {
		
	}
	
	public void sendMessage(Id idToSendTo,MyMssg message){
		//log.info("My node id is : " +node.getLocalNodeHandle().getId().toStringFull());
		message.setFrom(node.getLocalNodeHandle());
		endpoint.route(idToSendTo, message, null);
	}
	
	@SuppressWarnings("unchecked")
	/*public void updateRank(Document doc,String url,Double upRank)
	{
		System.out.println("Url to be searched : "+doc.getUrl());
		System.out.println("Inlink to be searched : "+url);
		ArrayList<Inlink> incoming=inlinkInfo.get(doc.getUrl());
		if(incoming!=null)
			System.out.println("Fetched inlink info list for "+doc.getUrl());
		else
			System.out.println("Could not fetch inlink info list for "+doc.getUrl());
		int pos;
		System.out.println("Number of incoming links:"+incoming.size());
		for(int i=0;i<incoming.size();i++)
			System.out.println("Inlink : "+incoming.get(i).url);
		
		pos=incoming.indexOf(url);
		System.out.println("Inlink to be updated is at position :"+pos);
		Inlink templink=new Inlink();
		Inlink oldTemp=incoming.get(pos);
		templink.numOut=oldTemp.numOut;
		templink.url=oldTemp.url;
		templink.rank=upRank;
		incoming.set(pos, templink);
		inlinkInfo.put(doc.getUrl(), incoming);
		ArrayList<String> outgoing=null;
		@SuppressWarnings("unused")
		URL inlink=null;
		URL outlink=null;
		String link=null;
		MyMssg message=null;
		String host=null;
		//long time;
		Document temp;
		double oldrank;
		double newrank;
		double relerr;
		double inRank;
		double sum=0.0;
		int outbound;
		oldrank=doc.getPagerank();
		log.info("Oldrank for "+doc.getUrl()+" is "+oldrank);
		log.info("No. of inlinks"+incoming.size());
		for(int i=0;i<incoming.size();i++)
		{
				inRank=incoming.get(i).rank;
				outbound=incoming.get(i).numOut;
				sum+=inRank/outbound;
		}
		newrank=(1-dampFactor)+dampFactor*sum;
		log.info("Newrank for "+doc.getUrl()+" is "+newrank);
		relerr=Math.abs(oldrank-newrank)/newrank;
		log.info("Relative Error for " +doc.getUrl()+" is "+ relerr );
		if(relerr>epsilon)
		{
			doc.setPagerank(newrank);
			DatabaseUtils.insertCrawledObject(doc, store);
			outgoing=new ArrayList(doc.getOutgoingLinks());
			log.info("No. of outlinks for "+ doc.getUrl()+" is: "+outgoing.size());
			for(int j=0;j<outgoing.size();j++)
			{
				link=outgoing.get(j);
				if(link==null || link=="")
					continue;
				
				try {
					temp=DatabaseUtils.retrieveCrawledObject(store,Hash.hashSha1(link));
				
				if(temp!=null)
				{
					log.info("Update from local for "+ link);
					Request req=new Request(link,doc.getUrl(),newrank);
					updateRequests.add(req);
				}
				else
				{
					try {
						outlink=new URL(link);
					} catch (MalformedURLException e) {
						e.printStackTrace();
					}
					host=outlink.getHost();
					log.info(node.getLocalNodeHandle().getId()+" sending Update for "+ link + " to "+ServerUtils.getIdFromBytes(host)); 
					message=new MyMssg(node.getLocalNodeHandle(),link,doc.getUrl(),newrank,messageType.UPDATE);
					sendMessage(ServerUtils.getIdFromBytes(host),message);
				}
				} catch (NoSuchAlgorithmException e1) {
					e1.printStackTrace();
				}
			}
		}
		else
			log.info("Threshold exceeded");
	}*/
	
	public void updateRank(Document doc,Double oldfactor, Double newfactor)
	{
		System.out.println("Url to be searched : "+doc.getUrl());
		ArrayList<String> outgoing=null;
		@SuppressWarnings("unused")
		URL inlink=null;
		URL outlink=null;
		String link=null;
		MyMssg message=null;
		String host=null;
		//long time;
		Document temp;
		double oldrank;
		double newrank;
		double relerr;
		int outNum;
		oldrank=doc.getPagerank();
		newrank=(1-dampFactor)+dampFactor*(newfactor-oldfactor);
		relerr=Math.abs(oldrank-newrank)/newrank;
		if(relerr>epsilon)
		{
			doc.setPagerank(newrank);
			DatabaseUtils.insertCrawledObject(doc, store);
			outgoing=new ArrayList(doc.getOutgoingLinks());
			outNum=outgoing.size();
			log.info("No. of outlinks for "+ doc.getUrl()+" is: "+outgoing.size());
			for(int j=0;j<outgoing.size();j++)
			{
				link=outgoing.get(j);
				if(link==null || link=="")
					continue;
				
				try {
					temp=DatabaseUtils.retrieveCrawledObject(store,Hash.hashSha1(link));
				
				if(temp!=null)
				{
					log.info("Update from local for "+ link);
					Request req=new Request(link,oldrank/outNum,newrank/outNum);
					if(!updateRequests.contains(req))
						updateRequests.add(req);
				}
				else
				{
					try {
						outlink=new URL(link);
					} catch (MalformedURLException e) {
						e.printStackTrace();
					}
					host=outlink.getHost();
					message=new MyMssg(node.getLocalNodeHandle(),link,oldfactor,newfactor,messageType.UPDATE);
					NodeManager.getInstance().getQueueStats().canSend();
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					sendMessage(ServerUtils.getIdFromBytes(host),message);
				}
				} catch (NoSuchAlgorithmException e1) {
					e1.printStackTrace();
				}
			}
		}
		else
			log.info("Threshold exceeded");
	}
	
	@SuppressWarnings("unchecked")
	public void findRank(Document doc)
	{
		System.out.println("Old Pagerank : "+doc.getPagerank());
		ArrayList<String> incoming=null;
		URL inlink=null;
		String link=null;
		MyMssg message=null;
		String host=null;
		Document temp;
		double newrank;
		double inRank=1.0;
		double sum=0.0;
		int outbound=0;
		incoming=new ArrayList(doc.getIncomingLinks());
		for(int j=0;j<incoming.size();j++)
		{
			link=incoming.get(j);
			if(link==null || link=="")
			{
				continue;
			}
			try {
				temp=DatabaseUtils.retrieveCrawledObject(store,Hash.hashSha1(link));
			
			if(temp!=null)
			{
				inRank=temp.getPagerank();
				outbound=temp.getOutgoingLinks().size();
			}
			
			else
			{
				
					inlink=new URL(link);
				
				host=inlink.getHost();
				message=new MyMssg(node.getLocalNodeHandle(),link,messageType.QUERY);
				message.setTimestamp(System.currentTimeMillis());
				NodeManager.getInstance().getQueueStats().canSend();
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				sendMessage(ServerUtils.getIdFromBytes(host),message);
				while(!resultMap.containsKey(message.getContent()+message.getTimestamp()))
				{
					try {
							Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				temp=resultMap.remove(message.getContent()+message.getTimestamp());
			
				if(temp!=null)
				{
					inRank=temp.getPagerank();
					outbound=temp.getOutgoingLinks().size();
				}
			}
				sum+=inRank/outbound;
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
			 catch (NoSuchAlgorithmException e1) {
				e1.printStackTrace();
			}	
		}
		newrank=(1-dampFactor)+dampFactor*sum;
		doc.setPagerank(newrank);
		DatabaseUtils.insertCrawledObject(doc, store);
	}
	
	@SuppressWarnings("unchecked")
	public void computeRank(Document doc)
	{
		ArrayList<String> incoming=null;
		ArrayList<String> outgoing=null;
		//ArrayList<Inlink> inInfo=new ArrayList<Inlink>();
		//Inlink in=new Inlink();
		URL inlink=null;
		URL outlink=null;
		String link=null;
		MyMssg message=null;
		String host=null;
		//long time;
		Document temp;
		double oldrank,oldfactor;
		double newrank,newfactor;
		double relerr;
		double inRank=1.0;
		double sum=0.0;
		int outbound=0;
		int outNum=0;
		incoming=new ArrayList(doc.getIncomingLinks());
		oldrank=doc.getPagerank();
		//System.out.println("Oldrank for "+doc.getUrl()+" is "+oldrank);
		//System.out.println("No. of inlinks"+incoming.size());
		for(int j=0;j<incoming.size();j++)
		{
			link=incoming.get(j);
			//System.out.println("Incoming link "+link);
			if(link==null || link=="")
			{
				//System.out.println("Link is null");
				continue;
			}
			try {
				temp=DatabaseUtils.retrieveCrawledObject(store,Hash.hashSha1(link));
			/*if(temp==null)
				System.out.println(link+" not found in local");*/
			if(temp!=null)
			{
				inRank=temp.getPagerank();
				outbound=temp.getOutgoingLinks().size();
				//System.out.println("Inrank from local : "+inRank);
				//System.out.println("Outbound from local : "+outbound);
				//System.out.println("Fetched from local " +inRank);
			}
			
			else
			{
					inlink=new URL(link);
				
				host=inlink.getHost();
				//System.out.println("Host to be sent to :" + host);
				message=new MyMssg(node.getLocalNodeHandle(),link,messageType.QUERY);
				message.setTimestamp(System.currentTimeMillis());
				//System.out.println(node.getLocalNodeHandle().getId()+" sent Query to "+ServerUtils.getIdFromBytes(host).toStringFull());
				//time=System.currentTimeMillis();
				//message.setTimestamp(time);
				NodeManager.getInstance().getQueueStats().canSend();
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				sendMessage(ServerUtils.getIdFromBytes(host),message);
				//System.out.println(node.getLocalNodeHandle().getId().toStringFull()+" waiting for results from "+ServerUtils.getIdFromBytes(host).toStringFull());
				//System.out.println(node.getLocalNodeHandle().getId().toStringFull()+"checking hashmap for "+message.getContent()+message.getTimestamp());
				while(!resultMap.containsKey(message.getContent()+message.getTimestamp()))
				{
					try {
						//log.info(Thread.currentThread().getId()+" waiting for results of "+message.getContent()+ " from " + 
//ServerUtils.getIdFromBytes(host));
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//System.out.println(node.getLocalNodeHandle().getId().toStringFull()+" wait for "+ServerUtils.getIdFromBytes(host).toStringFull()+" ends ");
				temp=resultMap.remove(message.getContent()+message.getTimestamp());
				/*if(temp==null)
				{
					System.out.println("Got NULL result from other node");
				}*/
				if(temp!=null)
				{
					inRank=temp.getPagerank();
					outbound=temp.getOutgoingLinks().size();
					//System.out.println("Inrank from other : "+inRank);
					//System.out.println("Outbound from other : "+outbound);
				}
			}
				//in.numOut=outbound;
				//in.url=link;
				//in.rank=inRank;
				//inInfo.add(in);
				sum+=inRank/outbound;
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}catch (NoSuchAlgorithmException e1) {
				e1.printStackTrace();
			}	
		}
		//inlinkInfo.put(doc.getUrl(), inInfo);
		newrank=(1-dampFactor)+dampFactor*sum;
		//System.out.println("Newrank is "+newrank);
		relerr=Math.abs(oldrank-newrank)/newrank;
		//System.out.println("Relative Error is "+ relerr );
		if(relerr>epsilon)
		{
			doc.setPagerank(newrank);
			DatabaseUtils.insertCrawledObject(doc, store);
			outgoing=new ArrayList(doc.getOutgoingLinks());
			outNum=outgoing.size();
			oldfactor=oldrank/outNum;
			newfactor=newrank/outNum;
			//System.out.println("No. of outlinks for "+ doc.getUrl()+" is: "+outgoing.size());
			for(int j=0;j<outgoing.size();j++)
			{
				link=outgoing.get(j);
				if(link==null || link=="")
				{
					System.out.println("Found blank link");
					continue;
				}
				try {
					temp=DatabaseUtils.retrieveCrawledObject(store,Hash.hashSha1(link));
				if(temp!=null)
				{
					//System.out.println("Update added to local for "+ link );
					//Request req=new Request(link,doc.getUrl(),newrank);
					Request req=new Request(link,oldfactor,newfactor);
					if(!updateRequests.contains(req))
						updateRequests.add(req);
				}
				else
				{
					try {
						outlink=new URL(link);
					
					host=outlink.getHost();
					//System.out.println("Sending Update for "+ link + " to "+ServerUtils.getIdFromBytes(host)); 
					//message=new MyMssg(node.getLocalNodeHandle(),link,doc.getUrl(),newrank,messageType.UPDATE);
					message=new MyMssg(node.getLocalNodeHandle(),link,oldfactor,newfactor,messageType.UPDATE);
					NodeManager.getInstance().getQueueStats().canSend();
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					sendMessage(ServerUtils.getIdFromBytes(host),message);
					//System.out.println("Sent update for "+ link + "and its inlink "+doc.getUrl());
					System.out.println("Sent update for "+ link);
					} catch (MalformedURLException e) {
						e.printStackTrace();
					}
				}
				} catch (NoSuchAlgorithmException e1) {
					e1.printStackTrace();
				}
			}
		}
		/*else
			System.out.println("Threshold exceeded");*/
	}
	
	public void start(int maxTime)
	{
		long begin,end;
		long startTime,closeTime;
		startTime=System.currentTimeMillis();
		float totalTime=0;
		float elapsed=0;
		System.out.println("Got into start");
		ArrayList<Document> docs=DatabaseUtils.retrieveAllCrawledObjects(store);
		int totalDocs=docs.size();
		Document doc;
		System.err.println("Number of docs for this node : "+totalDocs);
		for(int i=0;i<totalDocs;i++)
		{
			doc=docs.get(i);
			doc.setPagerank(1.0);
			DatabaseUtils.insertCrawledObject(doc, store);
		}
		for(int i=0;i<totalDocs;i++)
		{
			doc=docs.get(i);
			if(!doc.isDeadLink())
			{
				//System.out.println("Reached Document "+i);	
				//System.out.println("Working for : "+doc.getUrl()+" using Thread "+Thread.currentThread().getId());
				computeRank(doc);
			}
		}
		System.err.println("Completed computing all docs: "+totalDocs);
		
		while(true)
		{
			closeTime=System.currentTimeMillis();
			totalTime=(closeTime-startTime)/1000F;
			if(totalTime>=maxTime)
				break;
				
			//System.out.println("Got in here");
			if(!updateRequests.isEmpty())
			{
				elapsed=0;
				System.out.println("Requests : "+updateRequests.size());
				//System.out.println("Servicing Queued Requests ");
				Request current=updateRequests.remove();
				//System.out.println("Url is"+current.url);
				//System.out.println("Inurl is "+current.inurl);
				//System.out.println(current.pagerank);
				Document doc1;
				try {
					doc1 = DatabaseUtils.retrieveCrawledObject(store, Hash.hashSha1(current.url));
					if(doc1!=null)
					{
						if(!doc1.isDeadLink())
						{
							//updateRank(doc1,current.inurl,current.pagerank);
							updateRank(doc1,current.oldRank,current.newRank);
						}
					}
					else
						System.out.println("Document found null for " +current.url);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				
			}
			else
			{
				begin=System.currentTimeMillis();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				end=System.currentTimeMillis();
				elapsed+=((end-begin)/1000F);
				System.out.println("Time elapsed :"+elapsed);
				if(elapsed>=20)
					break;
			}
		}
		
		for(int i=0;i<totalDocs;i++)
		{
			doc=docs.get(i);
			if(!doc.isDeadLink())
			{
				System.out.println("Document "+(i+1));	
				System.out.println("Url : "+doc.getUrl());
				System.out.println("New Pagerank : "+doc.getPagerank());
			}
			else
			{
				System.out.println("Document "+(i+1));	
				System.out.println("Url : "+doc.getUrl());
				findRank(doc);
				System.out.println("New Pagerank : "+doc.getPagerank());
			}
		}
	}
	
}
