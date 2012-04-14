package edu.upenn.cis555.crawler.distributed;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Node;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.routing.RouteSet;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

 /**
  * A simple class for creating multiple Pastry nodes in the same
  * ring
  *
  *
  * @author Nick Taylor
  *
  */
 public class NodeFactory {
        Environment env;
        NodeIdFactory nidFactory;
        SocketPastryNodeFactory factory;
        InetSocketAddress bootSocketAddress;
        NodeHandle bootHandle;
        int createdCount = 0;
        int port;

        NodeFactory(int port) {
                this(new Environment(), port);
        }

        NodeFactory(int port, InetSocketAddress bootSocketAddress) {
                this(port);
                this.bootSocketAddress=bootSocketAddress;
                this.env.getParameters().setString("nat_search_policy","never");
                this.env.getParameters().setString("pastry_socket_writer_max_queue_length","3000");
                bootHandle = factory.getNodeHandle(bootSocketAddress);
        }

      
		NodeFactory(Environment env, int port) {
                this.env = env;
                this.port = port;
                try {
                        nidFactory = new RandomNodeIdFactory(env);
                        factory = new SocketPastryNodeFactory(nidFactory, port, env);
                } catch (java.io.IOException ioe) {
                throw new RuntimeException(ioe.getMessage(), ioe);
                        
                }

        }
        /*NodeHandle getBootNodeHandle(){
                return this.bootHandle;
        }*/
        public PastryNode getNode() {
           

                        PastryNode node =  factory.newNode();
                        node.boot(this.bootSocketAddress);
                        while (! node.isReady()) {
                                try {
                                        Thread.sleep(100);
                                } catch (InterruptedException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                }
                        }
                        synchronized (this) {
                                ++createdCount;
                        }
                        return node;
                  
        }

        public void shutdownNode(Node n) {
                ((PastryNode) n).destroy();

        }

        public NodeHandle getBootHandle() {
			return bootHandle;
		}

		public Id getIdFromBytes(byte[] material) {
                try {
                		
                        MessageDigest md= MessageDigest.getInstance ("SHA1");
                        md.update (material);
                        byte shaDigest[]=md.digest ();
                        PastryIdFactory pf= new PastryIdFactory(env);
                        return pf.buildId(shaDigest);
                        } catch (NoSuchAlgorithmException e) {
                        // TODO Auto-generated catch block
                        System.out.println (e.getMessage ());
                }
                return null;
        }

        public NodeHandle getHandle(InetSocketAddress address)
        {
                return this.factory.getNodeHandle(address);
        }

        public Environment getEnv() {
                return env;
        }

		public void setBootHandle(NodeHandle nextHandle) {
			// TODO Auto-generated method stub
			bootHandle = nextHandle;
		}
	
 }