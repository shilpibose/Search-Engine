package edu.upenn.cis555.node;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import rice.p2p.commonapi.Node;
import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.IPNodeIdFactory;

/**
 * NodeFactory class which created a node using a given port number
 */
public class NodeFactory {

	private final Logger log = Logger.getLogger (NodeFactory.class);

	private Environment env;

	private NodeIdFactory nidFactory;

	private SocketPastryNodeFactory factory;

	private NodeHandle bootHandle;

	int port;

	/**
	 * Initialize the environment with port number
	 * 
	 * @param port
	 * @throws UnknownHostException
	 */
	private NodeFactory (int port) throws UnknownHostException {
		this (new Environment (), port);
	}

	public NodeFactory (int port, InetSocketAddress bootPort)
			throws UnknownHostException {
		this (port);
		bootHandle = factory.getNodeHandle (bootPort);
		System.err.println (bootHandle);
	}

	private NodeFactory (Environment env, int port) throws UnknownHostException {
		this.env = env;
		this.port = port;
		nidFactory = new IPNodeIdFactory (InetAddress.getLocalHost (), port,
				env);
		try {
			factory = new SocketPastryNodeFactory (nidFactory, port, env);
			this.env.getParameters ().setString ("nat_search_policy", "never");
			this.env.getParameters ().setString (
					"pastry_socket_writer_max_queue_length", "10000");
		} catch (java.io.IOException ioe) {
			throw new RuntimeException (ioe.getMessage (), ioe);
		}
	}

	public PastryNode getNode () {
		try {
			PastryNode node = factory.newNode (bootHandle);
			while (!node.isReady ()) {
				Thread.sleep (100);
			}
			return node;
		} catch (Exception e) {
			throw new RuntimeException (e.getMessage (), e);
		}
	}

	public void shutdownNode (Node n) {
		((PastryNode) n).destroy ();
	}
}
