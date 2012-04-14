package edu.upenn.cis555.restserver.server.queue;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Class representing the Event Queue of HTTP Connection requests
 */
public class EventQueue {

	private static final Logger log = Logger.getLogger(EventQueue.class);

	protected List<Socket> messageQueue = new ArrayList<Socket>();

	/**
	 * Enqueue a new request from a client
	 */
	public void enqueue(Socket clientSocket) {
		synchronized (messageQueue) {
			messageQueue.add(clientSocket);
			log.info("Enqueued request to the message queue");
			log.info("Notifying a thread of a new request");
			messageQueue.notify();
			log.info("Notification successful");
		}
	}

	/**
	 * Dequeue a request from the front of the Queue. If empty, the calling
	 * thread waits on the Queue object
	 */
	public Socket dequeue() {
		synchronized (messageQueue) {
			while (messageQueue.size() == 0) {
				try {
					messageQueue.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			log.info("Dequeuing a connection request");
			return messageQueue.remove(0);
		}
	}
}
