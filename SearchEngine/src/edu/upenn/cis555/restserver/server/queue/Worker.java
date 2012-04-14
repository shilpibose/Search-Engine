package edu.upenn.cis555.restserver.server.queue;

import org.apache.log4j.Logger;

/**
 * Worker class handling a new connection request from a client
 */
public abstract class Worker extends Thread {

	private final Logger log = Logger.getLogger(Worker.class);

	protected EventQueue eventQueue;

	public Worker(EventQueue queue) {
		eventQueue = queue;
	}

	public EventQueue getEventQueue() {
		return eventQueue;
	}

	public abstract Worker clone();
}
