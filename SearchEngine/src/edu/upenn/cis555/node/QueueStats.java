package edu.upenn.cis555.node;

import java.util.Collection;

import org.mpisws.p2p.transport.priority.PriorityTransportLayer;

import rice.pastry.PastryNode;
import rice.pastry.socket.SocketPastryNodeFactory;

public class QueueStats extends Thread {

	private static int QUEUE_LENGTH_LIMIT = 3000;

	protected PastryNode node;

	protected boolean okToSend = true;

	public QueueStats (PastryNode node) {
		this.node = node;
	}

	public void run () {
		PriorityTransportLayer priority = (PriorityTransportLayer) node
				.getVars ().get (SocketPastryNodeFactory.PRIORITY_TL);

		while (true) {
			Collection<org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress> ids = priority
					.nodesWithPendingMessages ();

			boolean okToSendLocal = true;
			for (org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress id : ids) {
				if (priority.queueLength (id) > QUEUE_LENGTH_LIMIT) {
					okToSendLocal = false;
					break;
				}
			}

			if (okToSendLocal) {
				synchronized (node) {
					okToSend = true;
					//System.err.println ("Queue has got space, notifying all");
					node.notifyAll ();
				}
			} else {
				System.err.println ("More messages in queue, blocking send");
				okToSend = false;
			}

			try {
				Thread.sleep (100);
			} catch (InterruptedException e) {
				e.printStackTrace ();
			}
		}
	}

	public void canSend () {
		while (!okToSend) {
			synchronized (node) {
				try {
					System.err.println ("Not OK to send, blocking it");
					node.wait ();
					System.err.println ("Now OK to send, unblocking it");
				} catch (InterruptedException e) {
					e.printStackTrace ();
				}
			}
		}
	}
}
