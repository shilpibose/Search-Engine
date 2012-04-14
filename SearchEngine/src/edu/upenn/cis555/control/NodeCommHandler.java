package edu.upenn.cis555.control;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.upenn.cis555.command.Command;
import edu.upenn.cis555.command.CommandType;
import edu.upenn.cis555.command.GlobalNumDocsCrawledMessage;
import edu.upenn.cis555.common.ClientSocket;

public class NodeCommHandler extends Thread {

	private final Logger log = Logger.getLogger (NodeCommHandler.class);

	protected ClientSocket clientSocket;

	protected List<Command> commandQueue = new ArrayList<Command> ();

	public NodeCommHandler (ClientSocket socket) {
		clientSocket = socket;
	}

	public void enqueue (Command command) {
		synchronized (commandQueue) {
			commandQueue.add (command);
			commandQueue.notify ();
		}
	}

	protected Command dequeue () throws InterruptedException {
		synchronized (commandQueue) {
			if (commandQueue.size () == 0) {
				commandQueue.wait ();
			}
			Command command = commandQueue.get (0);
			commandQueue.remove (0);
			return command;
		}
	}

	public String receivedCommand (String line) {
		String[] components = line.split ("[ \t]+");
		return components[0];
	}

	public int receivedCommandArg (String line) {
		String[] components = line.split ("[ \t]+");
		int arg = Integer.parseInt (components[1]);
		return arg;
	}

	public void run () {
		try {
			String line = clientSocket.readLine ();
			if (receivedCommand (line).equals (CommandType.NODE_UP.toString ())) {
				System.err.println (this.getName ()
						+ " Updating Node Job Status");
				Controller.getInstance ().updateJobStatus ();
			}

			Command command = null;
			do {
				try {
					command = dequeue ();
				} catch (InterruptedException e) {
					e.printStackTrace ();
				}

				CommandType type = command.getCommandType ();
				if (type == CommandType.CRAWL_INDEX_START) {
					clientSocket.writeLine (type.toString ());

				} else if (type == CommandType.CRAWL_INDEX_END) {
					clientSocket.writeLine (type.toString ());
					line = clientSocket.readLine ();
					if (receivedCommand (line).equals (
							CommandType.LOCAL_NUM_DOCS_CRAWLED.toString ())) {
						Controller.getInstance ().updateCrawlStats (
								receivedCommandArg (line));
						Controller.getInstance ().updateJobStatus ();
					}

				} else if (type == CommandType.GLOBAL_NUM_DOCS_CRAWLED) {
					GlobalNumDocsCrawledMessage msg = (GlobalNumDocsCrawledMessage) command;
					clientSocket.writeLine (type.toString () + " "
							+ msg.getNumDocsCrawled ());

				} else if (type == CommandType.GLOBAL_INDEX_START) {
					clientSocket.writeLine (type.toString ());
					line = clientSocket.readLine ();
					if (receivedCommand (line).equals (
							CommandType.GLOBAL_INDEX_COMPLETE.toString ())) {
						Controller.getInstance ().updateJobStatus ();
					}

				} else if (type == CommandType.DOCUMENT_WEIGHT_CALC_START) {
					clientSocket.writeLine (type.toString ());
					line = clientSocket.readLine ();
					if (receivedCommand (line).equals (
							CommandType.DOCUMENT_WEIGHT_CALC_COMPLETE
									.toString ())) {
						Controller.getInstance ().updateJobStatus ();
					}

				} else if (type == CommandType.DISTRIBUTED_PAGE_RANK_START) {
					clientSocket.writeLine (type.toString ());
					line = clientSocket.readLine ();
					if (receivedCommand (line).equals (
							CommandType.DISTRIBUTED_PAGE_RANK_COMPLETE
									.toString ())) {
						Controller.getInstance ().updateJobStatus ();
					}

				} else if (type == CommandType.OPEN_REST_SERVER) {
					clientSocket.writeLine (type.toString ());

				} else if (type == CommandType.CLOSE_REST_SERVER) {
					clientSocket.writeLine (type.toString ());
					return;
				}

			} while (true);
		} catch (IOException e) {
			e.printStackTrace ();
		}
	}
}
