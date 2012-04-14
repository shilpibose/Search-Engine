package edu.upenn.cis555.command;

public class GlobalNumDocsCrawledMessage extends Command {

	private long numDocsCrawled;

	public GlobalNumDocsCrawledMessage (long numDocs) {
		super (CommandType.GLOBAL_NUM_DOCS_CRAWLED);
		numDocsCrawled = numDocs;
	}

	public long getNumDocsCrawled () {
		return numDocsCrawled;
	}
}
