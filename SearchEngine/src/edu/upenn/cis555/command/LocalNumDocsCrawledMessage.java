package edu.upenn.cis555.command;

public class LocalNumDocsCrawledMessage extends Command {

	private long numDocsCrawled;

	public LocalNumDocsCrawledMessage (long numDocs) {
		super (CommandType.LOCAL_NUM_DOCS_CRAWLED);
		numDocsCrawled = numDocs;
	}

	public long getNumDocsCrawled () {
		return numDocsCrawled;
	}
}
