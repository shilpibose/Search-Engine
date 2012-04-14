package edu.upenn.cis555.indexer;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

import edu.upenn.cis555.crawler.distributed.util.ServerUtils;
import edu.upenn.cis555.indexer.IndexMsg;
import edu.upenn.cis555.indexer.LexiconEntryLocal;
import edu.upenn.cis555.indexer.LexiconEntryLocalReceived;
import edu.upenn.cis555.node.NodeManager;
import edu.upenn.cis555.node.utils.Utility;
import edu.upenn.cis555.restserver.service.ServiceManager;

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.RouteMessage;

public class MyApp implements Application {
	private final Logger log = Logger.getLogger (MyApp.class);

	Node node;
	Endpoint endpoint;
	MyStore _myStore;
	int globalCount;
	int documentLocalSendCount;
	int printCount;
	int printDocCount;
	int rcvlexiconCount;
	int rcvDocCount;

	public MyApp (Node node, MyStore myStore) {
		this._myStore = myStore;
		this.node = node;
		System.out.println ("Node:" + node.getId ());
		this.endpoint = node.buildEndpoint (this, "Index Application");
		this.endpoint.register ();
		globalCount = 0;
		documentLocalSendCount = 0;
		printCount = 99;
		printDocCount = 99;
		rcvlexiconCount = 99;
		rcvDocCount = 49;
	}

	public Node getNode () {
		return node;
	}

	/**
	 * This function is used to send the local inverted index to a pastry node.
	 * The node to send to is determined by the word.
	 * 
	 * @param msgType
	 * @param lexiconEntry
	 */
	public void sendLocalIndexMessage (MsgType msgType, String wordstr,
			LexiconEntryLocal lexiconEntryLocal) {
		IndexMsg msg = new IndexMsg (node.getLocalNodeHandle ());
		msg.setMsgType (msgType);
		msg.setLexiconEntryLocal (lexiconEntryLocal);

		msg.setWantResponse (true);
		Id idToSendTo = Utility.getIdFromKey (wordstr);
		// System.out.print("Sending Lexicon entry for word "+wordstr);
		// System.out.print("   "+node.getId()+"sending '"
		// +msg.getMsgType()+" :to "+idToSendTo);
		// System.out.println("Checking queue status...");

		// NodeManager.getInstance ().getQueueStats ().canSend ();
		// System.out.println("Queue is not overflowed..Sending message");
		globalCount = globalCount + 1;
		int numDocs = lexiconEntryLocal.getNdocs ();
		NodeManager.getInstance ().getQueueStats ().canSend ();
		endpoint.route (idToSendTo, msg, null);
		printCount = printCount + 1;
		if (printCount == 100) {
			printCount = 0;
			System.out.println ("Count: " + globalCount
					+ " Send Local Lexicon entry for word: " + wordstr
					+ " having:  " + numDocs + " documents");
			log.info ("Count: " + globalCount
					+ " Send Local Lexicon entry for word: " + wordstr
					+ " having:  " + numDocs + " documents");
		}
	}

	public void sendLocalDocumentWeightMessage (MsgType msgType,
			DocumentLocal documentLocal) {
		DocumentWeightLocalMsg msg = new DocumentWeightLocalMsg (node
				.getLocalNodeHandle ());
		msg.setMsgType (msgType);
		msg.setDocumentLocal (documentLocal);
		String urlstr = documentLocal.getUrl ();
		URL url = null;
		try {
			url = new URL (urlstr);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace ();
		}
		String hostName = url.getHost ();
		// System.out.println("hostname: "+hostName);
		// Id idToSendTo = Utility.getIdFromKey (hostName);
		Id idToSendTo = ServerUtils.getIdFromBytes (hostName);

		// System.out.print("Sending Lexicon entry for word "+wordstr);
		// System.out.print("   "+node.getId()+"sending '"
		// +msg.getMsgType()+" :to "+idToSendTo);
		// System.out.println("Checking queue status...");

		// NodeManager.getInstance ().getQueueStats ().canSend (endpoint);

		// NodeManager.getInstance ().getQueueStats ().canSend ();
		// System.out.println("Queue is not overflowed..Sending message");
		documentLocalSendCount = documentLocalSendCount + 1;
		String docUrl = documentLocal.getUrl ();
		NodeManager.getInstance ().getQueueStats ().canSend ();
		endpoint.route (idToSendTo, msg, null);

		printDocCount = printDocCount + 1;
		if (printDocCount == 100) {
			printDocCount = 0;
			System.out.println ("Send . Document local entry for document: "
					+ docUrl + "Count: " + documentLocalSendCount);
			log.info ("Send . Document local entry for document: " + docUrl
					+ "Count: " + documentLocalSendCount);
			// System.out.println("Count: "+globalCount+"  Lexicon entry for word: "+wordstr+" having:  "+
			// numDocs+" documents");
		}
	}

	public void sendResultReqMessage (String word) {
		WordWeightVectorMsg msg = new WordWeightVectorMsg (node
				.getLocalNodeHandle ());
		msg.setMsgType (MsgType.GET_WORD_WEIGHT_VECTOR);
		msg.setWordSearchTerm (word);
		msg.setWantResponse (true);

		Id idToSendTo = Utility.getIdFromKey (word);
		System.err.println ("Routing for word: " + word + ", with ID: "
				+ idToSendTo.toStringFull ());
		endpoint.route (idToSendTo, msg, null);
	}

	@Override
	public boolean forward (RouteMessage arg0) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void update (rice.p2p.commonapi.NodeHandle arg0, boolean arg1) {
		// TODO Auto-generated method stub
	}

	@Override
	public void deliver (Id id, Message message) {

		MyMsg rcvdMsg = (MyMsg) message;
		MsgType msgType = rcvdMsg.getMsgType ();

		switch (msgType) {
		case PUT_INDEX: {
			IndexMsg indexMsg = (IndexMsg) message;
			LexiconEntryLocal lexiconEntryLocal = indexMsg
					.getLexiconEntryLocal ();
			LexiconEntryLocalReceived lexiconEntryLocalReceived = new LexiconEntryLocalReceived (
					lexiconEntryLocal);
			String wordId = lexiconEntryLocalReceived.getWordId ();
			String wordstr = lexiconEntryLocalReceived.getWord ();
			// System.out.print(node.getId()+" received '"+ msgType+
			// rcvdMsg.hfrom);

			rcvlexiconCount = rcvlexiconCount + 1;
			if (rcvlexiconCount == 100) {
				rcvlexiconCount = 0;
				System.out.println (" .Received Lexicon entry copy for Word  :"
						+ wordstr);
				log
						.info (" .Received Lexicon entry copy for Word  :"
								+ wordstr);
			}
			_myStore.putLexiconEntryLocalReceived (lexiconEntryLocalReceived);
			break;
		}

		case GET_WORD_WEIGHT_VECTOR: {
			WordWeightVectorMsg wordWeightVectorMsg = (WordWeightVectorMsg) message;
			String wordSearchTerm = wordWeightVectorMsg.getWordSearchTerm ();
			System.out.println ("Word to search for:" + wordSearchTerm);
			String wordId = new String (Utility.hash (wordSearchTerm));
			System.out.println ("Word Id :" + wordId);
			System.err.println ("Node ID: " + node.getLocalNodeHandle ());
			LexiconEntry lexiconEntry = _myStore.getLexiconEntry (wordId);
			// if(lexiconEntry ==
			// null)System.out.println("Lexicon Entry is null");
			WordWeightVectorMsg replyWordWeightVectorMsg = new WordWeightVectorMsg (
					node.getLocalNodeHandle ());
			replyWordWeightVectorMsg
					.setMsgType (MsgType.RESULT_WORD_WEIGHT_VECTOR);
			replyWordWeightVectorMsg.setWordSearchTerm (wordSearchTerm);
			replyWordWeightVectorMsg.setLexiconEntry (lexiconEntry);
			System.err.println ("Recived req for " + wordSearchTerm
					+ ", returning " + lexiconEntry);
			endpoint.route (null, replyWordWeightVectorMsg, rcvdMsg.hfrom);
			break;
		}

		case RESULT_WORD_WEIGHT_VECTOR: {
			WordWeightVectorMsg wordWeightVectorMsg = (WordWeightVectorMsg) message;
			String word = wordWeightVectorMsg.getWordSearchTerm ();
			LexiconEntry lexEntry = wordWeightVectorMsg.getLexiconEntry ();
			System.err.println ("Received response for " + word + " with "
					+ lexEntry);
			ServiceManager.getInstance ().deliverResult (word, lexEntry);
			break;
		}

		case DOCUMENT_WEIGHT_LOCAL: {
			DocumentWeightLocalMsg documentWeightLocalMsg = (DocumentWeightLocalMsg) message;
			if (documentWeightLocalMsg == null)
				break;
			DocumentLocal documentLocal = documentWeightLocalMsg
					.getDocumentLocal ();
			if (documentLocal == null)
				break;
			DocumentLocalReceived documentLocalReceived = new DocumentLocalReceived (
					documentLocal);
			// System.out.print(node.getId()+" received '"+ msgType+
			// rcvdMsg.hfrom);
			String url = documentLocalReceived.getUrl ();

			System.out.println ("Received message for document local");

			_myStore.putDocumentLocalReceived (documentLocalReceived);
			rcvDocCount = rcvDocCount + 1;
			if (rcvDocCount == 50) {
				rcvDocCount = 0;
				log
						.info (" .Received Document weight local entry copy for document  :"
								+ url);
				System.out
						.println (" Stored Document weight local entry copy for document  :"
								+ url + "in dtabase");
			}

			break;

		}
		}
	}

	public String toString () {
		return "MyApp " + endpoint.getId ();
	}
}
