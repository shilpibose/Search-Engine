package edu.upenn.cis555.indexer;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;

public class MyMsg implements Message {

	NodeHandle hfrom;
	MsgType msgType;
	int messageId;
	String key;
	String value;
	Id from;
	Id to;
	boolean wantResponse = false;
	String content;

	public MyMsg (Id from, Id to) {
		this.from = from;
		this.to = to;
	}

	public MyMsg (NodeHandle hfrom) {
		this.hfrom = hfrom;
	}

	public String getContent () {
		return content;
	}

	public void setContent (String content) {
		this.content = content;
	}

	public String getKey () {
		return key;
	}

	public void setKey (String key) {
		this.key = key;
	}

	public String getValue () {
		return value;
	}

	public void setValue (String value) {
		this.value = value;
	}

	public int getMessageId () {
		return messageId;
	}

	public void setMessageId (int messageId) {
		this.messageId = messageId;
	}

	public boolean isWantResponse () {
		return wantResponse;
	}

	public void setWantResponse (boolean wantResponse) {
		this.wantResponse = wantResponse;
	}

	public MsgType getMsgType () {
		return msgType;
	}

	public void setMsgType (MsgType msgType) {
		this.msgType = msgType;
	}

	/**
	 * Called when we receive a message.
	 */

	@Override
	public int getPriority () {
		// TODO Auto-generated method stub
		return Message.LOW_PRIORITY;
	}

	public String toString () {
		return "MyMsg from " + from + " to " + to;
	}

	public NodeHandle getHfrom () {
		return hfrom;
	}

	public void setHfrom (NodeHandle hfrom) {
		this.hfrom = hfrom;
	}
}
