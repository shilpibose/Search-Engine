package edu.upenn.cis555.indexer;

import java.io.IOException;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

public class SAXHandler extends DefaultHandler {
 private int _level;
 Attributes _attributes;
 String _qName;
 int _state ;
 Indexer indexer;
 StringBuffer sb;
 String value;


SAXHandler(){
	
 }
public Indexer getIndexer() {
	return indexer;
}

public void setIndexer(Indexer indexer) {
	this.indexer = indexer;
}




@Override
	public void processingInstruction(String target, String data)
			throws SAXException {
		// TODO Auto-generated method stub
		super.processingInstruction(target, data);
	}
	
	@Override
	public void startDocument() throws SAXException {
//		System.out.println("Start Document");
		}

	
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
	//	System.out.println("StartElement:"+qName+":");
	    
		indexer.startElementHandler(_qName,_attributes); // characters in between them
			
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		
		
		String value = new String(ch,start,length);
		//System.out.println("Chracters:"+value+":");
		indexer.characterHandler(value);
			}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		 indexer.endElementHandler(qName);
		 //System.out.println("EndElement:"+qName+":");
		}


	
	@Override
	public void endDocument() throws SAXException {
        indexer.endDocumentHandler();
		//   	System.out.println("End Document");
	}

	
}
