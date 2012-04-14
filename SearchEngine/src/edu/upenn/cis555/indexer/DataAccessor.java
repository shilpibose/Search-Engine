package edu.upenn.cis555.indexer;

import org.apache.log4j.Logger;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

import edu.upenn.cis555.crawler.bean.Document;



public class DataAccessor {
	private final Logger log = Logger.getLogger (DataAccessor.class);
	
	PrimaryIndex<String,Document> documentIndex;
	
	PrimaryIndex<String,DocumentLocal> documentLocalIndex;
	
	PrimaryIndex<String,Word> wordIndex;
	PrimaryIndex<String,WordReceived> wordReceivedIndex;
	PrimaryIndex<Integer,NodeDocCount> idIndex;;
	
	
	PrimaryIndex<Integer,WordDocumentHit> wordDocumentHitIndex;
	PrimaryIndex<Integer,WordDocumentWeight> wordDocumentWeightIndex;
	
	
	PrimaryIndex<String,LexiconEntry> lexiconEntryIndex;
	PrimaryIndex<String,LexiconEntryLocal> lexiconEntryLocalIndex;
	PrimaryIndex<Integer,LexiconEntryLocalReceived> lexiconEntryLocalReceivedIndex;
	
	PrimaryIndex<Integer,DocumentLocalReceived> documentLocalReceivedIndex;
	
	
	PrimaryIndex<String,ForwardIndexEntry> documentForwardIndexEntryIndex;
	
	
	SecondaryIndex<String,Integer,WordDocumentHit> wordFkIndex;
	SecondaryIndex<String,Integer,LexiconEntryLocalReceived> wordFkLexiconEntryLocalReceivedIndex;
	
	SecondaryIndex<String,Integer,DocumentLocalReceived> documentFkDocumentLocalReceivedIndex;
	
	
	
	
	SecondaryIndex<String,Integer,WordDocumentHit> documentFkIndex;
	
	SecondaryIndex<String,Integer,WordDocumentWeight> documentFkWeightIndex;
	
public DataAccessor(EntityStore store) {
	documentIndex = store.getPrimaryIndex(String.class,Document.class);
	
	documentLocalIndex = store.getPrimaryIndex(String.class,DocumentLocal.class);
	
	
	wordIndex = store.getPrimaryIndex(String.class,Word.class);
	wordReceivedIndex = store.getPrimaryIndex(String.class,WordReceived.class);
	
	idIndex = store.getPrimaryIndex(Integer.class,NodeDocCount.class);
	wordDocumentHitIndex = store.getPrimaryIndex(Integer.class,WordDocumentHit.class);
	wordDocumentWeightIndex = store.getPrimaryIndex(Integer.class,WordDocumentWeight.class);
	
	
	lexiconEntryIndex = store.getPrimaryIndex(String.class,LexiconEntry.class);
	lexiconEntryLocalIndex = store.getPrimaryIndex(String.class,LexiconEntryLocal.class);
	
	lexiconEntryLocalReceivedIndex = store.getPrimaryIndex(Integer.class,LexiconEntryLocalReceived.class);
	
	
	documentLocalReceivedIndex = store.getPrimaryIndex(Integer.class,DocumentLocalReceived.class);
	
	documentForwardIndexEntryIndex = store.getPrimaryIndex(String.class,ForwardIndexEntry.class);
	
	
	
	wordFkIndex = store.getSecondaryIndex(wordDocumentHitIndex,String.class,"wordFk");
	documentFkIndex = store.getSecondaryIndex(wordDocumentHitIndex,String.class,"documentFk");

	documentFkWeightIndex = store.getSecondaryIndex(wordDocumentWeightIndex,String.class,"documentFk");

	
	wordFkLexiconEntryLocalReceivedIndex = store.getSecondaryIndex(lexiconEntryLocalReceivedIndex,String.class,"wordId");
	
	documentFkDocumentLocalReceivedIndex = store.getSecondaryIndex(documentLocalReceivedIndex,String.class,"documentId");
		
	}
}
