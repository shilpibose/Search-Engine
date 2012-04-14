package edu.upenn.cis555.common;

public class Constants {

	public static final String DB_STORE_NAME = "entityStore";

	public static final String HADOOP_METHOD = "hadoop";

	public static final String DISTRIBUTED_METHOD = "distributed";

	public static final String WORD_LOCAL_FILENAME_PRINT = "wordslocal.txt";

	public static final String WORD_RECEIVED_FILENAME_PRINT = "wordsreceived.txt";

	public static final String WORD_DOCUMENT_HIT_LOCAL_FILENAME_PRINT = "worddocumentslocal.txt";

	public static final String DOCUMENT_LOCAL_WEIGHT_FILENAME_PRINT = "documentweightlocal.txt";

	public static final String DOCUMENT_LOCAL_RECEIVED_WEIGHT_FILENAME_PRINT = "documentweightlocalreceived.txt";

	public static final String WORD_DOCUMENT_LOCAL_WEIGHT_FILENAME_PRINT = "worddocumentweightlocal.txt";

	public static final String DOCUMENT_FILENAME_PRINT = "documents.txt";

	public static final String LEXICON_LOCAL_NOTUPDATED_FILENAME_PRINT = "lexiconlocalnotupdated.txt";

	public static final String FORWARD_INDEX_LOCAL_FILENAME_PRINT = "forwardindex.txt";

	public static final String LEXICON_LOCAL_UPDATED_FILENAME_PRINT = "lexiconlocalupdated.txt";

	public static final String LEXICON_FINAL_FILENAME_PRINT = "./finallexiconinvertedindex.txt";

	public static final String DICTIONARY_PRINT = "./dictionary.txt";

	/** Properties Strings */

	public static final String MAX_NUM_OF_URLS = "maxNumOfURLs";

	public static final String SEED_URLS = "seedUrls";

	public static final String MAX_FILE_SIZE = "maxFileSize";

	public static final String NUM_CRAWLER_THREADS = "numCrawlerThreads";

	public static final String NUM_INDEXER_THREADS = "numIndexThreads";

	public static final String PAGERANK_FILE_DIR = "pageRankFileDir";

	public static final String PAGERANK_FILENAME = "pagerankFileName";

	public static final String BOOTSTRAP_ADDRESS = "bootstrapAddress";

	public static final String BOOTSTRAP_PORT = "bootstrapPort";

	public static final String DB_DIR = "dbDir";

	public static final String CONTROLLER_ADDRESS = "controllerAddress";

	public static final String CONTROLLER_PORT = "controllerPort";

	public static final String REST_SERVER_PORT = "restServerPort";

	public static final String CONVERGENCE_TIME = "convergenceTime";

	public static final String DICTIONARY_FILE = "dictionaryFile";

	/** Internal Constants */

	public static final String SPLIT_EXPR = "([,<.>/?'\":;\\[{\\]}\\+|=\\-_)(*&^%$#@!`~])|(\\s+)";

	public static final int NUM_RESULTS = 30;

	public static final String METHOD_HADOOP = "hadoop";

	public static final String METHOD_DISTRIBUTED = "distributed";
}
