package edu.upenn.cis555.node.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class NodeUtils {

	private static final Logger log = Logger.getLogger (NodeUtils.class);

	private static NodeUtils singleInstance;

	protected Properties properties = new Properties ();

	public NodeUtils (String propFile) throws FileNotFoundException,
			IOException {
		log.info ("Loading properties file: " + propFile);

		FileInputStream stream = new FileInputStream (propFile);
		BufferedReader reader = new BufferedReader (new FileReader (propFile));
		System.err.println (reader.readLine ());

		log.info ("FileInputStream: " + stream);

		properties.load (stream);
		initializeLog ();
	}

	public String getValue (String key) {
		return properties.getProperty (key).trim ();
	}

	public void initializeLog () {
		Properties logProperties = new Properties ();
		try {
			logProperties.load (new FileInputStream ("conf/log4j.properties"));
			PropertyConfigurator.configure (logProperties);
			System.err.println ("Logging initialized.");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace ();
		} catch (IOException e1) {
			e1.printStackTrace ();
		}
	}

	public synchronized void infoLog (String message) {
		log.info (message);
	}

	public synchronized void debugLog (String message) {
		log.debug (message);
	}

	public synchronized void errorLog (String message) {
		log.error (message);
	}

	public static NodeUtils getInstance () {
		return singleInstance;
	}

	public static void Instantiate (String propFile)
			throws FileNotFoundException, IOException {
		singleInstance = new NodeUtils (propFile);
	}
}
