
package edu.upenn.cis555.indexer;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class MyDbEnv {

	private Environment myEnv;
	private EntityStore store;
	public MyDbEnv(){}
	
public  void setup(Environment env){
	myEnv = env;	
	StoreConfig storeConfig = new StoreConfig();
	storeConfig.setReadOnly(false);
	storeConfig.setTransactional(true);
	storeConfig.setAllowCreate(true);
	store = new EntityStore(myEnv,"EntityStore",storeConfig);
 }


public  EntityStore getEntityStore(){
	return store;
  }

public  Environment getEnv(){
	return myEnv;
}

public  void close(){
	if(store != null){
		try{
			store.close();
		}catch(DatabaseException dbe){
			System.err.println("Error closing store: " +
			                   dbe.toString());
			System.exit(-1);
		}
	}
	if(myEnv != null){
			try{
				myEnv.close();
			}catch (DatabaseException dbe) {
				System.err.println("Error closing environment"+
						 dbe.toString());// TODO: handle exception
			}
		}
	}
	
}
