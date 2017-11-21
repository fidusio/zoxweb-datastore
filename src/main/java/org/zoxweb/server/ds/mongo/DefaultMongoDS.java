package org.zoxweb.server.ds.mongo;

public class DefaultMongoDS 
{
	
	private MongoDataStore dataStore = null;
	
	public static final DefaultMongoDS SIGLETON = new DefaultMongoDS();
	
	private DefaultMongoDS()
	{
	}
	
	
	public MongoDataStore getDataStore()
	{
		return dataStore;
	}
	
	public void setDataStore(MongoDataStore dataStore)
	{
		this.dataStore = dataStore;
	}
}
