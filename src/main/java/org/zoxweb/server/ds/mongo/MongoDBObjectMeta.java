package org.zoxweb.server.ds.mongo;

import org.zoxweb.shared.util.NVConfigEntity;

import com.mongodb.BasicDBObject;

public class MongoDBObjectMeta 
{
	private BasicDBObject content;
	private NVConfigEntity nvce;
	
	public MongoDBObjectMeta(NVConfigEntity nvce)
	{
		this.nvce = nvce;
	}
	
	public BasicDBObject getContent()
	{
		return content;
	}

	public void setContent(BasicDBObject content)
	{
		this.content = content;
	}

	public NVConfigEntity getNVConfigEntity()
	{
		return nvce;
	}

}