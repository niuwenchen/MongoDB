package com.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;

import org.bson.Document;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ConnectToMongoDB {
		
//		public static final String dbIP="192.168.1.5";

	   	private static String dbIP="192.168.222.128";  //10.9.201.190 192.168.222.128
	   	public static  int dbPort=27017;
	   	// 本地机：Jack  Jack
	   	// 远程机 虚拟机：jack  jack
	   	public static final String userName="jack";
	   	public static final String password="jack";
//		public static final String userName="Jack";
//	   	public static final String password="Jack";
		public static final String dbName="test";
		public static final String dbTest="test";//
		public static final String dbMysqlInfo="slow_query_mysql_info";
		public static final String dbMongodbInfo="slow_query_mongodb_info";
		public static final String dbMonitorSendInfo = "monitor_send_info";
		public static final String dbMonitorFailLog = "business_fail_log";
	    private static Mongo mongo;  
	    private static MongoCollection<Document> collection;
	    private static MongoCredential credential;
	    private static MongoClient mongoClient;
	    private static MongoDatabase db;
	    private static String mongo_client_addr_str;
	    private static String[] mongo_client_addr;
	    
	    @SuppressWarnings("static-access")
		public ConnectToMongoDB(String configFile){
	    	Properties prop = new Properties();
			InputStream input = null;
			try {
				if(configFile != null){
					input = new FileInputStream(configFile);
					prop.load(input);
					mongo_client_addr_str = prop.getProperty("mongo_client_addr");
					if(!mongo_client_addr_str.isEmpty()){
						mongo_client_addr=mongo_client_addr_str.split(",");
						if(mongo_client_addr.length>1){
							String tmp=mongo_client_addr[1];//取主mongodb
							String[] ip_port=tmp.split(":");
							if(ip_port.length>1){
								this.dbIP=ip_port[0];
								this.dbPort=Integer.parseInt(ip_port[1]);
							}
						}
					}	
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {

				if (input != null) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
	    }
	    
	    
	    public ConnectToMongoDB(String dbIp,int dbPort){
	    	this.dbIP=dbIp;
	    	this.dbPort=dbPort;
	    }
		public MongoCollection<Document> getCollection () throws UnknownHostException{  
		    try{
		    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
		    	mongoClient=new MongoClient(new ServerAddress(dbIP+":"+dbPort),Arrays.asList(credential));
		    	db=mongoClient.getDatabase(dbName);
		    	collection=db.getCollection(dbMonitorSendInfo);
		    }catch(Exception e) {  
	            e.printStackTrace();  
		    }
			return collection;  
	    }
//		public MongoCollection<Document> getCollection (String collectName) throws UnknownHostException{  
//		    try{
//		    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
//		    	mongoClient=new MongoClient(new ServerAddress(dbIP+":"+dbPort),Arrays.asList(credential));
//		    	db=mongoClient.getDatabase(dbName);
//		    	collection=db.getCollection(collectName); 
//		    }catch(Exception e) {  
//	            e.printStackTrace();  
//		    }
//			return collection;  
//	    }
		public MongoCollection<Document> getReadCollection (String collectName) throws UnknownHostException{  
			try{
		    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
		    	mongoClient=new MongoClient(new ServerAddress(dbIP+":"+dbPort),Arrays.asList(credential));
		    	mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
		    	db=mongoClient.getDatabase(dbName);
		    	collection=db.getCollection(collectName); 
		    }catch(Exception e) {  
	            e.printStackTrace();  
		    }
			return collection;  
	    }
		public MongoCollection<Document> getCollection (String collectName) throws UnknownHostException{  
			try{
		    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
		    	mongoClient=new MongoClient(new ServerAddress(dbIP+":"+dbPort),Arrays.asList(credential));
		    	db=mongoClient.getDatabase(dbName);
		    	collection=db.getCollection(collectName); 
		    }catch(Exception e) {  
	            e.printStackTrace();  
		    }
			return collection;  
	    }
		public MongoCollection<Document> getCollection (String collectDB,String collectName) throws UnknownHostException{  
			try{
		    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
		    	mongoClient=new MongoClient(new ServerAddress(dbIP+":"+dbPort),Arrays.asList(credential));
		    	db=mongoClient.getDatabase(collectDB);
		    	collection=db.getCollection(collectName);  
		    }catch(Exception e) {  
	            e.printStackTrace();  
		    }
			return collection;  
	    }
		
		public MongoCollection<Document> getCollection (String dbIP,String collectDB,String collectName) throws UnknownHostException{  
			try{
		    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
		    	mongoClient=new MongoClient(new ServerAddress(dbIP+":"+dbPort),Arrays.asList(credential));
		    	db=mongoClient.getDatabase(collectDB);
		    	collection=db.getCollection(collectName);  
		    }catch(Exception e) {  
	            e.printStackTrace();  
		    }
			return collection;  
	    }
		
		public MongoCollection<Document> getCollection (String dbIP,int port,String collectDB,String collectName) throws UnknownHostException{  
			
			try{
		    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
		    	mongoClient=new MongoClient(new ServerAddress(dbIP+":"+port),Arrays.asList(credential));
		    	db=mongoClient.getDatabase(collectDB);
		    	collection=db.getCollection(collectName);  
		    }catch(Exception e) {  
	            e.printStackTrace();  
		    } 
			return collection;  
	    }
		/**
	     * 修改
	     */
		public void destory() {
			      if (mongoClient != null)
			    	  mongoClient.close();
			      mongoClient = null;
			      db = null;
			      credential=null;
			      collection = null;
			      System.gc();
			  }
}

