package com.jackniu;

import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/*@Author  JackNiu
*/
public class MongoDBJDBC {


	public static void main(String[] args) {
		
		try{
		//连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址  
        //ServerAddress()两个参数分别为 服务器地址 和 端口  
			ServerAddress serverAddress= new ServerAddress("127.0.0.1", 27017);
			MongoCredential credential = MongoCredential.createScramSha1Credential("Jack", "test", "Jack".toCharArray());
			ArrayList<MongoCredential> credentials= new ArrayList<MongoCredential>();
			credentials.add(credential);
			MongoClient client = new MongoClient(serverAddress, credentials);
			
			MongoDatabase database = client.getDatabase("test");
//			System.out.println("Connect to database successfully");  
//			
//			System.out.println(database.getCollection("jacktestdb").count());
			
			 MongoCollection<Document> collection = database.getCollection("jack1");
//			 Document document  = new Document("title","mongo").
//					 append("username", "Jack").
//					 append("age", "23");
//			 collection.insertOne(document);
//			
			 
			 collection.updateMany(Filters.eq("age", "23"), new Document("$set",new  Document("age","20")));
			 collection.deleteOne(Filters.eq("age", "20"));
			 
			 FindIterable<Document> iterator = collection.find();
			 MongoCursor<Document> cursor = iterator.iterator();
			 while(cursor.hasNext())
			 {
				 System.out.println(cursor.next());
			 }
			 
	      }catch(Exception e){
	        System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	     }

	}

}
