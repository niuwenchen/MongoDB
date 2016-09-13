package com.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.changhong.bigdata.bdlog.LoggerString;
import com.changhong.bslog.Clogger;

import com.jackniu.LogToLocalBolt;
import com.mongodb.Mongo;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import net.sf.json.JSONObject;

public class TestMongo {
	private MongoCollection<Document> mongodbCollection;
	private static final Logger LOG = Logger.getLogger(TestMongo.class);
	 private ConnectToMongoDB connectMg;
	 StringBuffer  sb;
	private static final String[] mongodbkeys =new String[]{"LogType","DateTimeStr","Product","System","File","LineNumber",
				"Errno","Mac","DevType","SN","Ip","LogId","Uri","TimeUsed","Info"};

	public static  void main(String[] args) 
	{
		new TestMongo().prepare();
	}
	
	public  void prepare()
	{
		//本机  ： 127.0.0.1
		// 虚拟机 ： 192.168.222.128
		this.connectMg= new ConnectToMongoDB("192.168.222.128", 27017);
		try {
			//jacktestdb
			this.mongodbCollection = this.connectMg.getCollection("people");
			
			System.out.println("输出测试");
			FindIterable<Document>  iter=this.mongodbCollection.find();
			MongoCursor<Document>  cursor = iter.iterator();
			while(cursor.hasNext())
			{
				System.out.println(cursor.next());
			}
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&7");
//		Map<String,Object> map = new HashMap<String,Object>();
//		map.put("1", "Jack");
//		map.put("2", "Niu");
//		
//		this.insertMongoDataToMmongo(map);
		
//		try{
//			this.mongodbCollection = this.connectMg.getCollection("jacktestdb");
//			
//			File file = new File("/kafkadata");
//		
//				FileReader  reader= new FileReader(file);
//				BufferedReader br = new BufferedReader(reader);
//				
//				sb = new StringBuffer();
//				String line=br.readLine();
//				while(line != null)
//				{
//					sb.append(line);
//					line=br.readLine();
//				}
//				
//				Map<String,Object> mongodbmap = new HashMap<String,Object>();
//				Map<String,String> mongodblog=JSONObject.fromObject(sb.toString());
//				for(String str:mongodblog.keySet())
//				{
//					String tempLogContent = mongodblog.get(str);
//					String[] templogContents = tempLogContent.split("\n");
//					
//					for(int i=0;i<templogContents.length;i++)
//					{
//						
//						String[] _templogContents = templogContents[i].split("\t");
//						System.out.println("log内部的长度"+ _templogContents.length);
//						for(int j=0;j<_templogContents.length;j++)
//							mongodbmap.put(mongodbkeys[j], _templogContents[j]);
//						
//						this.insertMongoDataToMmongo(mongodbmap);
//						mongodbmap.clear();			
//					}
//		
//				}
//			
//		}catch(Exception e)
//		{
//			e.printStackTrace();
//			LoggerString loger = LoggerString.getInstance(LogToLocalBolt.class);
//			loger.setErrno("002");
//			if(e.getMessage()!=null&&!"".equals(e.getMessage())){
//				loger.setInfo(e.getMessage());
//			}
//			LOG.info("上传失败");
//			LOG.error(Clogger.getStringLog(loger));
//		}
	}
	
	//插入数据到MongoDB数据库中
		public void insertMongoDataToMmongo(Map<String,Object> mondodbmap)
			{
				Document insertData=new Document();
				for(String  key: mondodbmap.keySet())
				{
					insertData.put(key, mondodbmap.get(key));
				}			
				this.mongodbCollection.insertOne(insertData);
				LOG.debug("插入数据成功！");
			}
}
