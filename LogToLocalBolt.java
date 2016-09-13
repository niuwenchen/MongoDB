package com.jackniu;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.changhong.bigdata.bdlog.LoggerString;
import com.changhong.bslog.Clogger;
import com.changhong.logmonitor.hdfs.utils.HdfsDAO;
import com.changhong.logmonitor.hive.utils.Excutehivesql;
//import com.changhong.logmonitor.mongodb.utils.ConnectToMongoDB;
import com.mongodb.client.MongoCollection;
import com.test.ConnectToMongoDB;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import kafka.common.UnknownException;
import net.sf.json.JSONObject;
import scala.sys.process.ProcessBuilderImpl.FileOutput;

public class LogToLocalBolt extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(LogToLocalBolt.class);
	// 缺一个linenumber
	private static final String[] mongodbkeys =new String[]{"LogType","DateTimeStr","Product","System","File","LineNumber",
			"Errno","Mac","DevType","SN","Ip","LogId","Uri","TimeUsed","Info"};
	
	 private final String mongodbIp;
     private final int mongodbPort;
//    
     // tableName 就是  MongoDB的名字
     private final String tableName;
    
	// MongoDB的配置 端口  连接 集合 记录 失败日志内容
    private ConnectToMongoDB connectMg;
    private MongoCollection<Document> mongodbCollection;
   
	
	
	public void prepare(Map stormConf, TopologyContext context)
	{
		this.connectMg= new ConnectToMongoDB(this.mongodbIp, this.mongodbPort);
		try{
			this.mongodbCollection = this.connectMg.getCollection(this.tableName);
		}catch(UnknownHostException e)
		{
			System.out.println("远程连接失败");
			e.printStackTrace();
			LoggerString loger = LoggerString.getInstance(LogToLocalBolt.class);
			loger.setErrno("002");
			if(e.getMessage()!=null&&!"".equals(e.getMessage())){
				loger.setInfo(e.getMessage());
			}
			LOG.error(Clogger.getStringLog(loger));
		}
	}
	
	public LogToLocalBolt(String tableName,String mongodbIp,int mongodbPort){
		// 自己定义的数据表名
		
//		this.tableName=tableName+"_fail_log";
		this.tableName="jackdbtest";
		this.mongodbIp=mongodbIp;
    	this.mongodbPort=mongodbPort;
    
	}
	
	// 108  记录 // 一个表名
	
	
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		try
		{
			LOG.debug("LOGTOHDFSBOLT TUPLE" + input.toString());
			String  tuple = input.toString();
			
			
			// 现在我需要的是这个tuple的原始数据  才能实现模拟
			
			System.out.println("******"+tuple);
			String  logContent = input.getValueByField("logContentIntegrate").toString();
			Map<String,Object> mongodbmap = new HashMap<String,Object>();
			Map<String,String> mongodblog=JSONObject.fromObject(logContent);
			//构建一个文件输出环境
//			FileWriter  fsout = new FileWriter(new File("/kafkadata"));
//			BufferedWriter  writer = new BufferedWriter(fsout);
//			writer.write(mongodblog.toString());
//			writer.close();
			
			
			
			
			for(String str:mongodblog.keySet())
			{
				String tempLogContent = mongodblog.get(str);
				String[] templogContents = tempLogContent.split("\n");
				
				for(int i=0;i<templogContents.length;i++)
				{
					
					String[] _templogContents = templogContents[i].split("\t");
					System.out.println("log内部的长度"+ _templogContents.length);
					for(int j=0;j<_templogContents.length;j++)
						mongodbmap.put(mongodbkeys[j], _templogContents[j]);
					
					this.insertMongoDataToMmongo(mongodbmap);
					mongodbmap.clear();			
				}
	
			}	
			
		}catch(Exception e)
		{
			e.printStackTrace();
			LOG.info("上传失败");
		}
		
	}

	//存在的问题： 字段一致  数值不同
	

	
	
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

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		
	}

    @Override
    public void cleanup(){
    	this.connectMg.destory();
    }

}
