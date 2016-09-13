package com.jackniu;

import java.io.BufferedReader;
import java.io.File;

import java.io.FileReader;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class LogReadFromFileSpout extends BaseRichSpout{
	SpoutOutputCollector _collector;
	private StringBuffer sb;
	private static final String[] mongodbkeys =new String[]{"LogType","DateTimeStr","Product","System","File","LineNumber",
			"Errno","Mac","DevType","SN","Ip","LogId","Uri","TimeUsed","Info"};
	
	
	
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this._collector=collector;
		
		File file = new File("/kafkadata");
		try {
			FileReader  reader= new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			
			sb = new StringBuffer();
			String line=br.readLine();
			while(line != null)
			{
				sb.append(line);
				line=br.readLine();
				
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
//		collector.emit(new Values("productname",contentJson.toString()));
		
	}

	public void nextTuple() {
		String logContent =sb.toString(); 
		_collector.emit(new Values("productName",logContent));
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("product","logContentIntegrate"));
		
	}

}
