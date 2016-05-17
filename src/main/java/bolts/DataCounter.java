package bolts;

import backtype.storm.topology.base.BaseBasicBolt;

import java.io.*;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class DataCounter extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Integer id;
	String name;
	Map<String, Integer> counters;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
			
		}
		
	}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		FileWriter writer;
		Date date=new Date();
		try {
			writer=new FileWriter("/home/ubuntu/results.txt",false);
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			bufferedWriter.write("Starting new run "+new Timestamp(date.getTime()));
			bufferedWriter.newLine();
            bufferedWriter.close();
	        } catch (IOException e) {}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String str = input.getString(0);
		/**
		 * If the word dosn't exist in the map we will create
		 * this, if not We will add 1 
		 */
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		FileWriter writer;
		try {
			writer=new FileWriter("/home/ubuntu/results.txt",true);
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			for(Map.Entry<String, Integer> entry : counters.entrySet()){
				bufferedWriter.write(entry.getKey()+": "+entry.getValue()+" ");
			}
			bufferedWriter.newLine();
            bufferedWriter.close();
	        } catch (IOException e) {}
		
	}
	public void writeResults(){
		
	}
	

}
