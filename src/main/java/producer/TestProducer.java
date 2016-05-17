package producer;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import bolts.DataCounter;

public class TestProducer {
	public static void main(String[] args) {
       // long events = Long.parseLong(args[0]);
        Random rnd = new Random();
        Properties props = new Properties();
        props.put("metadata.broker.list", "172.24.42.20:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "producer.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
        String[] sentences = new String[]{ "Orlando	M	40	Python", "Lina	F	39	C#",
                "John	M	30	Python", "Jane	F	32	Python", "Michelle	F	18	Python", "Daniel	M	20	C#" };
        for (long nEvents = 0; nEvents < 10; nEvents++) { 

        	String sentence = sentences[rnd.nextInt(sentences.length)];
        	System.out.println(sentence);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("test_topic",sentence);

            producer.send(data);

        }
       producer.close();
    }
}

