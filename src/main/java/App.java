//package jayant.dataanalytics.kafka_consumer_storm;
import bolts.DataCounter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.DataSplitter;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class App {
	public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException
    {
        ZkHosts zkHosts=new ZkHosts("172.24.42.8:2181");
         
        String topic_name="test_topic";
        String consumer_group_id="id7";
        String zookeeper_root="/test_topic";
        SpoutConfig kafkaConfig=new SpoutConfig(zkHosts, topic_name, zookeeper_root, consumer_group_id);
         
        kafkaConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
//        kafkaConfig.forceFromStart=true;
         
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
     //   builder.setBolt("PrinterBolt", new PrinterBolt()).globalGrouping("KafkaSpout");
		builder.setBolt("data-splitter", new DataSplitter()).shuffleGrouping("KafkaSpout");
		builder.setBolt("word-counter", new DataCounter(),1).fieldsGrouping("data-splitter", new Fields("gender"));
         
        Config config=new Config();
         
     //   LocalCluster cluster=new LocalCluster();
        config.setNumWorkers(1);
         
        try {
			StormSubmitter.submitTopology("Gender Count", config, builder.createTopology());
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         
        try{
         Thread.sleep(60000);
        }catch(InterruptedException ex)
        {
         ex.printStackTrace();
        }
//         
//        cluster.killTopology("KafkaConsumerTopology");
//        cluster.shutdown();
    }
}
