package simple;


import fileWalkerCounter.FileWalkerSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import simple.PrinterBolt;


public class PrintSampleStream {        
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", new FileWalkerSpout("/home/manrich/vacwork/datasets/enron_sent/"));
        builder.setBolt("fileData", new FileReaderBolt(),2).shuffleGrouping("spout");
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("fileData");
                
        Config conf = new Config();
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
