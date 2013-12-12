package simple;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class PrinterBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    System.out.println(tuple);
    
    double[] dist = (double[]) tuple.getValue(0);
    
    for(int i=0;i<dist.length;i++){
    	System.out.println(dist[i]);
    }
    
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
