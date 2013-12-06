package simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FileReaderBolt extends BaseRichBolt {
	private OutputCollector _collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		_collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
	try {
		String fileContent = readFile(input.getString(0));
		int strIndex = fileContent.indexOf("X-FileName");
		strIndex = fileContent.indexOf("\n", strIndex)+1;
		fileContent = fileContent.substring(strIndex);
		_collector.emit(new Values(fileContent));
	} catch (IOException e) {
		 System.err.print("######## FileReaderBolt.execute: " + e);
	} 
	
//    System.out.println(tuple);
	
  }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("fileContent"));
		
	}

	static String readFile(String path) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		Charset encoding = Charset.defaultCharset();
		return encoding.decode(ByteBuffer.wrap(encoded)).toString();
	}

}
