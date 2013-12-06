package fileWalkerCounter;

import static java.nio.file.FileVisitResult.CONTINUE;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
        private SpoutOutputCollector collector;
        private String parentDir;
        private LineNumberReader in;
        
        public FileReaderSpout(String parentDir) {
                this.parentDir = parentDir;
        }

        public boolean isDistributed() {
                return false;
        }
        

        public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
                this.collector = collector;
                try {
                        in = new LineNumberReader(new FileReader(parentDir));
                } catch (FileNotFoundException e){
                        System.err.print("######## FileReaderSpout.nextTuple: " + e);
                }
        }

        public void close() {
                if (in == null) return;
                try {
                        in.close();
                } catch (IOException e) {
                        System.err.print("######## FileReaderSpout.nextTuple: " + e);
                }
        }

        public void ack(Object msgId) {
                System.out.println("######## FileReaderSpout.ack: msgId=" + msgId);
        }

        public void fail(Object msgId) {
                System.out.println("######## FileReaderSpout.fail: msgId=" + msgId);
        }

        public void nextTuple() {
                if (in == null) return;
                String line = null;
                
                try {
                        line = in.readLine();
                } catch (IOException e) {
                        System.err.print("######## FileReaderSpout.nextTuple: " + e);
                }
                
                if (line != null) {
                        int lineNum = in.getLineNumber();
                        System.out.println("######## FileReaderSpout.nextTuple: " + lineNum + ". " + line);
                        collector.emit(new Values(line));
                }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

		@Override
		public void activate() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void deactivate() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
		
		static String readFile(String path, Charset encoding) 
		  throws IOException 
		{
		  byte[] encoded = Files.readAllBytes(Paths.get(path));
		  return encoding.decode(ByteBuffer.wrap(encoded)).toString();
		}
}