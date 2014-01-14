package mailAnalyserPackage;

import static java.nio.file.FileVisitResult.CONTINUE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Directly interfaces with the Reddit API to create a raw stream of reddit posts
 * 
 * @author Luke Barnett 1109967
 * @author Tony Chen 1111377
 *
 */
public class RawMailSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = -4867266218997902575L;
	private SpoutOutputCollector collector;
	private final String PERSON;
	private final String PATH;
	private int count = 0;
	
	private List<String> paths;
	private List<String> curFiles;
	private HashMap<String,List<String>> fileMap;

	/**
	 * Creates a new raw mail spout for the provided person
	 * @param strString The parent directory containing all mail
	 * @param person The person name to use for the spout
	 */
	public RawMailSpout(String strPath, String person){
		PERSON = person;
		PATH = strPath + PERSON + "/";
		
		Path startingDir = Paths.get(PATH);
		
		PrintFiles filePrinter = new PrintFiles(startingDir);
		try {
			Files.walkFileTree(startingDir, filePrinter);
		} catch (IOException e) {
			System.err.print("######## FileWalkerSpout.open: " + e);
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		//Sleep to reduce congestion
		Utils.sleep(50);
		//Try and get the next post
		Mail nextMail = getNextMail();
		//If we have gotten a post emit it
		if(nextMail != null){
			collector.emit(new Values(nextMail, curFiles.get(count-1)));
		}
			
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("redditPost","filename"));
	}
	
	private Mail getNextMail(){
		String fileContent;
		if(count <curFiles.size()){
			try {
				fileContent = readFile(curFiles.get(count++));
				int strIndex = fileContent.indexOf("X-FileName");
				strIndex = fileContent.indexOf("\n", strIndex)+1;
				fileContent = fileContent.substring(strIndex);
				Mail newMail =  new Mail();
				newMail.setData(fileContent);
				newMail.setLabel(PERSON);
				return newMail;
			} catch (IOException e) {
				System.err.print("######## FileReaderBolt.execute: " + e);
			}
		}
		return null;
	}
	
	public class PrintFiles extends SimpleFileVisitor<Path> {

		public PrintFiles(Path startingDir) {
		}

		// Print information about
		// each type of file.
		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attr) {
			if (curFiles == null)
				curFiles = new ArrayList<String>();
			curFiles.add(file.toString());
			return CONTINUE;
		}

		// Print each directory visited.
		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
			if (dir.endsWith(PERSON)){
				return CONTINUE;
			}else{
				return FileVisitResult.SKIP_SUBTREE;
			}
		}

		// If there is some error accessing
		// the file, let the user know.
		// If you don't override this method
		// and an error occurs, an IOException
		// is thrown.
		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) {
			System.err.println(exc);
			return CONTINUE;
		}
	}
	
	@Override
    public void close() {
        
    }
	
	static String readFile(String path) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		Charset encoding = Charset.defaultCharset();
		return encoding.decode(ByteBuffer.wrap(encoded)).toString();
	}

}
