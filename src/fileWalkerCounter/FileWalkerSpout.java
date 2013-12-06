package fileWalkerCounter;

/*
 * Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;

import static java.nio.file.FileVisitResult.*;
import static java.nio.file.FileVisitOption.*;

import java.util.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Sample code that finds files that match the specified glob pattern. For more
 * information on what constitutes a glob pattern, see
 * http://docs.oracle.com/javase
 * /javatutorials/tutorial/essential/io/fileOps.html#glob
 * 
 * The file or directories that match the pattern are printed to standard out.
 * The number of matches is also printed.
 * 
 * When executing this application, you must put the glob pattern in quotes, so
 * the shell will not expand any wild cards: java Find . -name "*.java"
 */

public class FileWalkerSpout extends BaseRichSpout{
	 SpoutOutputCollector _collector;
	 String _strPath;
	 List<String> paths;
	 List<String> curFiles;
	 HashMap<String,List<String>> fileMap;
	 
	 public FileWalkerSpout(String strPath) {
			_strPath = strPath;
			paths = new ArrayList<String>();
			curFiles = new ArrayList<String>();
			fileMap = new HashMap<String,List<String>>();
		}

	public class PrintFiles extends SimpleFileVisitor<Path> {

		public PrintFiles(Path startingDir) {
		}

		// Print information about
		// each type of file.
		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attr) {
			curFiles.add(file.toString());
			return CONTINUE;
		}

		// Print each directory visited.
		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
			if (!dir.equals(Paths.get(_strPath))){
//				System.out.format("%s%n", dir);
				paths.add(dir.toString());
			}
			if (!curFiles.isEmpty()){
				fileMap.put(paths.get(paths.size()-1), curFiles);
			}else{
				curFiles = new ArrayList<String>();
			}
			return CONTINUE;
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

//	static void usage() {
//		System.err.println("java Find <path>");
//		System.exit(-1);
//	}
//
//	 public static void main(String[] args)
//	 throws IOException {
//	
//	 if (args.length < 1)
//	 usage();
//	
//	 Path startingDir = Paths.get(args[0]);
//	
//	 PrintFiles filePrinter = new PrintFiles(startingDir);
//	 Files.walkFileTree(startingDir, filePrinter);
//	 }

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		
		Path startingDir = Paths.get(_strPath);
		
		PrintFiles filePrinter = new PrintFiles(startingDir);
		try {
			Files.walkFileTree(startingDir, filePrinter);
		} catch (IOException e) {
			System.err.print("######## FileWalkerSpout.open: " + e);
		}
		curFiles = fileMap.get(paths.get(index++));
	}
	
	private int index = 0;
	@Override
	public void nextTuple() {
		if(index<paths.size() && curFiles.isEmpty()){
//			_collector.emit(new Values(paths.get(index)));
			curFiles = fileMap.get(paths.get(index++));
		}
		
		if(!curFiles.isEmpty()){
			_collector.emit(new Values(curFiles.remove(0)));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("dir"));	
	}
}