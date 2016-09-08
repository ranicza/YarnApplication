package com.epam.bigdata.q3.task2.yarn_app;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class HelloYarn {
	
	// Offset of lines from the file for certain container.
	private int offset;
	
	// Count of lines from the file for certain container.
	private int count;
	    
	private Set<String> stopWords = new HashSet();

	public HelloYarn() {}
	
    public HelloYarn(int offset, int count) {
        this.offset = offset;
        this.count = count;
    }

	private void execute() {
		// Initialize a set with stop words for excluding.
		WordLogic.initStopWords(stopWords);
		
		List<String> links = new ArrayList<String>();
		List<String> topWords = null;
		List<List<String>> totalTopWords = new ArrayList<>();
		
		//Path pt = new Path(Constants.INPUT_FILE);
		
		try {
//			Configuration conf = new Configuration();
//			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//			FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
//			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			
			BufferedReader br = FileLogic.initReader(Constants.INPUT_FILE);
					
			List<String> lines = new ArrayList<String>();

			 int currentLine = 0;
			 
			String line = br.readLine();
			String header = line;
			line = br.readLine();
//			while (line != null) {
//				lines.add(line.trim());
//				line = br.readLine();
//			}
			
			while (line != null && currentLine < offset + count) {
	            if (currentLine >= offset) {
	                lines.add(line.trim());
	            }
	            line = br.readLine();
	            currentLine++;
	        }
			

			System.out.println("Lines size: " + lines.size());

			for (String item : lines) {
				String link = TextLogic.extractLinks(item);
				if (link != null) {
					links.add(link);
					System.out.println("Link: " + link);
				}
			}

			System.out.println("Links size: " + links.size());

			for (String url : links) {
				String content = TextLogic.getTextFromUrl(url);
				
				topWords = WordLogic.getAllWords(content, stopWords).stream().map(String::toLowerCase)
						.collect(groupingBy(Function.identity(), counting())).entrySet().stream()
						.sorted(Map.Entry.<String, Long>comparingByValue(Collections.reverseOrder())
								.thenComparing(Map.Entry.comparingByKey()))
						.limit(10).map(Map.Entry::getKey).collect(toList());
				totalTopWords.add(topWords);

				System.out.println("Top words size: " + topWords.size());

				for (String w : topWords) {
					System.out.println(w);
				}
			}

			System.out.println("totalTopWords.size() " + totalTopWords.size());

			try {
//				Path ptOut = new Path(Constants.OUTPUT_FILE + offset);
//				Configuration confOut = new Configuration();
//				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//				FileSystem fsOut = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), confOut);
//				BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fsOut.create(ptOut, true)));
				
				BufferedWriter brOut = FileLogic.initWriter(Constants.OUTPUT_FILE + "_" + offset + ".txt");
						
				brOut.write(header);
				brOut.write("\n");
			
				
				for (int i = 0; i < lines.size(); i++) {

					String curLine = lines.get(i);
					String[] params = curLine.split("\\s+");

					for (int j = 0; j < params.length; j++) {
						if (j == 1) {
							List<String> currentTopWords = totalTopWords.get(i);
							for (int k = 0; k < currentTopWords.size(); k++) {
								brOut.write(currentTopWords.get(k));					
								if (k < (currentTopWords.size() - 1)) {
									brOut.write(",");
								}
							}
							brOut.write(" ");
						}
						brOut.write(params[j]);
						if (j < (params.length - 1)) {
							brOut.write(" ");
						}
					}
					brOut.write("\n");
				}
				
//				 for (int i = 0; i <= lines.size()-1; i++){
//			            String curLine = lines.get(i);
//			            String totalWords = "";
//			            List<String> words = totalTopWords.get(i);
//			            for (int j = 0; j < words.size(); j++){
//			                if (j > 0){
//			                    totalWords += ",";
//			                }
//			                totalWords += words.get(j);
//			            }
//			            String text = curLine.replaceFirst("\\s", " " + totalWords);
//			            brOut.write(text);
//			            brOut.write("\n");
//			        }
				
				
				
				
				
				brOut.close();
			} catch (Exception e) {
				System.out.println("exception 1: " + e.getMessage() + e);
			}
		} catch (Exception e) {
			System.out.println("exception 2: " + e.getMessage() + e);
		}
	}

//	// Get all links from text
//	private String extractLinks(String line) {
//		String result = null;
//		Pattern pattern = Pattern.compile("(http[s]*:[^\\s\\r\\n]+)", Pattern.DOTALL);
//		Matcher matcher = pattern.matcher(line);
//
//		while (matcher.find()) {
//			result = matcher.group();
//		}
//		return result;
//	}

//	// Get all words from text
//	public List<String> getAllWords(String text) {
//		List<String> words = new ArrayList<String>();
//		StringTokenizer tokenizer = new StringTokenizer(text, " .,?!:;()<>[]\b\t\n\f\r\"\'");
//
//		while (tokenizer.hasMoreTokens()) {
//			words.add(tokenizer.nextToken().toUpperCase());
//		}
//		words = cleanWords(text.toUpperCase());
//		return words;
//	}
//
//	// Get all correct words
//	private List<String> cleanWords(String text) {
//		List<String> correctWords = Pattern.compile("\\W").splitAsStream(text).filter((s -> !s.isEmpty()))
//				.filter(w -> !Pattern.compile("\\d+").matcher(w).matches()).collect(toList());
//		correctWords = correctWords.stream().filter(w -> !stopWords.contains(w)).collect(toList());
//		return correctWords;
//	}

	public static void main(String[] args) {
        String offset = args[0];
        String count = args[1];
        
		HelloYarn helloYarn = new HelloYarn(Integer.valueOf(offset), Integer.valueOf(count));
		helloYarn.execute();

	}

//	private void initStopWords() {
//		Path file = new Path(Constants.STOPWORDS_FILE);
//		try {
//			Configuration conf = new Configuration();
//			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//			FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
//			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));
//
//			String stopWord = null;
//			while ((stopWord = br.readLine()) != null) {
//				stopWords.add(stopWord.trim().toUpperCase());
//			}
//		} catch (Exception e) {
//			System.out.println("Exception while reading stop words file: " + e.getMessage());
//		}
//	}

//	private String getTextFromUrl(String url) {
//		String content = null;
//		try {
//			Document d = Jsoup.connect(url).ignoreHttpErrors(true).timeout(500).get();
//			String text = d.body().text();
//			Document doc = Jsoup.parse(text);
//			content = doc.text();
//		} catch (IOException e) {
//			content = url.split(".com/")[1].replace(".html", "").replace("-", " ");
//			System.out.println("words from url: " + content);
//			System.out.println("Exception in extracting text from url: " + e.getMessage());
//		}
//		return content;
//	}
	
//	private BufferedReader fileConnection(String path) {
//		Path file = new Path(path);
//		BufferedReader br = null;
//		try {
//			Configuration conf = new Configuration();
//			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//			FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
//			br = new BufferedReader(new InputStreamReader(fs.open(file)));
//
//		} catch (Exception e) {
//			System.out.println("Exception while reading stop words file: " + e.getMessage());
//		}		
//		return br;
//	}
}
