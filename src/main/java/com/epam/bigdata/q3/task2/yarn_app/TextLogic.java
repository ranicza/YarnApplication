package com.epam.bigdata.q3.task2.yarn_app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.apache.hadoop.conf.Configuration;

public class TextLogic {
	
	private static final String URL_PATTERN = "(http[s]*:[^\\s\\r\\n]+)";
	private static final String _COM = ".com/";
	private static final String _HTML = ".html";
	
	private static final String ERROR_URL = "Exception in extracting text from url: ";
	private static final String WORDS_URL = "words from url: ";
	private static final String HYPHEN = "-";
	
	/**
	 * Get all links from text.
	 * 
	 * @param line
	 * @return String with links
	 */
	public static String extractLinks(String line) {
		String result = null;
		Pattern pattern = Pattern.compile(URL_PATTERN, Pattern.DOTALL);
		Matcher matcher = pattern.matcher(line);
		while (matcher.find()) {
			result = matcher.group();
		}
		return result;
	}
	
	/**
	 * Get text from html page by link.
	 * If page not found - extract words from url.
	 * 
	 * @param url
	 * @return String with text
	 */
	public static String getTextFromUrl(String url) {
		String content = null;
		try {
			Document d = Jsoup.connect(url).ignoreHttpErrors(true).timeout(500).get();
			String text = d.body().text();
			Document doc = Jsoup.parse(text);
			content = doc.text();
		} catch (IOException e) {
			content = url.split(_COM)[1].replace(_HTML, "").replace(HYPHEN, " ");
			System.out.println(WORDS_URL + content);
			System.out.println(ERROR_URL + e.getMessage());
		}
		return content;
	}
	
    public static long linesCount (String file) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
        Path path = new Path(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        int linesCount = 0;
        while (br.readLine() != null) {
            linesCount++;
        }
        
        System.out.println("Lines count: " + (linesCount - 1));
        return linesCount - 1;
    }
    
    public static void writeTextToFile(int offset, String header, List<String> lines, List<List<String>> topWords){
		try {				
	    	BufferedWriter brOut = FileLogic.initWriter(Constants.OUTPUT_FILE + "_" + offset + ".txt");			
	    	
	    	brOut.write(header);
			brOut.write("\n");		
			
			for (int i = 0; i < lines.size(); i++) {

				String curLine = lines.get(i);
				String[] params = curLine.split("\\s+");

				for (int j = 0; j < params.length; j++) {
					if (j == 1) {
						List<String> currentTopWords = topWords.get(i);
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
			brOut.close();			
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage() + e);
		}
		
    }
}
