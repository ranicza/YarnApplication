package com.epam.bigdata.q3.task2.yarn_app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class TextLogic {
	
	private static final String URL_PATTERN = "(http[s]*:[^\\s\\r\\n]+)";
	private static final String _COM = ".com/";
	private static final String _HTML = ".html";
	private static final String WORDS_URL = "words from url: ";
	private static final String HYPHEN = "-";
	private static final String NEWLINE = "\n";
	private static final String SPLIT_BY = "\\s+";
	private static final String SPACE = " ";
	private static final String COMMA = ",";
	
	private static final String ERROR_URL = "Exception in extracting text from url: ";
	private static final String ERROR_READ ="Exception during reading file: ";
	private static final String ERROR_WRITE = "Exception during write to file: ";
	
	private static String header;
	
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
			content = url.split(_COM)[1].replace(_HTML, "").replace(HYPHEN, SPACE);
			System.out.println(WORDS_URL + content);
			System.out.println(ERROR_URL + e.getMessage());
		}
		return content;
	}
	
	/**
	 * Get count of all the lines from the file.
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
    public static long linesCount (String file) throws IOException, URISyntaxException {
        FileSystem fileSystem = FileLogic.getFS();
        Path path = new Path(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        int linesCount = 0;
        while (br.readLine() != null) {
            linesCount++;
        }      
        System.out.println("Lines count: " + (linesCount - 1));
        return linesCount - 1;
    }
    
    /**
     * Read file to array of lines.
     * 
     * @param lines
     * @param offset
     * @param count
     */
    public static void readFromFile( List<String> lines, int offset, int count) {
    	int currentLine = 0;
		try {
			BufferedReader br = FileLogic.initReader(Constants.INPUT_FILE);
			String line = br.readLine();
			header = line;
			line = br.readLine();

			while (line != null && currentLine < offset + count) {
	            if (currentLine >= offset) {
	                lines.add(line.trim());
	            }
	            line = br.readLine();
	            currentLine++;
	        }
		} catch (IOException e) {
			System.out.println(ERROR_READ + e.getMessage() + e);
		}	
    }
    
    /**
     * Write content to file.
     * 
     * @param offset
     * @param lines
     * @param topWords
     */
    public static void writeToFile(int offset,List<String> lines, List<List<String>> topWords){
		try {				
	    	BufferedWriter brOut = FileLogic.initWriter(Constants.OUTPUT_FILE + "_" + offset + ".txt");			
	    	
	    	brOut.write(header);
			brOut.write(NEWLINE);		
			
			for (int i = 0; i < lines.size(); i++) {

				String curLine = lines.get(i);
				String[] params = curLine.split(SPLIT_BY);

				for (int j = 0; j < params.length; j++) {
					if (j == 1) {
						List<String> currentTopWords = topWords.get(i);
						for (int k = 0; k < currentTopWords.size(); k++) {
							brOut.write(currentTopWords.get(k));					
							if (k < (currentTopWords.size() - 1)) {
								brOut.write(COMMA);
							}
						}
						brOut.write(SPACE);
					}
					brOut.write(params[j]);
					if (j < (params.length - 1)) {
						brOut.write(SPACE);
					}
				}
				brOut.write(NEWLINE);
			}
			brOut.close();			
		} catch (IOException e) {
			System.out.println(ERROR_WRITE + e.getMessage() + e);
		}		
    }
    
}
