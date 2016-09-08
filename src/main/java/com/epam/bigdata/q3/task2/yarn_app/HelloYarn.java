package com.epam.bigdata.q3.task2.yarn_app;

import java.io.BufferedReader;

import java.util.*;

public class HelloYarn {
	
	// Offset of lines from the file for certain container.
	private int offset;
	
	// Count of lines from the file for certain container.
	private int count;
	    
	private Set<String> stopWords = new HashSet<String>();

	public HelloYarn() {}
	
    public HelloYarn(int offset, int count) {
        this.offset = offset;
        this.count = count;
    }

	private void execute() {
		int currentLine = 0;
		
		List<String> links = new ArrayList<String>();
		List<String> lines = new ArrayList<String>();
		List<String> words = null;
		List<List<String>> topWords = new ArrayList<>();	
		
		// Initialize a set with stop words for excluding.
		WordLogic.initStopWords(stopWords);
		
		try {
			BufferedReader br = FileLogic.initReader(Constants.INPUT_FILE);
			String line = br.readLine();
			String header = line;
			line = br.readLine();

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
				}
			}
			System.out.println("Links size: " + links.size());

			for (String url : links) {
				String content = TextLogic.getTextFromUrl(url);
				WordLogic.getTopWords(content, topWords, stopWords);
			}
			
			TextLogic.writeTextToFile(offset, header, lines, topWords);			
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage() + e);
		}
	}
	
	public static void main(String[] args) {
        String offset = args[0];
        String count = args[1];
        
		HelloYarn helloYarn = new HelloYarn(Integer.valueOf(offset), Integer.valueOf(count));
		helloYarn.execute();
	}
}
