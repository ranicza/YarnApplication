package com.epam.bigdata.q3.task2.yarn_app;

import java.util.*;

public class YarnMain {
	private static final String ERROR = "Exception: "; 
	// Offset of lines from the file for certain container.
	private int offset;
	
	// Count of lines from the file for certain container.
	private int count;
	    
	private Set<String> stopWords = new HashSet<String>();
	private List<String> links;
	private List<String> lines;
	private List<List<String>> topWords;

	public YarnMain() {}
	
    public YarnMain(int offset, int count) {
        this.offset = offset;
        this.count = count;
    }

	private void execute() {
		links = new ArrayList<String>();
		lines = new ArrayList<String>();
		topWords = new ArrayList<>();

		// Initialize a set with stop words for excluding.
		WordLogic.initStopWords(stopWords);
		
		try {
			TextLogic.readFromFile(lines, offset, count);
			for (String item : lines) {
				String link = TextLogic.extractLinks(item);
				if (link != null) {
					links.add(link);
				}
			}

			for (String url : links) {
				String content = TextLogic.getTextFromUrl(url);
				WordLogic.getTopWords(content, topWords, stopWords);
			}
			
			TextLogic.writeToFile(offset, lines, topWords);			
		} catch (Exception e) {
			System.out.println(ERROR + e.getMessage() + e);
		}
	}
	
	public static void main(String[] args) {
        String offset = args[0];
        String count = args[1];
        
		YarnMain helloYarn = new YarnMain(Integer.valueOf(offset), Integer.valueOf(count));
		helloYarn.execute();
	}
}
