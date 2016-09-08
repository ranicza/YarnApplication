package com.epam.bigdata.q3.task2.yarn_app;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WordLogic {
	
	public static void initStopWords(Set<String> stopWords) {
		Path file = new Path(Constants.STOPWORDS_FILE);
		BufferedReader br = FileLogic.initReader(Constants.STOPWORDS_FILE);
		
		String stopWord = null;
		try {
			while ((stopWord = br.readLine()) != null) {
				stopWords.add(stopWord.trim().toUpperCase());
			}
		} catch (IOException e) {
			System.out.println("Exception while reading stop words file: " + e.getMessage());
		}
	}
	
	// Get all words from text
	public static List<String> getAllWords(String text, Set<String> stopWords) {
		List<String> words = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(text, " .,?!:;()<>[]\b\t\n\f\r\"\'");

		while (tokenizer.hasMoreTokens()) {
			words.add(tokenizer.nextToken().toUpperCase());
		}
		words = cleanWords(text.toUpperCase(), stopWords);
		return words;
	}

	// Get all correct words
	private static List<String> cleanWords(String text, Set<String> stopWords) {
		List<String> correctWords = Pattern.compile("\\W").splitAsStream(text).filter((s -> !s.isEmpty()))
				.filter(w -> !Pattern.compile("\\d+").matcher(w).matches()).collect(toList());
		correctWords = correctWords.stream().filter(w -> !stopWords.contains(w)).collect(toList());
		return correctWords;
	}
	
	/**
	 * Add top words to the collection.
	 * 
	 * @param content
	 * @param topWords
	 * @param stopWords
	 */
	public static void getTopWords(String content, List<List<String>> topWords, Set<String> stopWords) {
		List<String> words = WordLogic.getAllWords(content, stopWords).stream().map(String::toLowerCase)
				.collect(groupingBy(Function.identity(), counting())).entrySet().stream()
				.sorted(Map.Entry.<String, Long>comparingByValue(Collections.reverseOrder())
						.thenComparing(Map.Entry.comparingByKey()))
				.limit(10).map(Map.Entry::getKey).collect(toList());
		topWords.add(words);
	}
	
}
