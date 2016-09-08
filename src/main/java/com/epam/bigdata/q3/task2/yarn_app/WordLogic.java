package com.epam.bigdata.q3.task2.yarn_app;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Function;
import java.util.regex.Pattern;

public class WordLogic {

	private static final String ERROR_INIT_STOP_WORDS = "Exception during initialing stop words: ";
	private static final String SPLIT_BY = " .,?!:;()<>[]\b\t\n\f\r\"\'";

	/**
	 * Initialize a set with stop words for excluding.
	 * 
	 * @param stopWords
	 */
	public static void initStopWords(Set<String> stopWords) {
		String stopWord = null;
		BufferedReader br = FileLogic.initReader(Constants.STOPWORDS_FILE);

		try {
			while ((stopWord = br.readLine()) != null) {
				stopWords.add(stopWord.trim().toUpperCase());
			}
		} catch (IOException e) {
			System.out.println(ERROR_INIT_STOP_WORDS + e.getMessage());
		}
	}

	/**
	 * Get all words from the text.
	 * 
	 * @param text
	 * @param stopWords
	 * @return
	 */
	public static List<String> getAllWords(String text, Set<String> stopWords) {
		List<String> words = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(text, SPLIT_BY);

		while (tokenizer.hasMoreTokens()) {
			words.add(tokenizer.nextToken().toUpperCase());
		}
		words = cleanWords(text.toUpperCase(), stopWords);
		return words;
	}

	/**
	 * Get all correct words.
	 * 
	 * @param text
	 * @param stopWords
	 * @return
	 */
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
