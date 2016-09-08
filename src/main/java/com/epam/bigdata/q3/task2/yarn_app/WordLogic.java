package com.epam.bigdata.q3.task2.yarn_app;

import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WordLogic {
	
	public static void initStopWords(Set<String> stopWords) {
		Path file = new Path(Constants.STOPWORDS_FILE);
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

			FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));

			String stopWord = null;
			while ((stopWord = br.readLine()) != null) {
				stopWords.add(stopWord.trim().toUpperCase());
			}
		} catch (Exception e) {
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
}
