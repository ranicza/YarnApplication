package com.epam.bigdata.q3.task2.yarn_app;

import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class HelloYarn {
	private static final long MEGABYTE = 1024L * 1024L;

	public HelloYarn() {
		System.out.println("HelloYarn!");
	}

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}

	private void execute() {
		// try{
		// Pattern p = Pattern.compile("http[s]*:[^\\s\\r\\n]+");
		// List<String> urls = new ArrayList<String>();
		//
		// Path pt=new Path(Constants.INPUT_FILE);
		//
		// FileSystem fs2 = FileSystem.get(new Configuration());
		// Configuration conf = new Configuration();
		// conf.set("fs.hdfs.impl",
		// org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		// conf.set("fs.file.impl",
		// org.apache.hadoop.fs.LocalFileSystem.class.getName());
		//
		// FileSystem fs = FileSystem.get(new
		// URI("hdfs://sandbox.hortonworks.com:8020"),conf);
		// BufferedReader br=new BufferedReader(new
		// InputStreamReader(fs.open(pt)));
		// List<String> lines = new ArrayList<String>();
		//
		// String line=br.readLine();
		// String topLine = line;
		// line=br.readLine();
		// while (line != null){
		// lines.add(line.trim());
		// line=br.readLine();
		// }
		//
		// System.out.println("STEP 1 " + lines.size());
		// for (String l : lines) {
		// Matcher m = p.matcher(l);
		// m.matches();
		// while (m.find()) {
		// urls.add(m.group());
		// }
		// }
		// //List<String> urls = getUrlsFromDB();
		// System.out.println("STEP 2 " +urls.size());
		// List<List<String>> totalTopWords = new ArrayList<>();
		// for (String u : urls) {
		// Document d = Jsoup.connect(u).get();
		// String text = d.body().text();
		//
		// StringTokenizer tokenizer = new StringTokenizer(text, "
		// .,?!:;()<>[]\b\t\n\f\r\"\'\\");
		// List<String> words = new ArrayList<String>();
		// while(tokenizer.hasMoreTokens()) {
		// words.add(tokenizer.nextToken());
		// //System.out.println(tokenizer.nextToken());
		// }
		//
		// List<String> topWords = words.stream()
		// .map(String::toLowerCase)
		// .collect(groupingBy(Function.identity(), counting()))
		// .entrySet().stream()
		// .sorted(Map.Entry.<String, Long>
		// comparingByValue(Collections.reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
		// .limit(10)
		// .map(Map.Entry::getKey)
		// .collect(toList());
		// totalTopWords.add(topWords);
		// }
		//
		// System.out.println("STEP 3 " +totalTopWords.size());
		// try{
		// Path ptOut=new Path(Constants.OUTPUT_FILE);
		// //Configuration conf = new Configuration();
		// conf.set("fs.hdfs.impl",
		// org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		// conf.set("fs.file.impl",
		// org.apache.hadoop.fs.LocalFileSystem.class.getName());
		//
		// FileSystem fsOut = FileSystem.get(new
		// URI("hdfs://sandbox.hortonworks.com:8020"),conf);
		// //FileSystem fsOut = FileSystem.get(new Configuration());
		// BufferedWriter brOut = new BufferedWriter(new
		// OutputStreamWriter(fsOut.create(ptOut,true)));
		//
		// brOut.write(topLine);
		// brOut.write("\n");
		// System.out.println("STEP 4");
		// for (int i = 0; i < lines.size(); i++) {
		// String currentLine = lines.get(i);
		// String[] params = currentLine.split("\\s+");
		// for (int j = 0; j < params.length; j++) {
		// if (j == 1) {
		// List<String> currentTopWords = totalTopWords.get(i);
		//
		// for (int k = 0; k < currentTopWords.size(); k++) {
		// brOut.write(currentTopWords.get(k));
		// if (k < (currentTopWords.size()-1)) {
		// brOut.write(",");
		// }
		// }
		// brOut.write(" ");
		// }
		// brOut.write(params[j]);
		// if (j < (params.length-1)) {
		// brOut.write(" ");
		// }
		// }
		// brOut.write("\n");
		// }
		//
		// System.out.println("STEP 5");
		// brOut.close();
		// }catch(Exception e) {
		// System.out.println(e.getMessage());
		// }
		// }catch(Exception e){
		// System.out.println(e.getMessage());
		// }

		try {
			List<String> links = new ArrayList<String>();
			List<String> topWords = null;
			List<List<String>> totalTopWords = new ArrayList<>();

			Path pt = new Path(Constants.INPUT_FILE);

			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

			FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			List<String> lines = new ArrayList<String>();

			String line = br.readLine();
			String topLine = line;
			line = br.readLine();
			while (line != null) {
				lines.add(line.trim());
				line = br.readLine();
			}

			System.out.println("Lines size: " + lines.size());

			for (String item : lines) {
				String link = extractLinks(item);
				if (link != null) {
					links.add(link);
					System.out.println("Link: " + link);
				}
			}

			System.out.println("Links size: " + links.size());

			for (String url : links) {
				Document d = Jsoup.connect(url).get();
				String text = d.body().text();
				Document doc = Jsoup.parse(text);

				topWords = getAllWords(doc.text()).stream().map(String::toLowerCase)
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

			System.out.println("WP STEP 3 " + totalTopWords.size());

			try {
				Path ptOut = new Path(Constants.OUTPUT_FILE);
				Configuration confOut = new Configuration();
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

				FileSystem fsOut = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), confOut);
				// FileSystem fsOut = FileSystem.get(new Configuration());
				BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fsOut.create(ptOut, true)));

				// brOut.write(topLine);
				// brOut.write("\n");
				// System.out.println("STEP 4");
				// for (int i = 0; i < lines.size(); i++) {
				// String currentLine = lines.get(i);
				// System.out.println("currentLine: " + currentLine);
				// String[] params = currentLine.split("\\s+");
				// for (int j = 0; j < params.length; j++) {
				// if (j == 1) {
				// List<String> currentTopWords = totalTopWords.get(i);
				// System.out.println("currentTopWords: " + currentTopWords);
				//
				// for (int k = 0; k < currentTopWords.size(); k++) {
				// brOut.write(currentTopWords.get(k));
				// if (k < (currentTopWords.size() - 1)) {
				// brOut.write(",");
				// }
				// }
				// brOut.write(" ");
				// }
				// brOut.write(params[j]);
				// if (j < (params.length - 1)) {
				// brOut.write(" ");
				// }
				// }
				// brOut.write("\n");
				// }
				// System.out.println("STEP 5");
				// brOut.close();

				brOut.write(topLine);
				brOut.write("\n");
				System.out.println("STEP 4");
				for (int i = 0; i < lines.size(); i++) {

					String currentLine = lines.get(i);
					System.out.println("currentLine: " + currentLine);
					String[] params = currentLine.split("\\s+");

					for (int j = 0; j < params.length; j++) {
						if (j == 1) {
							List<String> currentTopWords = totalTopWords.get(i);
							System.out.println("currentTopWords: " + currentTopWords);
							for (int k = 0; k < currentTopWords.size(); k++) {
								brOut.write(currentTopWords.get(k));
								System.out.println("currentTopWords.get(k): " + currentTopWords.get(k));
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
				System.out.println("STEP 5");

				brOut.close();
			} catch (Exception e) {
				System.out.println("exception 1: " + e.getMessage() + e);
			}
		} catch (Exception e) {
			System.out.println("exception 2: " + e.getMessage() + e);
		}
	}

	// Get all links from text
	private static String extractLinks(String line) {
		String result = null;
		Pattern pattern = Pattern.compile("(http[s]*:[^\\s\\r\\n]+)", Pattern.DOTALL);
		Matcher matcher = pattern.matcher(line);

		while (matcher.find()) {
			result = matcher.group();
		}
		return result;
	}

	// Get all words from text
	public static List<String> getAllWords(String text) {
		List<String> words = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(text, " .,?!:;()<>[]\b\t\n\f\r\"\'");

		while (tokenizer.hasMoreTokens()) {
			words.add(tokenizer.nextToken());
		}
		words = cleanWords(text);
		return words;
	}

	// Get all correct words
	public static List<String> cleanWords(String text) {
		List<String> correctWords = Pattern.compile("\\W").splitAsStream(text).filter((s -> !s.isEmpty()))
				.filter(w -> !Pattern.compile("\\d+").matcher(w).matches()).collect(toList());
		return correctWords;
	}

	public static void main(String[] args) {
		HelloYarn helloYarn = new HelloYarn();
		helloYarn.execute();
	}
}
