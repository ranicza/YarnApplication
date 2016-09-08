package com.epam.bigdata.q3.task2.yarn_app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileLogic {

	private static final String PATH = "hdfs://sandbox.hortonworks.com:8020";
	private static final String FS_HDFS = "fs.hdfs.impl";
	private static final String FS_FILE = "fs.file.impl";
	
	private static final String ERROR_BREADER = "Exception while initializing BufferedReader object: ";
	private static final String ERROR_BWRITER = "Exception while initializing BufferedWriter object: ";
	
	/**
	 * Get FileSystem object.
	 * 
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static FileSystem getFS() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		conf.set(FS_HDFS, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set(FS_FILE, org.apache.hadoop.fs.LocalFileSystem.class.getName());

		return FileSystem.get(new URI(PATH), conf);
	}
	/**
	 * Initialize BufferedReader object.
	 * @param path
	 * @return
	 */
	public static BufferedReader initReader(String path) {
		Path file = new Path(path);
		BufferedReader br = null;
		try {
			FileSystem fs = getFS();
			br = new BufferedReader(new InputStreamReader(fs.open(file)));
		} catch (IOException | URISyntaxException e) {
			System.out.println(ERROR_BREADER + e.getMessage());
		}		
		return br;
	}
	
	/**
	 * Initialize BufferedWriter object.
	 * 
	 * @param path
	 * @return
	 */
	public static BufferedWriter initWriter(String path) {
		Path file = new Path(path);
		BufferedWriter bw = null;
		try {
			FileSystem fsOut = getFS();
			bw = new BufferedWriter(new OutputStreamWriter(fsOut.create(file, true)));
		} catch (IOException | URISyntaxException e) {
			System.out.println(ERROR_BWRITER + e.getMessage());
		}		
		return bw;
	}
	
}
