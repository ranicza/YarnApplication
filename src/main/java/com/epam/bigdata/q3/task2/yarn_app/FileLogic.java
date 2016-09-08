package com.epam.bigdata.q3.task2.yarn_app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileLogic {

	public static BufferedReader initReader(String path) {
		Path file = new Path(path);
		BufferedReader br = null;
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

			FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
			br = new BufferedReader(new InputStreamReader(fs.open(file)));

		} catch (Exception e) {
			System.out.println("Exception while reading stop words file: " + e.getMessage());
		}		
		return br;
	}
	
	public static BufferedWriter initWriter(String path) {
		Path file = new Path(path);
		BufferedWriter bw = null;
		try {
			Configuration confOut = new Configuration();
			confOut.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			confOut.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

			FileSystem fsOut = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), confOut);
			bw = new BufferedWriter(new OutputStreamWriter(fsOut.create(file, true)));
		} catch (Exception e) {
			System.out.println("Exception while reading stop words file: " + e.getMessage());
		}		
		return bw;
	}
	
}
