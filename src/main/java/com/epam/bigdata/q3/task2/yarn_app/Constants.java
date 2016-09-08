package com.epam.bigdata.q3.task2.yarn_app;

public class Constants {
	/**
	   * Environment key name pointing to the the app master jar location
	   */
	  public static final String AM_JAR_PATH = "AM_JAR_PATH";

	  /**
	   * Environment key name denoting the file timestamp for the shell script.
	   * Used to validate the local resource.
	   */
	  public static final String AM_JAR_TIMESTAMP = "AM_JAR_TIMESTAMP";

	  /**
	   * Environment key name denoting the file content length for the shell script.
	   * Used to validate the local resource.
	   */
	  public static final String AM_JAR_LENGTH = "AM_JAR_LENGTH";


	  public static final String AM_JAR_NAME = "AppMaster.jar";
	  
	  public static final String INPUT_FILE = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/homework2/input.txt";

	  public static final String OUTPUT_FILE = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/homework2/out";
	  
	  public static final String STOPWORDS_FILE = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/homework2/stopWords.txt";
}
