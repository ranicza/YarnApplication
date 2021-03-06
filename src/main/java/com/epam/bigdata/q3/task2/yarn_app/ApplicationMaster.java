package com.epam.bigdata.q3.task2.yarn_app;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ApplicationMaster {
	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	// Application Attempt Id ( combination of attemptId and fail count )
	protected ApplicationAttemptId appAttemptID;

	// No. of containers to run shell command on
	private int numTotalContainers = 1;

	// Memory to request for the container on which the shell command will run
	private int containerMemory = 10;

	// VirtualCores to request for the container on which the shell command will
	// run
	private int containerVirtualCores = 1;

	// Priority of the request
	private int requestPriority;

	// Amount of all the lines in the input file.
	private long linesCount;

	// Location of shell script ( obtained from info set in env )
	// Shell script path in fs
	private String appJarPath = "";
	// Timestamp needed for creating a local resource
	private long appJarTimestamp = 0;
	// File length needed for local resource
	private long appJarPathLen = 0;

	// Configuration
	private Configuration conf;

	public ApplicationMaster() {
		// Set up the configuration
		conf = new YarnConfiguration();
	}

	/**
	 * Parse command line options
	 *
	 * @param args
	 *            Command line args
	 * @return Whether init successful and run should be invoked
	 * @throws org.apache.commons.cli.ParseException
	 * @throws java.io.IOException
	 */
	public boolean init(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
		opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
		 opts.addOption("container_memory", true,
		 "Amount of memory in MB to be requested to run the shell command");
		 opts.addOption("container_vcores", true,
		 "Amount of virtual cores to be requested to run the shell command");
		opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("help", false, "Print usage");

		CommandLine cliParser = new GnuParser().parse(opts, args);

		Map<String, String> envs = System.getenv();

		if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException("Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils
					.toContainerId(envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
		}
		if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
			throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name() + " not set in the environment");
		}
		if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT + " not set in the environment");
		}
		if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
			throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name() + " not set in the environment");
		}

		if (envs.containsKey(Constants.AM_JAR_PATH)) {
			appJarPath = envs.get(Constants.AM_JAR_PATH);

			if (envs.containsKey(Constants.AM_JAR_TIMESTAMP)) {
				appJarTimestamp = Long.valueOf(envs.get(Constants.AM_JAR_TIMESTAMP));
			}
			if (envs.containsKey(Constants.AM_JAR_LENGTH)) {
				appJarPathLen = Long.valueOf(envs.get(Constants.AM_JAR_LENGTH));
			}

			if (!appJarPath.isEmpty() && (appJarTimestamp <= 0 || appJarPathLen <= 0)) {
				throw new IllegalArgumentException("Illegal values in env for shell script path");
			}
		}

		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException("Cannot run MyAppliCationMaster with no containers");
		}
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

		return true;
	}

	/**
	 * Main run function for the application master
	 *
	 * @throws org.apache.hadoop.yarn.exceptions.YarnException
	 * @throws java.io.IOException
	 */
	@SuppressWarnings({ "unchecked" })
	public void run() throws Exception {

		// Initialize clients to ResourceManager and NodeManagers
		AMRMClient<ContainerRequest> amRMClient = AMRMClient.createAMRMClient();
		amRMClient.init(conf);
		amRMClient.start();

		// Register with ResourceManager
		amRMClient.registerApplicationMaster("", 0, "");

		// Set up resource type requirements for Container
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(containerMemory);
		capability.setVirtualCores(containerVirtualCores);

		// Priority for worker containers - priorities are intra-application
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(requestPriority);

		// Make container requests to ResourceManager
		for (int i = 0; i < numTotalContainers; ++i) {
			ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
			amRMClient.addContainerRequest(containerAsk);
		}

		NMClient nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();

		// Setup CLASSPATH for Container
		Map<String, String> containerEnv = new HashMap<String, String>();
		containerEnv.put("CLASSPATH", "./*");

		// Setup ApplicationMaster jar file for Container
		LocalResource appMasterJar = createAppMasterJar();

		// Obtain allocated containers and launch
		int allocatedContainers = 0;
		// We need to start counting completed containers while still allocating
		// them since initial ones may complete while we're allocating
		// subsequent
		// containers and if we miss those notifications, we'll never see them
		// again
		// and this ApplicationMaster will hang indefinitely.
		int completedContainers = 0;
		while (allocatedContainers < numTotalContainers) {
			AllocateResponse response = amRMClient.allocate(0);
			for (Container container : response.getAllocatedContainers()) {
				allocatedContainers++;

				// Get amount of all the lines in the file.
				linesCount = TextLogic.linesCount(Constants.INPUT_FILE);

				ContainerLaunchContext appContainer = createContainerLaunchContext(appMasterJar, containerEnv,
						allocatedContainers);
				nmClient.startContainer(container, appContainer);
			}
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
			}
			Thread.sleep(100);
		}

		// Now wait for the remaining containers to complete
		while (completedContainers < numTotalContainers) {
			AllocateResponse response = amRMClient.allocate(completedContainers / numTotalContainers);
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
			}
			Thread.sleep(100);
		}

		// Un-register with ResourceManager
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}

	private LocalResource createAppMasterJar() throws IOException {
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		if (!appJarPath.isEmpty()) {
			appMasterJar.setType(LocalResourceType.FILE);
			Path jarPath = new Path(appJarPath);
			jarPath = FileSystem.get(conf).makeQualified(jarPath);
			appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
			appMasterJar.setTimestamp(appJarTimestamp);
			appMasterJar.setSize(appJarPathLen);
			appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
		}
		return appMasterJar;
	}

	/**
	 * Launch container by create ContainerLaunchContext
	 * 
	 * @param appMasterJar
	 * @param containerEnv
	 * @param allocatedContainers
	 * @return
	 */
	private ContainerLaunchContext createContainerLaunchContext(LocalResource appMasterJar,
			Map<String, String> containerEnv, int allocatedContainers) {
		long count, offset;
		
		count = getCount(linesCount, allocatedContainers, numTotalContainers);
		offset = getOffset(linesCount, allocatedContainers, numTotalContainers);
				
		ContainerLaunchContext appContainer = Records.newRecord(ContainerLaunchContext.class);
		appContainer.setLocalResources(Collections.singletonMap(Constants.AM_JAR_NAME, appMasterJar));
		appContainer.setEnvironment(containerEnv);
		appContainer.setCommands(Collections
				.singletonList("$JAVA_HOME/bin/java" + " -Xmx256M" + " com.epam.bigdata.q3.task2.yarn_app.YarnMain "
						+ offset + " " + count + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

		return appContainer;
	}
	
	/**
	 * Get count of lines from the file for certain container.
	 * 
	 * @param linesCount
	 * @param allocatedContainers
	 * @param numTotalContainers
	 * @return
	 */
	private long getCount(long linesCount, int allocatedContainers, int numTotalContainers) {
		long count;
		
		if (allocatedContainers < numTotalContainers) {
			count = Math.round(linesCount / numTotalContainers);
		} else {
			count = linesCount - Math.round(linesCount / numTotalContainers) * (numTotalContainers - 1);
		}
		return count;
	}
	
	/**
	 * Get offset of lines from the file for certain container.
	 * 
	 * @param linesCount
	 * @param allocatedContainers
	 * @param numTotalContainers
	 * @return
	 */
	private long getOffset(long linesCount, int allocatedContainers, int numTotalContainers) {		
		return Math.round(linesCount / numTotalContainers) * (allocatedContainers - 1);
	}
	


	public static void main(String[] args) throws Exception {
		try {
			ApplicationMaster appMaster = new ApplicationMaster();
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			appMaster.run();
		} catch (Throwable t) {
			LOG.fatal("Error running ApplicationMaster", t);
			LogManager.shutdown();
			ExitUtil.terminate(1, t);
		}
	}
}
