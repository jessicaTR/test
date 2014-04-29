package com.thomsonreuters.ce.exportjobcontroller.task.file;

import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.dbor.extract.TaskType;
import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;

public class JobLoader extends ControlledThread {

	private Logger logger;
	private Properties prop;

	public JobLoader(ThreadController tc, Properties prop, Logger logger) {
		super(tc);
		this.logger = logger;
		this.prop = prop;
	}

	public void ControlledProcess() {
		try {
			String TaskNames = prop.getProperty("TaskTypes");
			StringTokenizer TaskNamesList = new StringTokenizer(TaskNames, ",",
					false);
			ArrayList<String> taskNames = new ArrayList<String>();
			while (TaskNamesList.hasMoreTokens()) {
				String TaskName = TaskNamesList.nextToken().trim();
				if (taskNames.contains(TaskName))
					continue;
				taskNames.add(TaskName);

				logger.debug("Task Name: " + TaskName);
				long Interval = Long.parseLong(prop.getProperty(TaskName
						+ ".checkinterval"));
				logger.debug("Check Interval: " + Interval);
				String ClassName = prop
						.getProperty(TaskName + ".fileprocessor");
				logger.debug("File Processor: " + ClassName);
				TaskType tasktype = new TaskType(TaskName, Interval, ClassName);
				TaskScanner newScanner = new TaskScanner(tasktype, TC);
				Starter.TimerService.createTimer(0, 0, newScanner);
			}

			logger.debug("Loading datasets is done !");

		} catch (Exception e) {
			logger.fatal("Error happened!", e);
		}

	}

}
