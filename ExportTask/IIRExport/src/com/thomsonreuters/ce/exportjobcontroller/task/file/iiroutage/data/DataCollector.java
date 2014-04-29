/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.data;

import java.util.List;

import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.AnalyticTask;

/**
 * @author lei.yang
 * 
 */
public class DataCollector implements Runnable{
	
	private List<AnalyticTask> analyticTasks;
	

	public List<AnalyticTask> getAnalyticTasks() {
		return analyticTasks;
	}

	public void setAnalyticTasks(List<AnalyticTask> analyticTasks) {
		this.analyticTasks = analyticTasks;
	}

	@Override
	public void run() {
		
		
	}

}
