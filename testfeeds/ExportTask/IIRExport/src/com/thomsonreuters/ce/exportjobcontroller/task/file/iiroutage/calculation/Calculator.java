/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.calculation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.AnalyticTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DataUnit;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.DataType;

/**
 * @author lei.yang
 *
 */
public abstract class Calculator {
	protected List<AnalyticTask> analyticTasks;
	protected Map<DataType, ArrayList<DataUnit>> allDVRepository;
	
	
	public List<AnalyticTask> getAnalyticTasks() {
		return analyticTasks;
	}

	public void setAnalyticTasks(List<AnalyticTask> analyticTasks) {
		this.analyticTasks = analyticTasks;
	}

	public Map<DataType, ArrayList<DataUnit>> getAllDVRepository() {
		return allDVRepository;
	}

	public void setAllDVRepository(Map<DataType, ArrayList<DataUnit>> allDVRepository) {
		this.allDVRepository = allDVRepository;
	}
}
