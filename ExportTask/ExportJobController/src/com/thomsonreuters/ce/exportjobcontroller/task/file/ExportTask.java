package com.thomsonreuters.ce.exportjobcontroller.task.file;

import java.util.Date;

public class ExportTask {

	protected int id;
	protected int taskDefId;
//	protected Date startDate;
//	protected Date endDate;
	protected String taskStatus;
	protected String taskRunType; // full or incremental
//	protected Date processBeginTime;
//	protected Date processEndTime;
	
	public ExportTask(int id, int taskDefId, String taskRunType) {
		this.id = id;
		this.taskDefId = taskDefId;		
		this.taskRunType = taskRunType;
	}

	public ExportTask(int id, int taskDefId, Date startDate, Date endDate,
			String taskRunType) {
		this.id = id;
		this.taskDefId = taskDefId;
//		this.startDate = startDate;
//		this.endDate = endDate;
		this.taskRunType = taskRunType;
	}


	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
	}


	public int getTaskDefId() {
		return taskDefId;
	}


	public void setTaskDefId(int taskDefId) {
		this.taskDefId = taskDefId;
	}


//	public Date getStartDate() {
//		return startDate;
//	}
//
//
//	public void setStartDate(Date startDate) {
//		this.startDate = startDate;
//	}
//
//
//	public Date getEndDate() {
//		return endDate;
//	}
//
//
//	public void setEndDate(Date endDate) {
//		this.endDate = endDate;
//	}


	public String getTaskStatus() {
		return taskStatus;
	}


	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}


	public String getTaskRunType() {
		return taskRunType;
	}


	public void setTaskRunType(String taskRunType) {
		this.taskRunType = taskRunType;
	}


//	public Date getProcessBeginTime() {
//		return processBeginTime;
//	}
//
//
//	public void setProcessBeginTime(Date processBeginTime) {
//		this.processBeginTime = processBeginTime;
//	}
//
//
//	public Date getProcessEndTime() {
//		return processEndTime;
//	}
//
//
//	public void setProcessEndTime(Date processEndTime) {
//		this.processEndTime = processEndTime;
//	}
	
	
}
