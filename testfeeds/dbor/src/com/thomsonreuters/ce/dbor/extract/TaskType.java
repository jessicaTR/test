package com.thomsonreuters.ce.dbor.extract;

public class TaskType {

	private String Name;
	private long Interval;
	private String thisProcessor;
	
	
	public TaskType(String name,long interval,String processor)
	{
		this.Name=name;
		this.Interval=interval;
		this.thisProcessor=processor;
	}

	public long getInterval() {
		return Interval;
	}

	public String getName() {
		return Name;
	}

	public String getTaskProcessor() {
		return this.thisProcessor;
	}
}
