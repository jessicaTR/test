package com.thomsonreuters.ce.dbor.extract;

import java.sql.Date;

public class Task implements QueueObject {

	private int tad_id;
	private Date parm_start;
	private Date parm_end;
	private String status;
	private String task_type;
	private int id;

	public Task(int id, int tad_id, Date parm_start, Date parm_end,
			String task_type) {
		this.id = id;
		this.tad_id = tad_id;
		this.parm_start = parm_start;
		this.parm_end = parm_end;
		this.task_type = task_type;
	}

	public int getId() {
		return this.id;
	}

	public int getTad() {
		return this.tad_id;
	}

	public String getTaskType() {
		return this.task_type;
	}

	public Date getParmStart() {
		return this.parm_start;
	}

	public Date getParmEnd() {
		return this.parm_end;
	}

	public String getStatus() {
		return this.status;
	}

	@Override
	public Date getStartDate() {
		return this.parm_start;
	}

	@Override
	public Date getEndDate() {
		return this.parm_end;
	}
	
	public void setStarttime(Date starttime) {
		this.parm_start = starttime;
	}

	public void setEndtime(Date endtime) {
		this.parm_end = endtime;
	}
}
