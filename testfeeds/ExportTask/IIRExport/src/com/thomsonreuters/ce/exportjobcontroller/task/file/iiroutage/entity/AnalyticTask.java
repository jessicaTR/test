/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity;

import java.util.Date;
import java.util.List;

import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.ConcurrentDateUtil;

/**
 * @author lei.yang
 * 
 */
public class AnalyticTask extends Entity {

	private List<Long> lstPermId;
	private Date logTime;

	public AnalyticTask() {
	}

	public AnalyticTask(AnalyticTask task) {
		this.rreId = task.getRreId();
		this.ric = task.getRic();
		this.analyticTypeId = task.getAnalyticTypeId();
		this.analyticTypeName = task.getAnalyticTypeName();
		this.startDate = task.getStartDate();
		this.endDate = task.getEndDate();
		this.lstPermId = task.getLstPermId();
	}

	public List<Long> getLstPermId() {
		return lstPermId;
	}

	public void setLstPermId(List<Long> lstPermId) {
		this.lstPermId = lstPermId;
	}

	public Date getLogTime() {
		return logTime;
	}

	public void setLogTime(Date logTime) {
		this.logTime = logTime;
	}

	public String toString() {
		String strRreId = "";
		String strRic = "";
		String anaTypeName = "";
		String strStartDate = "";
		String strEndDate = "";
		String permIds = "";
		if (this.rreId != null) {
			strRreId = this.rreId.toString();
		}
		if (this.ric != null) {
			strRic = this.ric;
		}
		if (this.analyticTypeName != null) {
			anaTypeName = this.analyticTypeName.toString();
		}
		if (this.startDate != null) {
			strStartDate = ConcurrentDateUtil.formatToyMdDash(this.startDate);
		}
		if (this.endDate != null) {
			strEndDate = ConcurrentDateUtil.formatToyMdDash(this.endDate);
		}
		if (!(this.lstPermId == null || this.lstPermId.isEmpty())) {
			permIds = this.lstPermId.toString();
		}
		return "rreId: " + strRreId + "; ric: " + strRic
				+ "; analyticTypeName: " + anaTypeName + "; startDate: "
				+ strStartDate + "; endDate: " + strEndDate + "; permdIds: "
				+ permIds;
	}

}
