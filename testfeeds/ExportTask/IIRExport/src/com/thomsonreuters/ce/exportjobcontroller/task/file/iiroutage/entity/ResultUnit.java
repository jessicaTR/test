/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity;

import java.util.Date;
import java.util.List;

/**
 * @author lei.yang
 *
 */
public class ResultUnit {
	private String ric;
	private String analyticTypeName;
	private Date startDate;
	private Date endDate;
	private List<DataUnit> lstDU;
	
	public String getRic() {
		return ric;
	}
	public void setRic(String ric) {
		this.ric = ric;
	}
	public String getAnalyticTypeName() {
		return analyticTypeName;
	}
	public void setAnalyticTypeName(String analyticTypeName) {
		this.analyticTypeName = analyticTypeName;
	}
	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public List<DataUnit> getLstDU() {
		return lstDU;
	}
	public void setLstDU(List<DataUnit> lstDU) {
		this.lstDU = lstDU;
	}

}
