/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity;

import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.AnalyticType;

/**
 * @author lei.yang
 *
 */
public class Entity extends DateRegion {
	
	protected Long rreId;
	protected String ric;
	protected Integer analyticTypeId;
	protected AnalyticType analyticTypeName;
	private Long value;
	
	
	public Long getRreId() {
		return rreId;
	}

	public void setRreId(Long rreId) {
		this.rreId = rreId;
	}

	public String getRic() {
		return ric;
	}

	public void setRic(String ric) {
		this.ric = ric;
	}

	public Integer getAnalyticTypeId() {
		return analyticTypeId;
	}

	public void setAnalyticTypeId(Integer analyticTypeId) {
		this.analyticTypeId = analyticTypeId;
	}
	
	public AnalyticType getAnalyticTypeName() {
		return analyticTypeName;
	}

	public void setAnalyticTypeName(AnalyticType analyticTypeName) {
		this.analyticTypeName = analyticTypeName;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}
	
	
}
