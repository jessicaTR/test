/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity;

import java.util.Date;

import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.ConcurrentDateUtil;

/**
 * @author lei.yang
 * 
 */
public class DateRegion implements Comparable<DateRegion> {

	protected Date startDate;
	protected Date endDate;

	public DateRegion() {
	}

	public DateRegion(Date startDate, Date endDate) {
		this.startDate = startDate;
		this.endDate = endDate;
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
	
	public String toString(){		
		return ConcurrentDateUtil.formatToyMdDash(this.startDate) + " : " + ConcurrentDateUtil.formatToyMdDash(this.endDate);
	}

	@Override
	public int compareTo(DateRegion region) {
		if (this.startDate.equals(region.getStartDate())
				&& this.endDate.equals(region.getEndDate()))
			return 0;
		else if (this.startDate.before(region.getStartDate())
				|| (this.startDate.equals(region.getStartDate()) && this.endDate.before(region.getEndDate())))
			return -1;
		else 
			return 1;

	}
}
