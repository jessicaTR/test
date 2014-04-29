/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity;

import java.util.Date;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.IIRTSProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.ConcurrentDateUtil;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.DataType;

/**
 * @author lei.yang
 *
 */
public class DataUnit extends DateRegion{
	private Long permId;
	private DataType dataType;	
	private Long value;	
	private static Logger logger = Starter.getLogger(IIRTSProcessor.SERVICE_NAME);
	private boolean validDUM = true;
	
	public DataUnit(){
		
	}
	
	public DataUnit(DataUnit dataUnit){		
		this.permId = dataUnit.getPermId();
		this.dataType = dataUnit.getDataType();
		this.startDate = dataUnit.getStartDate();
		this.endDate = dataUnit.getEndDate();
		this.value = dataUnit.getValue();
	}
	
	public Long getPermId() {
		return permId;
	}
	public void setPermId(Long permId) {
		this.permId = permId;
	}
	public DataType getDataType() {
		return dataType;
	}
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	
	public Long getValue() {
		return (Long)value;
	}
	public void setValue(Long value) {
		this.value = value;
	}
	

	/**
	 * @return the validDUM
	 */
	public boolean isValidDUM() {
		return validDUM;
	}

	/**
	 * @param validDUM the validDUM to set
	 */
	public void setValidDUM(boolean validDUM) {
		this.validDUM = validDUM;
	}

	@Override
	public String toString() {
		String result = null;
		String strStartDate = null;
		String strEndDate = null;
		try{
			if (this.startDate != null)
				strStartDate = ConcurrentDateUtil.formatToyMdDash(this.startDate);
			if (this.endDate != null){
				strEndDate = ConcurrentDateUtil.formatToyMdDash(this.endDate);
			}
			result = "{permId:" + this.permId + "; dataType:" + this.dataType + 
					"; startDate:" + strStartDate + "; endDate:" + strEndDate + "; value:" + this.value + "}";			
		}catch(Exception ex){
			logger.warn("Failed to translate Object DataUnit with permId: " + this.permId + "to String");
		}
		
		return result;
	}
	
	public boolean isValidDV(DataType dataType) {
		if (dataType.equals(DataType.INSTALL_CAPACITY)
				&& (this.value < 0L || this.startDate == null || (this.endDate != null && this.startDate.after(this.endDate)))){
			return false;
		}
		if (dataType.equals(DataType.OUTAGE) 
				&& (this.startDate == null || this.endDate == null || this.startDate.after(this.endDate))){
			return false;
		}
		return true;
	}
	
	public boolean inRegion(Date from, Date to){		
		if ((this.startDate.equals(from) || this.startDate.after(from))
				&& (this.endDate.equals(to) || this.endDate.before(to))){
			return true;
		} else
			return false;		
	}
	
	public boolean isOverlap(Date from, Date to){
		if (this.startDate.equals(from) 
				|| this.startDate.after(from) && this.startDate.before(to) 
				|| this.startDate.equals(to)
				|| this.endDate.equals(from)
				|| this.endDate.after(from) && this.endDate.before(to)
				|| this.endDate.equals(to))
			return true;
		else 
			return false;
	}
	
	
	public boolean cover(Long permId, Date date){
		return(permId.compareTo(this.permId) == 0 && !(date.before(this.getStartDate()) || (this.getEndDate() != null && date.after(this.getEndDate()))));
		
	}
		
}
