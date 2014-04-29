package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class PlantPlaOutage extends SDICursorRow {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String outage_type;
	private Integer outage_country_id;
	private String outage_reason;
	private Date outage_from;
	private Date outage_to;
	private String outage_unit;
	private Integer outage_level;
	private String outage_confidence_level;
	private String outage_data_source_type;
	private Integer outage_unit_number;
	private Date outage_corrected_date;
	private Date outage_reported_date;
	private String outage_story_code;
	private String outage_latest_change_reason;
	
	

	
	public PlantPlaOutage(long Perm_ID, String outage_type, Integer outage_country_id, String outage_reason, Date outage_from, Date outage_to, String outage_unit, Integer outage_level, String outage_confidence_level, String outage_data_source_type, Integer outage_unit_number, Date outage_corrected_date, Date outage_reported_date, String outage_story_code, String outage_latest_change_reason) {
		super(Perm_ID);
		this.outage_type = outage_type;
		this.outage_country_id = outage_country_id;
		this.outage_reason = outage_reason;
		this.outage_from = outage_from;
		this.outage_to = outage_to;
		this.outage_unit = outage_unit;
		this.outage_level = outage_level;
		this.outage_confidence_level = outage_confidence_level;
		this.outage_data_source_type = outage_data_source_type;
		this.outage_unit_number = outage_unit_number;
		this.outage_corrected_date = outage_corrected_date;
		this.outage_reported_date = outage_reported_date;
		this.outage_story_code = outage_story_code;
		this.outage_latest_change_reason = outage_latest_change_reason;
	}




	public static PlantPlaOutage LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		String outage_type=RS.getString("outage_type");
		Integer outage_country_id=RS.getInt("outage_country_id");
		if(RS.wasNull())
		{
			outage_country_id=null;
		}
		
		String outage_reason=RS.getString("outage_reason");
		Date outage_from=null;
		Timestamp Ts=RS.getTimestamp("outage_from");
		if (Ts!=null)
		{
			outage_from = new Date(Ts.getTime());
		}
		Date outage_to=null;
		Ts=RS.getTimestamp("outage_to");
		if (Ts!=null)
		{
			outage_to = new Date(Ts.getTime());
		}
		String outage_unit=RS.getString("outage_unit");
		Integer outage_level=RS.getInt("outage_level");
		if(RS.wasNull())
		{
			outage_level=null;
		}		
		
		String outage_confidence_level=RS.getString("outage_confidence_level");
		String outage_data_source_type=RS.getString("outage_data_source_type");
		Integer outage_unit_number=RS.getInt("outage_unit_number");
		if(RS.wasNull())
		{
			outage_unit_number=null;
		}		
		
		Date outage_corrected_date=null;
		Ts=RS.getTimestamp("outage_corrected_date");
		if (Ts!=null)
		{
			outage_corrected_date = new Date(Ts.getTime());
		}
		Date outage_reported_date=null;
		Ts=RS.getTimestamp("outage_reported_date");
		if (Ts!=null)
		{
			outage_reported_date = new Date(Ts.getTime());
		}
		String outage_story_code=RS.getString("outage_story_code");
		String outage_latest_change_reason=RS.getString("outage_latest_change_reason");
		
		return new PlantPlaOutage(Perm_id, outage_type, outage_country_id, outage_reason, outage_from, outage_to, outage_unit, outage_level, outage_confidence_level, outage_data_source_type, outage_unit_number, outage_corrected_date, outage_reported_date,  outage_story_code, outage_latest_change_reason);
		
	}




	public String getOutage_confidence_level() {
		return outage_confidence_level;
	}




	public Date getOutage_corrected_date() {
		return outage_corrected_date;
	}




	public Integer getOutage_country_id() {
		return outage_country_id;
	}




	public String getOutage_data_source_type() {
		return outage_data_source_type;
	}




	public Date getOutage_from() {
		return outage_from;
	}




	public String getOutage_latest_change_reason() {
		return outage_latest_change_reason;
	}




	public Integer getOutage_level() {
		return outage_level;
	}




	public String getOutage_reason() {
		return outage_reason;
	}




	public Date getOutage_reported_date() {
		return outage_reported_date;
	}




	public String getOutage_story_code() {
		return outage_story_code;
	}




	public Date getOutage_to() {
		return outage_to;
	}




	public String getOutage_type() {
		return outage_type;
	}




	public String getOutage_unit() {
		return outage_unit;
	}




	public Integer getOutage_unit_number() {
		return outage_unit_number;
	}

	


}
