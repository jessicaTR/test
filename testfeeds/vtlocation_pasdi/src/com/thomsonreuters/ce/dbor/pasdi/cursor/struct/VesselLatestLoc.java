package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class VesselLatestLoc extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String callsign;
	private String destination;
	private String loc_name;
	private Float loc_heading;
	private Float loc_speed;
	private Float loc_draft;
	private Float loc_draft_percentage;
	private Float loc_length;
	private Float loc_width;
	private Date loc_eta;
	private Date loc_timestamp;
	//the order should not be changed
	private String loc_data_source_code;
	private String loc_data_source;
	private String loc_data_source_type;
	
	
	


	public VesselLatestLoc(long Perm_ID, String callsign, String destination, String loc_name, Float loc_heading, Float loc_speed, Float loc_draft, Float loc_length, Float loc_width, Date loc_eta, Date loc_timestamp, String loc_data_source_code, String loc_data_source, String loc_data_source_type, Float loc_draft_percentage) {
		super(Perm_ID);
		this.callsign = callsign;
		this.destination = destination;
		this.loc_name = loc_name;
		this.loc_heading = loc_heading;
		this.loc_speed = loc_speed;
		this.loc_draft = loc_draft;
		this.loc_draft_percentage = loc_draft_percentage;
		this.loc_length = loc_length;
		this.loc_width = loc_width;
		this.loc_eta = loc_eta;
		this.loc_timestamp = loc_timestamp;
		this.loc_data_source_code = loc_data_source_code;
		this.loc_data_source = loc_data_source;
		this.loc_data_source_type = loc_data_source_type;		
	}



	public static VesselLatestLoc LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		String callsign=RS.getString("callsign");
		String destination=RS.getString("destination");
		String loc_name=RS.getString("loc_name");
		Float loc_heading=RS.getFloat("loc_heading");
		if(RS.wasNull())
		{
			loc_heading=null;
		}		
		
		Float loc_speed=RS.getFloat("loc_speed");
		if(RS.wasNull())
		{
			loc_speed=null;
		}		
		
		Float loc_draft=RS.getFloat("loc_draft");
		if(RS.wasNull())
		{
			loc_draft=null;
		}		
		
		Float draft_percentage = RS.getFloat("loc_draft_percentage");
		if (RS.wasNull()){
			draft_percentage = null;
		}
		
		Float loc_length=RS.getFloat("loc_length");
		if(RS.wasNull())
		{
			loc_length=null;
		}		
		
		Float loc_width=RS.getFloat("loc_width");
		if(RS.wasNull())
		{
			loc_width=null;
		}		
		
		Date loc_eta=null;
		Timestamp Ts=RS.getTimestamp("loc_eta");
		if (Ts!=null)
		{
			loc_eta = new Date(Ts.getTime());
		}
		Date loc_timestamp=null;
		Ts=RS.getTimestamp("loc_timestamp");
		if (Ts!=null)
		{
			loc_timestamp = new Date(Ts.getTime());
		}
		String loc_data_source_code=RS.getString("loc_data_source_code");
		String loc_data_source=RS.getString("loc_data_source");
		String loc_data_source_type=RS.getString("loc_data_source_type");
		
		
		
		return new VesselLatestLoc(Perm_id, callsign ,destination, loc_name, loc_heading, loc_speed, loc_draft, loc_length, loc_width, loc_eta, loc_timestamp, loc_data_source_code, loc_data_source, loc_data_source_type, draft_percentage);
	}



	public String getCallsign() {
		return callsign;
	}



	public String getDestination() {
		return destination;
	}



	public String getLoc_data_source() {
		return loc_data_source;
	}



	public String getLoc_data_source_code() {
		return loc_data_source_code;
	}



	public String getLoc_data_source_type() {
		return loc_data_source_type;
	}



	public Float getLoc_draft() {
		return loc_draft;
	}



	public Date getLoc_eta() {
		return loc_eta;
	}



	public Float getLoc_heading() {
		return loc_heading;
	}



	public Float getLoc_length() {
		return loc_length;
	}



	public String getLoc_name() {
		return loc_name;
	}



	public Float getLoc_speed() {
		return loc_speed;
	}



	public Date getLoc_timestamp() {
		return loc_timestamp;
	}



	public Float getLoc_width() {
		return loc_width;
	}



	public Float getLoc_draft_percentage() {
		return loc_draft_percentage;
	}



	public void setLoc_draft_percentage(Float loc_draft_percentage) {
		this.loc_draft_percentage = loc_draft_percentage;
	}





}
