package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class VesselLocationFact extends SDICursorRow {
	
	private static final long serialVersionUID = 1L;
	private String fact_type;
	private String fact_value;
	private Date entry_time;
	private Date action_time;
	private String is_latest;
	
	public VesselLocationFact(long Perm_ID,String fact_type,String fact_value,Date entry_time,String is_latest,Date action_time) {
		super(Perm_ID);
		this.fact_type = fact_type;
		this.fact_value = fact_value;
		this.entry_time = entry_time;
		this.is_latest = is_latest;
		this.action_time = action_time;
	}
	
	public static VesselLocationFact LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		String fact_type = RS.getString("fact_type");
		String fact_value = RS.getString("fact_value");
		Date entry_time=null;
		Timestamp Ts=RS.getTimestamp("entry_time");
		if (Ts!=null)
		{
			entry_time = new Date(Ts.getTime());
		}
		
		String is_latest = RS.getString("is_latest");
		
		Date action_time=null;
		Timestamp actTs=RS.getTimestamp("action_time");
		if (actTs!=null)
		{
			action_time = new Date(actTs.getTime());
		}
		
		return new VesselLocationFact(Perm_id,fact_type,fact_value,entry_time,is_latest,action_time);
	}
	
	public String getFact_type() {
		return fact_type;
	}
	
	public String getFact_value() {
		return fact_value;
	}
	
	public Date getEntry_time() {
		return entry_time;
	}

	public String getIs_latest() {
		return is_latest;
	}
	
	public Date getAction_time() {
		return action_time;
	}
	

}
