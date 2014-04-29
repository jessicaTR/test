package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class VesselEventOriginDestination extends SDICursorRow {
	
	private String cur_name;
	private String zone_type;
	private Long zone_id;
	private String zone_raw_value;
	private Date relation_effective_from;
	private Date entry_time;
	private Date out_time;
	
	public VesselEventOriginDestination(long Perm_ID, String cur_name, String zone_type, Long zone_id, String zone_raw_value, Date relation_effective_from, Date entry_time, Date out_time) {
		super(Perm_ID);
		this.cur_name = cur_name;
		this.zone_type = zone_type;
		this.zone_id = zone_id;
		this.zone_raw_value = zone_raw_value;
		this.relation_effective_from = relation_effective_from;
		this.entry_time = entry_time;
		this.out_time = out_time;
	}

	public String getCur_name() {
		return cur_name;
	}

	public Date getEntry_time() {
		return entry_time;
	}

	public Date getOut_time() {
		return out_time;
	}

	public Date getRelation_effective_from() {
		return relation_effective_from;
	}

	public Long getZone_id() {
		return zone_id;
	}

	public String getZone_raw_value() {
		return zone_raw_value;
	}

	public String getZone_type() {
		return zone_type;
	}
	
	public static VesselEventOriginDestination LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		
		String cur_name=RS.getString("cur_name");
		String zone_type=RS.getString("zone_type");
		
		Long zone_id=RS.getLong("zone_id");
		if(RS.wasNull())
		{
			zone_id=null;
		}
		
		String zone_raw_value=RS.getString("zone_raw_value");
		
		Date relation_effective_from =null;
		Timestamp Ts=RS.getTimestamp("relation_effective_from");
		if (Ts!=null)
		{
			relation_effective_from = new Date(Ts.getTime());
		}
		
		Date entry_time =null;
		Ts=RS.getTimestamp("entry_time");
		if (Ts!=null)
		{
			entry_time = new Date(Ts.getTime());
		}
		
		Date out_time =null;
		Ts=RS.getTimestamp("out_time");
		if (Ts!=null)
		{
			out_time = new Date(Ts.getTime());
		}		
		
		
		
		return new VesselEventOriginDestination(Perm_id, cur_name, zone_type, zone_id, zone_raw_value, relation_effective_from, entry_time, out_time);
		
	}
}
