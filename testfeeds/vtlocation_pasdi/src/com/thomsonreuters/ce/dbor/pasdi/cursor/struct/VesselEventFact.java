package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.IDENTIFIER_VALUE_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.RELATION_OBJECT_ID_TYPE;

public class VesselEventFact extends SDICursorRow {

	private static final long serialVersionUID = 1L;
	private String zone_role;
	private String zone_type;
	private String is_latest;
	private RELATION_OBJECT_ID_TYPE zone_perm_id;
	private IDENTIFIER_VALUE_TYPE zone_identifier ;
	private String zone_code;
	private String zone_name;
	private Date entry_time;
	private Date out_time;
	private Date action_time;

	
	public VesselEventFact(long Perm_ID,String zone_role,String zone_type,String is_latest,RELATION_OBJECT_ID_TYPE zone_perm_id,IDENTIFIER_VALUE_TYPE zone_identifier,String zone_code,String zone_name,Date entry_time,Date out_time,Date action_time) {
		super(Perm_ID);
		this.zone_role = zone_role;
		this.zone_type = zone_type;
		this.is_latest = is_latest;
		this.zone_perm_id=zone_perm_id;
		this.zone_identifier = zone_identifier;
		this.zone_code = zone_code;
		this.zone_name = zone_name;
		this.entry_time = entry_time;
		this.out_time = out_time;
		this.action_time = action_time;
	}

	public static VesselEventFact LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		String zone_role = RS.getString("zone_role");
		String zone_type=RS.getString("zone_type");
		String is_latest = RS.getString("is_latest");
		RELATION_OBJECT_ID_TYPE zone_perm_id= null;
		IDENTIFIER_VALUE_TYPE zone_identifier = null;
		try{
			zone_perm_id=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)RS.getObject("zone_perm_id")).getAttributes());
		}catch(NullPointerException e){
			zone_perm_id=null;
		}
		if(RS.wasNull())
		{
			zone_perm_id=null;
		}
		try{
			zone_identifier = new IDENTIFIER_VALUE_TYPE(((oracle.sql.STRUCT)RS.getObject("zone_identifier")).getAttributes());
		}catch(NullPointerException e){
			zone_identifier=null;
		}
		
		if(RS.wasNull())
		{
			zone_identifier=null;
		}
		String zone_code=RS.getString("zone_code");		
		String zone_name=RS.getString("zone_name");
		Date entry_time=null;
		Timestamp Ts=RS.getTimestamp("entry_time");
		if (Ts!=null)
		{
			entry_time = new Date(Ts.getTime());
		}
		
		Date out_time=null;
		Ts=RS.getTimestamp("out_time");
		if (Ts!=null)
		{
			out_time = new Date(Ts.getTime());
		}
		
		Date action_time=null;
		Ts=RS.getTimestamp("action_time");
		if (Ts!=null)
		{
			action_time = new Date(Ts.getTime());
		}
		return new VesselEventFact(Perm_id, zone_role,zone_type, is_latest, zone_perm_id, zone_identifier,zone_code,zone_name,entry_time, out_time, action_time);
	
	}
	
	public String getZone_role() {
		return zone_role;
	}
	
	public String getZone_type() {
		return zone_type;
	}
	
	public String getIs_latst() {
		return is_latest;
	}
	
	public RELATION_OBJECT_ID_TYPE getZone_perm_id() {
		return zone_perm_id;
	}
	
	public IDENTIFIER_VALUE_TYPE  getZone_identifier() {
		return zone_identifier;
	}
	
	public String getZone_code() {
		return zone_code;
	}
	
	public String getZone_name() {
		return zone_name;
	}
	
	public Date getEntry_time() {
		return entry_time;
	}

	public Date getOut_time() {
		return out_time;
	}
	
	public Date getAction_time() {
		return action_time;
	}

}
