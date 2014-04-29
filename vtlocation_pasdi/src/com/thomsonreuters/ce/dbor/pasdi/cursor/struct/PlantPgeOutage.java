package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class PlantPgeOutage extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Date outage_from;
	private Date outage_to;
	
	public PlantPgeOutage(long Perm_ID, Date outage_from, Date outage_to) {
		super(Perm_ID);
		this.outage_from = outage_from;
		this.outage_to = outage_to;
	}
	
	public static PlantPgeOutage LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID= RS.getLong("perm_id");
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
		
		return new PlantPgeOutage(Perm_ID, outage_from, outage_to);
		
	}

	public Date getOutage_from() {
		return outage_from;
	}

	public Date getOutage_to() {
		return outage_to;
	}
	
	

}
