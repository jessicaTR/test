package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class AstName extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String name_type;
	private String name_value;
	private Date effective_from;
	private Date effective_to;
	
	
	public AstName(long Perm_ID, String name_type, String name_value, Date effective_from,Date effective_to) {
		super(Perm_ID);
		this.name_type = name_type;
		this.name_value = name_value;
		this.effective_from = effective_from;
		this.effective_to = effective_to;
	}

	public static AstName LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID=RS.getLong("PERM_ID");
		String name_type=RS.getString("name_type");
		String name_value=RS.getString("name_value");
		Date effective_from =null;
		Timestamp Ts=RS.getTimestamp("effective_from");
		if (Ts!=null)
		{
			effective_from = new Date(Ts.getTime());
		}
		
		Date effective_to =null;
		Ts=RS.getTimestamp("effective_to");
		if (Ts!=null)
		{
			effective_to = new Date(Ts.getTime());
		}
		
		return new AstName(Perm_ID, name_type, name_value, effective_from, effective_to);
	}	
	
	public Date getEffective_from() {
		return effective_from;
	}
	
	public Date getEffective_to() {
		return effective_to;
	}


	public String getName_type() {
		return name_type;
	}


	public String getName_value() {
		return name_value;
	}
	
}
