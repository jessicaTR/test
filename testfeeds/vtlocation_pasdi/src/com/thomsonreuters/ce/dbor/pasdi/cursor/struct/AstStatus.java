package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AstStatus extends SDICursorRow {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String status_type;
	private String status_value;
	
	
	public AstStatus(long Perm_ID, String status_type, String status_value) {
		super(Perm_ID);
		this.status_type = status_type;
		this.status_value = status_value;
	}

	public static AstStatus LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID=RS.getLong("PERM_ID");
		String status_type=RS.getString("status_type");
		String status_value=RS.getString("status_value");
		
		return new AstStatus(Perm_ID, status_type, status_value);
		
	}

	public String getStatus_type() {
		return status_type;
	}


	public String getStatus_value() {
		return status_value;
	}
	
	
	
}
