package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PlantCommonStatus extends SDICursorRow
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String status_type;
	private String status_type_value;
	private String status_note;
	private String status_data_source;
	
	public PlantCommonStatus(long Perm_ID, String status_type, String status_type_value, String status_note, String status_data_source) {
		super(Perm_ID);
		this.status_type = status_type;
		this.status_type_value = status_type_value;
		this.status_note = status_note;
		this.status_data_source = status_data_source;
	}

	public static PlantCommonStatus LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long perm_id=RS.getLong("PERM_ID");
		String status_type = RS.getString("status_type");
		String status_type_value=RS.getString("status_type_value");		
		String status_note=RS.getString("status_note");
		String status_data_source=RS.getString("status_data_source");

		
		return new PlantCommonStatus(perm_id, status_type, status_type_value, status_note, status_data_source);
		
	}
	
	public String getStatus_data_source() {
		return status_data_source;
	}

	public String getStatus_note() {
		return status_note;
	}

	public String getStatus_type() {
		return status_type;
	}

	public String getStatus_type_value() {
		return status_type_value;
	}
	
	

}
