package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PlantPgeAnalytics extends SDICursorRow {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String analytic_name;

	private Float analytic_value;

	public PlantPgeAnalytics(long Perm_ID, String analytic_name,
			Float analytic_value)

	{
		super(Perm_ID);
		this.analytic_name = analytic_name;
		this.analytic_value = analytic_value;
	}

	public static PlantPgeAnalytics LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID= RS.getLong("perm_id");
		String analytic_name=RS.getString("analytic_name");
		Float analytic_value=RS.getFloat("analytic_value");
		if(RS.wasNull())
		{
			analytic_value=null;
		}
		
		return new PlantPgeAnalytics(Perm_ID, analytic_name,analytic_value);

	}

	public String getAnalytic_name() {
		return analytic_name;
	}

	public Float getAnalytic_value() {
		return analytic_value;
	}
	
	
}
