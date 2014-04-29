package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PlantPgeStatistic extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String statistic_type;
	private String statistic_period_type;
	private String statistic_year;
	private Float statistic_value;
	private String statistic_measurement;
	private String statistic_validity_type;
	private String statistic_data_source;
	
	
	public PlantPgeStatistic(long Perm_ID, String statistic_type, String statistic_period_type, String statistic_year, Float statistic_value, String statistic_measurement, String statistic_validity_type, String statistic_data_source) {
		super(Perm_ID);
		this.statistic_type = statistic_type;
		this.statistic_period_type = statistic_period_type;
		this.statistic_year = statistic_year;
		this.statistic_value = statistic_value;
		this.statistic_measurement = statistic_measurement;
		this.statistic_validity_type = statistic_validity_type;
		this.statistic_data_source = statistic_data_source;
	}

	public static PlantPgeStatistic LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID= RS.getLong("perm_id");
		String statistic_type=RS.getString("statistic_type");
		String statistic_period_type=RS.getString("statistic_period_type");
		String statistic_year=RS.getString("statistic_year");
		Float statistic_value=RS.getFloat("statistic_value");
		if(RS.wasNull())
		{
			statistic_value=null;
		}
		
		String statistic_measurement=RS.getString("statistic_measurement");
		String statistic_validity_type=RS.getString("statistic_validity_type");
		String statistic_data_source=RS.getString("statistic_data_source");
		
		return new PlantPgeStatistic( Perm_ID,  statistic_type,  statistic_period_type,  statistic_year,  statistic_value,  statistic_measurement,  statistic_validity_type,  statistic_data_source);
	}
	
	
	public String getStatistic_data_source() {
		return statistic_data_source;
	}


	public String getStatistic_measurement() {
		return statistic_measurement;
	}


	public String getStatistic_period_type() {
		return statistic_period_type;
	}


	public String getStatistic_type() {
		return statistic_type;
	}


	public String getStatistic_validity_type() {
		return statistic_validity_type;
	}


	public Float getStatistic_value() {
		return statistic_value;
	}


	public String getStatistic_year() {
		return statistic_year;
	}
	
	

}
