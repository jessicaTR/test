package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PlantPlaCoalMine extends SDICursorRow {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer coal_mine_id;
	private String coal_mine_coal_field;
	private String coal_mine_type;
	private String coal_mine_province;
	private Integer coal_mine_seams;
	private Float coal_mine_seam_thickness;
	private Float coal_mine_total_thickness;
	private Float coal_mine_strip_ratio;
	private Integer coal_mine_life;
	private Integer coal_mine_opened;
	private Integer coal_mine_close;
	private Float coal_mine_capacity;
	private String coal_mine_data_source;
	private String coal_mine_coal_rank;
	private String coal_mine_coal_volatility;
	private String coal_mine_coal_use;
	
	


	public PlantPlaCoalMine(long Perm_ID, Integer coal_mine_id, String coal_mine_coal_field, String coal_mine_type, String coal_mine_province, Integer coal_mine_seams, Float coal_mine_seam_thickness, Float coal_mine_total_thickness, Float coal_mine_strip_ratio, Integer coal_mine_life, Integer coal_mine_opened, Integer coal_mine_close, Float coal_mine_capacity, String coal_mine_data_source, String coal_mine_coal_rank, String coal_mine_coal_volatility, String coal_mine_coal_use) {
		super(Perm_ID);
		this.coal_mine_id = coal_mine_id;
		this.coal_mine_coal_field = coal_mine_coal_field;
		this.coal_mine_type = coal_mine_type;
		this.coal_mine_province = coal_mine_province;
		this.coal_mine_seams = coal_mine_seams;
		this.coal_mine_seam_thickness = coal_mine_seam_thickness;
		this.coal_mine_total_thickness = coal_mine_total_thickness;
		this.coal_mine_strip_ratio = coal_mine_strip_ratio;
		this.coal_mine_life = coal_mine_life;
		this.coal_mine_opened = coal_mine_opened;
		this.coal_mine_close = coal_mine_close;
		this.coal_mine_capacity = coal_mine_capacity;
		this.coal_mine_data_source = coal_mine_data_source;
		this.coal_mine_coal_rank = coal_mine_coal_rank;
		this.coal_mine_coal_volatility = coal_mine_coal_volatility;
		this.coal_mine_coal_use = coal_mine_coal_use;
	}




	public static PlantPlaCoalMine LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		
		Integer coal_mine_id=RS.getInt("coal_mine_id");
		if(RS.wasNull())
		{
			coal_mine_id=null;
		}
		
		
		String coal_mine_coal_field=RS.getString("coal_mine_coal_field");
		String coal_mine_type=RS.getString("coal_mine_type");
		String coal_mine_province=RS.getString("coal_mine_province");
		Integer coal_mine_seams=RS.getInt("coal_mine_seams");
		if(RS.wasNull())
		{
			coal_mine_seams=null;
		}
		
		Float coal_mine_seam_thickness=RS.getFloat("coal_mine_seam_thickness");
		if(RS.wasNull())
		{
			coal_mine_seam_thickness=null;
		}
		
		Float coal_mine_total_thickness=RS.getFloat("coal_mine_total_thickness");
		if(RS.wasNull())
		{
			coal_mine_total_thickness=null;
		}
		
		Float coal_mine_strip_ratio=RS.getFloat("coal_mine_strip_ratio");
		if(RS.wasNull())
		{
			coal_mine_strip_ratio=null;
		}
		
		Integer coal_mine_life=RS.getInt("coal_mine_life");
		if(RS.wasNull())
		{
			coal_mine_life=null;
		}
		
		Integer coal_mine_opened=RS.getInt("coal_mine_opened");
		if(RS.wasNull())
		{
			coal_mine_opened=null;
		}
		
		Integer coal_mine_close=RS.getInt("coal_mine_close");
		if(RS.wasNull())
		{
			coal_mine_close=null;
		}
		
		Float coal_mine_capacity=RS.getFloat("coal_mine_capacity");
		if(RS.wasNull())
		{
			coal_mine_capacity=null;
		}
		
		String coal_mine_data_source=RS.getString("coal_mine_data_source");
		String coal_mine_coal_rank=RS.getString("coal_mine_coal_rank");
		String coal_mine_coal_volatility=RS.getString("coal_mine_coal_volatility");
		String coal_mine_coal_use=RS.getString("coal_mine_coal_use");
		
		return new PlantPlaCoalMine(Perm_id, coal_mine_id, coal_mine_coal_field, coal_mine_type, coal_mine_province, coal_mine_seams, coal_mine_seam_thickness, coal_mine_total_thickness, coal_mine_strip_ratio, coal_mine_life, coal_mine_opened,  coal_mine_close,  coal_mine_capacity,  coal_mine_data_source,  coal_mine_coal_rank,  coal_mine_coal_volatility,  coal_mine_coal_use);
	}




	public Float getCoal_mine_capacity() {
		return coal_mine_capacity;
	}




	public Integer getCoal_mine_close() {
		return coal_mine_close;
	}




	public String getCoal_mine_coal_field() {
		return coal_mine_coal_field;
	}




	public String getCoal_mine_coal_rank() {
		return coal_mine_coal_rank;
	}




	public String getCoal_mine_coal_use() {
		return coal_mine_coal_use;
	}




	public String getCoal_mine_coal_volatility() {
		return coal_mine_coal_volatility;
	}




	public String getCoal_mine_data_source() {
		return coal_mine_data_source;
	}




	public Integer getCoal_mine_id() {
		return coal_mine_id;
	}




	public Integer getCoal_mine_life() {
		return coal_mine_life;
	}




	public Integer getCoal_mine_opened() {
		return coal_mine_opened;
	}




	public String getCoal_mine_province() {
		return coal_mine_province;
	}




	public Float getCoal_mine_seam_thickness() {
		return coal_mine_seam_thickness;
	}




	public Integer getCoal_mine_seams() {
		return coal_mine_seams;
	}




	public Float getCoal_mine_strip_ratio() {
		return coal_mine_strip_ratio;
	}




	public Float getCoal_mine_total_thickness() {
		return coal_mine_total_thickness;
	}




	public String getCoal_mine_type() {
		return coal_mine_type;
	}



	
}
