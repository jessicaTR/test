package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class PlantCommonBase extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String identifier_id;
	private String plant_has_gfms_ts;
	private Date plant_labour_contract_expiry;
	private Date plant_target_date;
	private String plant_data_source;
	private String plant_asset_type_data_source;
	private Long plant_id;
	private Long plant_part_id;
	private Integer cty_id;
	private Integer commodity_group_id;
	private String commodity_group_name;
	private Integer plant_city_id;
	


	public PlantCommonBase(long Perm_ID, String identifier_id,String plant_has_gfms_ts, Date plant_labour_contract_expiry, Date plant_target_date, String plant_data_source, String plant_asset_type_data_source, Long plant_id, Long plant_part_id, Integer cty_id, Integer commodity_group_id, String commodity_group_name, Integer plant_city_id) {
		super(Perm_ID);
		this.identifier_id = identifier_id;
		this.plant_has_gfms_ts = plant_has_gfms_ts;
		this.plant_labour_contract_expiry = plant_labour_contract_expiry;
		this.plant_target_date = plant_target_date;
		this.plant_data_source = plant_data_source;
		this.plant_asset_type_data_source = plant_asset_type_data_source;
		this.plant_id = plant_id;
		this.plant_part_id = plant_part_id;
		this.cty_id = cty_id;
		this.commodity_group_id = commodity_group_id;
		this.commodity_group_name = commodity_group_name;
		this.plant_city_id = plant_city_id;

	}


	public static PlantCommonBase LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		
		String identifier_id=RS.getString("identifier_id");
		
		String plant_has_gfms_ts=RS.getString("plant_has_gfms_ts");
		
		Date plant_labour_contract_expiry =null;
		Timestamp Ts=RS.getTimestamp("plant_labour_contract_expiry");
		if (Ts!=null)
		{
			plant_labour_contract_expiry = new Date(Ts.getTime());
		}
		
		Date plant_target_date=null;
		Ts=RS.getTimestamp("plant_target_date");
		if (Ts!=null)
		{
			plant_target_date = new Date(Ts.getTime());
		}
		
		String plant_data_source=RS.getString("plant_data_source");
		String plant_asset_type_data_source=RS.getString("plant_asset_type_data_source");
		Long plant_id=RS.getLong("plant_id");
		if(RS.wasNull())
		{
			plant_id=null;
		}
		
		Long plant_part_id=RS.getLong("plant_part_id");
		if(RS.wasNull())
		{
			plant_part_id=null;
		}
		
		Integer cty_id=RS.getInt("cty_id");
		if(RS.wasNull())
		{
			cty_id=null;
		}
		
		Integer commodity_group_id=RS.getInt("commodity_group_id");
		if(RS.wasNull())
		{
			commodity_group_id=null;
		}
		
		String commodity_group_name=RS.getString("commodity_group_name");
		
		Integer plant_city_id=RS.getInt("plant_city_id");
		if(RS.wasNull())
		{
			plant_city_id=null;
		}
		
		return new PlantCommonBase(Perm_id, identifier_id ,plant_has_gfms_ts, plant_labour_contract_expiry, plant_target_date, plant_data_source, plant_asset_type_data_source, plant_id, plant_part_id, cty_id, commodity_group_id, commodity_group_name,  plant_city_id);
		
	}


	public Integer getCommodity_group_id() {
		return commodity_group_id;
	}


	public String getCommodity_group_name() {
		return commodity_group_name;
	}


	public Integer getCty_id() {
		return cty_id;
	}

	public String getIdentifier_id() {
		return identifier_id;
	}

    public String getPlant_has_gfms_ts(){
    	return plant_has_gfms_ts;
    }
    
	public String getPlant_asset_type_data_source() {
		return plant_asset_type_data_source;
	}

	public Integer getPlant_city_id() {
		return plant_city_id;
	}

	public String getPlant_data_source() {
		return plant_data_source;
	}


	public Long getPlant_id() {
		return plant_id;
	}


	public Date getPlant_labour_contract_expiry() {
		return plant_labour_contract_expiry;
	}


	public Long getPlant_part_id() {
		return plant_part_id;
	}


	public Date getPlant_target_date() {
		return plant_target_date;
	}



	
}
