package com.thomsonreuters.ce.dbor.pasdi.cursor;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.*;


public class CursorType implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public static final CursorType AST_BASE_INFO = new CursorType("ast_base_info");                         
	public static final CursorType AST_NAME_INFO = new CursorType("ast_name_info");                         
	public static final CursorType AST_ORG_ASS_INFO = new CursorType("ast_org_ass_info");                   
	public static final CursorType AST_TYPE_COMMODITY_INFO = new CursorType("ast_type_commodity_info");     
	public static final CursorType AST_STATUS_INFO = new CursorType("ast_status_info");                     
	public static final CursorType AST_LOCATION_INFO = new CursorType("ast_location_info");                 
	public static final CursorType AST_COORDINATE_INFO = new CursorType("ast_coordinate_info"); 
	public static final CursorType AST_IDENTIFIER_INFO = new CursorType("ast_identifier_info"); 
	public static final CursorType PLANT_COMMON_BASE_INFO = new CursorType("plant_common_base_info");       
	public static final CursorType PLANT_COMMON_NOTE_INFO = new CursorType("plant_common_note_info");       
	public static final CursorType PLANT_COMMON_STATUS_INFO = new CursorType("plant_common_status_info");  
	public static final CursorType PLANT_PLA_OUTAGE_INFO = new CursorType("plant_pla_outage_info");        
	public static final CursorType PLANT_PLA_COAL_MINE_INFO = new CursorType("plant_pla_coal_mine_info");  
	public static final CursorType PLANT_PLA_STATISTIC_INFO = new CursorType("plant_pla_statistic_info");  
	public static final CursorType PLANT_PGE_OUTAGE_INFO = new CursorType("plant_pge_outage_info");        
	public static final CursorType PLANT_PGE_OPERATOR_INFO = new CursorType("plant_pge_operator_info");    
	public static final CursorType PLANT_PGE_STATISTIC_INFO = new CursorType("plant_pge_statistic_info");  
	public static final CursorType PLANT_PGE_ANALYTICS_INFO = new CursorType("plant_pge_analytics_info");  
	public static final CursorType VESSEL_BASE_INFO = new CursorType("vessel_base_info");                  
	public static final CursorType VESSEL_LATEST_LOC_INFO = new CursorType("vessel_latest_loc_info");      
	public static final CursorType VESSEL_OPEN_EVENT_INFO = new CursorType("vessel_open_event_info");  	
	public static final CursorType VESSEL_ORIGIN_INFO = new CursorType("vessel_origin_info");
	public static final CursorType VESSEL_DESTINATION_INFO = new CursorType("vessel_destination_info");
	public static final CursorType VESSEL_EVENT_FACT_DETAIL_INFO = new CursorType("vessel_event_fact_detail_info");
	public static final CursorType VESSEL_LOCATION_FACT_DETAIL_INFO = new CursorType("vessel_location_fact_detail_info");
	public static final CursorType AST_IDENTIFIER_VALUE_INFO = new CursorType("ast_identifier_value_info");
	
	
	
	private String CursorName;
	
	
	protected CursorType(String cursor_name)
	{
		this.CursorName=cursor_name;
		
	}
	
	
	public String getCursorName()
	{
		return this.CursorName;
	}
	
	
	protected SDICursorRow GetDataFromRS(ResultSet RS) throws SQLException
	{

         if (this == CursorType.AST_BASE_INFO) {
			return AstBase.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_NAME_INFO) {
			return AstName.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_ORG_ASS_INFO) {
			return AstOrgAss.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_TYPE_COMMODITY_INFO) {
			return AstTypeCommodity.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_STATUS_INFO) {
			return AstStatus.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_LOCATION_INFO) {
			return AstLocation.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_COORDINATE_INFO) {
			return AstCoordinate.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_IDENTIFIER_INFO) {
			return AstIdentifier.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_COMMON_BASE_INFO) {
			return PlantCommonBase.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_COMMON_NOTE_INFO) {
			return PlantCommonNote.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_COMMON_STATUS_INFO) {
			return PlantCommonStatus.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_PLA_OUTAGE_INFO) {
			return PlantPlaOutage.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_PLA_COAL_MINE_INFO) {
			return PlantPlaCoalMine.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_PLA_STATISTIC_INFO) {
			return PlantPlaStatistic.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_PGE_OUTAGE_INFO) {
			return PlantPgeOutage.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_PGE_OPERATOR_INFO) {
			return PlantPgeOperator.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_PGE_STATISTIC_INFO) {
			return PlantPgeStatistic.LoadDataFromRS(RS);
		} else if (this == CursorType.PLANT_PGE_ANALYTICS_INFO) {
			return PlantPgeAnalytics.LoadDataFromRS(RS);
		} else if (this == CursorType.VESSEL_BASE_INFO) {
			return VesselBase.LoadDataFromRS(RS);
		} else if (this == CursorType.VESSEL_LATEST_LOC_INFO) {
			return VesselLatestLoc.LoadDataFromRS(RS);
		} else if (this == CursorType.VESSEL_OPEN_EVENT_INFO) {
			return VesselEventOriginDestination.LoadDataFromRS(RS);
		} else if (this == CursorType.VESSEL_ORIGIN_INFO) {
			return VesselEventOriginDestination.LoadDataFromRS(RS);
		} else if (this == CursorType.VESSEL_DESTINATION_INFO) {
			return VesselEventOriginDestination.LoadDataFromRS(RS);
		} else if (this == CursorType.VESSEL_EVENT_FACT_DETAIL_INFO) {
			return VesselEventFact.LoadDataFromRS(RS);
		} else if (this == CursorType.VESSEL_LOCATION_FACT_DETAIL_INFO) {
			return VesselLocationFact.LoadDataFromRS(RS);
		} else if (this == CursorType.AST_IDENTIFIER_VALUE_INFO) {
			return AstIdentifierValue.LoadDataFromRS(RS);
		} 



		return null;
	}	
	
	@Override
	public boolean equals(Object x) {
		if (x instanceof CursorType) {
			if (((CursorType) x).CursorName.equals(this.CursorName)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.CursorName.hashCode();
	}
	

	

}
