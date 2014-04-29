package com.thomsonreuters.ce.dbor.pasdi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

import oracle.jdbc.OracleTypes;


import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.EditorialRCS;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GeographicUnit_EditorialRCS;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.Geographic_Hierarchy;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.Geographic_Unit;

import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericMetaDataEleDetail;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericCfgOutput;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericMetaDataGroupDetail;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.UniverseVesZoneDetail;
import com.thomsonreuters.ce.queue.DiskCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.IDENTIFIER_VALUE_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.RELATION_OBJECT_ID_TYPE;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class SDIPreLoadCache {
	
	private final static String SQL_1 = "{call sdi_util_pkg_v2.collect_data(?,?)}";
	private static final String SQL_2 = "{call sdi_util_pkg_v2.collect_data(?,?,?)}";
	
	public HashMap<String, String> application_configuration;
	public HashMap<Long, Geographic_Unit> universe_gun_detail_info;
	public HashMap<String, HashMap<String, HashMap<String,EditorialRCS[]>>> RCSMapping;
	public HashMap<Long, HashMap<String, EditorialRCS[]>> SpecialRCSMapping;
	public ArrayList<ArrayList<String>> VesselZoomCfgArray;
	public int ZoomLevelCount=0;

	public HashMap<Long, GenericMetaDataEleDetail> generic_metadata_ele_detail_info;
	public HashMap<String, GenericCfgOutput[]> generic_cfg_output_list;
	public HashMap<Long, GenericMetaDataGroupDetail[]> generic_metadata_group_detail_info;
	
	public HashMap<Long, HashMap<String, UniverseVesZoneDetail>> universe_ves_zone_detail_info;
	
	public DiskCache<RELATION_OBJECT_ID_TYPE> AssetRelations;
	
	public void LoadAssetRelations(boolean IsFull)
	{
		AssetRelations = new DiskCache<RELATION_OBJECT_ID_TYPE>(SDIConstants.tempfolder);

		Connection DBConn = null;
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objStatement = DBConn.prepareCall(SQL_2);
			objStatement.setString("cur_name_in",
					"ast_relation_info");
			objStatement
					.registerOutParameter("cur_out", OracleTypes.CURSOR);
			
			if (IsFull)
			{
				objStatement.setString("indicator_in", "F");
			}
			else
			{
				objStatement.setString("indicator_in", "I");
			}
			
			objStatement.execute();
			ResultSet result_set = (ResultSet) objStatement
					.getObject("cur_out");

			while (result_set.next()) {
				RELATION_OBJECT_ID_TYPE ROIT=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)result_set.getObject("relation_object_id")).getAttributes());
				AssetRelations.Append(ROIT);
			}

			result_set.close();
			objStatement.close();
			SDIConstants.SDILogger.info("ast_relation_info------------------->Done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error(e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				SDIConstants.SDILogger.error(e);
			}
		}
	}
	
	public void LoadGenericSDIMetaData()
	{
		///////////////////////////////////
		//Cache generic_metadata_ele_detail_info
		
		generic_metadata_ele_detail_info=new HashMap<Long, GenericMetaDataEleDetail>();
		
		Connection DBConn = null;
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objStatement = DBConn.prepareCall(SQL_1);
			objStatement.setString("cur_name_in",
					"generic_metadata_ele_detail_info");
			objStatement
					.registerOutParameter("cur_out", OracleTypes.CURSOR);
			objStatement.execute();
			ResultSet result_set = (ResultSet) objStatement
					.getObject("cur_out");

			while (result_set.next()) {
				
			    Long sco_id=result_set.getLong("sco_id"); 
			    
			    String GAMetadataSearchable=result_set.getString("GAMetadataSearchable");
			    
			    Date GAMetadata_effective_from=null;
			    Timestamp Ts=result_set.getTimestamp("GAMetadata_effective_from");
				if (Ts!=null)
				{
					GAMetadata_effective_from = new Date(Ts.getTime());
				}
			    
			    String GAMetadataID=result_set.getString("GAMetadataID"); 
			    String GAMetadataEnglishLongName=result_set.getString("GAMetadataEnglishLongName");
			    String GAMetadataDescription=result_set.getString("GAMetadataDescription");
			    String GAMetadataDataType=result_set.getString("GAMetadataDataType");
			    
			    Integer GAMetadataMinLength=result_set.getInt("GAMetadataMinLength");		
				if (result_set.wasNull())
				{
					GAMetadataMinLength=null;
				}
				
			    Integer GAMetadataMaxLength=result_set.getInt("GAMetadataMaxLength");		
				if (result_set.wasNull())
				{
					GAMetadataMaxLength=null;
				}
				
			    Integer GAMetadataLength=result_set.getInt("GAMetadataLength");		
				if (result_set.wasNull())
				{
					GAMetadataLength=null;
				}
			    
			    Integer GAMetadataTotalDigits=result_set.getInt("GAMetadataTotalDigits");		
				if (result_set.wasNull())
				{
					GAMetadataTotalDigits=null;
				}
				
			    Integer GAMetadataFractionDigits=result_set.getInt("GAMetadataFractionDigits");		
				if (result_set.wasNull())
				{
					GAMetadataFractionDigits=null;
				}
				
			    Float GAMetadataMinInclusive=result_set.getFloat("GAMetadataMinInclusive");		
				if (result_set.wasNull())
				{
					GAMetadataMinInclusive=null;
				}
			    
			    Float GAMetadataMaxExclusive=result_set.getFloat("GAMetadataMaxExclusive");		
				if (result_set.wasNull())
				{
					GAMetadataMaxExclusive=null;
				}			    
			    
			    String GAMetadataNullable=result_set.getString("GAMetadataNullable");
			    String GAMetadataSortable=result_set.getString("GAMetadataSortable");
			    String GAMetadataAssetGroups=result_set.getString("GAMetadataAssetGroups");
			    String GAMetadataEnumerationValues=result_set.getString("GAMetadataEnumerationValues");
			    String column_name=result_set.getString("column_name");
			    String table_name=result_set.getString("table_name");
			    
			    Date entity_created_date=null;
			    Ts=result_set.getTimestamp("entity_created_date");
				if (Ts!=null)
				{
					entity_created_date = new Date(Ts.getTime());
				}
				
			    Date entity_modified_date=null;
			    Ts=result_set.getTimestamp("entity_modified_date");
				if (Ts!=null)
				{
					entity_modified_date = new Date(Ts.getTime());
				}
				
				String is_active=result_set.getString("is_active");
				
				
			    GenericMetaDataEleDetail GMED=new GenericMetaDataEleDetail(sco_id,GAMetadataSearchable,GAMetadata_effective_from,GAMetadataID,GAMetadataEnglishLongName,GAMetadataDescription,GAMetadataDataType,GAMetadataMinLength,GAMetadataMaxLength,GAMetadataLength,GAMetadataTotalDigits,GAMetadataFractionDigits,GAMetadataMinInclusive,GAMetadataMaxExclusive,GAMetadataNullable,GAMetadataSortable,GAMetadataAssetGroups,GAMetadataEnumerationValues,column_name,table_name,entity_created_date,entity_modified_date,is_active);
				
			    generic_metadata_ele_detail_info.put(GMED.getSco_id(), GMED);
			}
			
			result_set.close();
			
			SDIConstants.SDILogger.info("generic_metadata_ele_detail_info------------------->Done");
			
			///////////////////////////////////
			//Cache generic_cfg_output_list
						
			generic_cfg_output_list = new HashMap<String, GenericCfgOutput[]> ();
			objStatement.setString("cur_name_in","generic_cfg_output_list");
			objStatement.registerOutParameter("cur_out", OracleTypes.CURSOR);
			objStatement.execute();
			result_set = (ResultSet) objStatement.getObject("cur_out");
			
			while (result_set.next()) {
				String pas_type=result_set.getString("pas_type"); 
				String is_group=result_set.getString("is_group"); 
				
				Long item_id=result_set.getLong("item_id");
				if(result_set.wasNull())
				{
					item_id=null;
				}		
				
				GenericCfgOutput GCO=new GenericCfgOutput(pas_type,is_group,item_id);
				
				GenericCfgOutput[] GCOList=generic_cfg_output_list.get(pas_type);
				if (GCOList==null)
				{
					generic_cfg_output_list.put(pas_type, new GenericCfgOutput[]{GCO});
				}
				else
				{
					GenericCfgOutput[] TempGCOList=new GenericCfgOutput[GCOList.length+1];					
					System.arraycopy(GCOList, 0, TempGCOList, 0, GCOList.length);
					TempGCOList[GCOList.length]=GCO;
					generic_cfg_output_list.put(pas_type,TempGCOList);
				}				
			}
			result_set.close();
			
			SDIConstants.SDILogger.info("generic_cfg_output_list------------------->Done");
			//////////////////////////////////////////
			//Cache generic_metadata_group_detail_info			
			generic_metadata_group_detail_info = new HashMap<Long, GenericMetaDataGroupDetail[]>();
			objStatement.setString("cur_name_in","generic_metadata_group_detail_info");
			objStatement.registerOutParameter("cur_out", OracleTypes.CURSOR);
			objStatement.execute();
			result_set = (ResultSet) objStatement.getObject("cur_out");
			
			while (result_set.next()) {
				Long list_id=result_set.getLong("list_id");
				String table_name=result_set.getString("table_name");
				Long sco_id=result_set.getLong("sco_id");
				GenericMetaDataEleDetail GMED=null;
				if(result_set.wasNull())
				{
					sco_id=null;
				}	
				else
				{			
					GMED= generic_metadata_ele_detail_info.get(sco_id);
				}
				String GAGroupID=result_set.getString("GAGroupID");
				String GAGroupType=result_set.getString("GAGroupType");
				String GAMetadataSearchable=result_set.getString("GAMetadataSearchable");
				String GAMetadataID=result_set.getString("GAMetadataID");
				String GAMetadataEnumerationValue=result_set.getString("GAMetadataEnumerationValue");
				
				Date entity_created_date=null;
			    Timestamp Ts=result_set.getTimestamp("entity_created_date");
				if (Ts!=null)
				{
					entity_created_date = new Date(Ts.getTime());
				}
				
			    Date entity_modified_date=null;
			    Ts=result_set.getTimestamp("entity_modified_date");
				if (Ts!=null)
				{
					entity_modified_date = new Date(Ts.getTime());
				}
				
				String is_active=result_set.getString("is_active");
				
				GenericMetaDataGroupDetail GMGD=new GenericMetaDataGroupDetail(list_id,table_name,GMED,GAGroupID,GAGroupType,GAMetadataSearchable,GAMetadataID,GAMetadataEnumerationValue,entity_created_date,entity_modified_date,is_active);

				GenericMetaDataGroupDetail[] GMGDList=generic_metadata_group_detail_info.get(list_id);
				if (GMGDList==null)
				{
					generic_metadata_group_detail_info.put(list_id, new GenericMetaDataGroupDetail[]{GMGD});
				}
				else
				{
					GenericMetaDataGroupDetail[] TempGMGDList=new GenericMetaDataGroupDetail[GMGDList.length+1];					
					System.arraycopy(GMGDList, 0, TempGMGDList, 0, GMGDList.length);
					TempGMGDList[GMGDList.length]=GMGD;
					generic_metadata_group_detail_info.put(list_id,TempGMGDList);
				}	
			}
			
			result_set.close();
			objStatement.close();
			SDIConstants.SDILogger.info("generic_metadata_group_detail_info------------------->Done");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error(e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				SDIConstants.SDILogger.error(e);
			}
		}
		
	}
	

	public void Load_Application_Configuration()
	{
		application_configuration = new HashMap<String, String>();

		Connection DBConn = null;
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objStatement = DBConn.prepareCall(SQL_1);
			objStatement.setString("cur_name_in",
					"universe_application_cfg");
			objStatement
					.registerOutParameter("cur_out", OracleTypes.CURSOR);
			objStatement.execute();
			ResultSet result_set = (ResultSet) objStatement
					.getObject("cur_out");

			while (result_set.next()) {
				String name = result_set.getString("name");
				String value = result_set.getString("value");
				application_configuration.put(name, value);
			}

			result_set.close();
			objStatement.close();
			SDIConstants.SDILogger.info("universe_application_cfg------------------->Done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error(e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				SDIConstants.SDILogger.error(e);
			}
		}
	}
	
	public void LoadGeographicUnit() {

		universe_gun_detail_info = new HashMap<Long, Geographic_Unit>();

			Connection DBConn = null;
			try {

				DBConn = new EasyConnection(DBConnNames.CEF_CNR);
				CallableStatement objStatement = DBConn.prepareCall(SQL_1);
				objStatement.setString("cur_name_in",
						"universe_gun_detail_info");
				objStatement
						.registerOutParameter("cur_out", OracleTypes.CURSOR);
				objStatement.execute();
				ResultSet result_set = (ResultSet) objStatement
						.getObject("cur_out");

				while (result_set.next()) {
					Long gunid = result_set.getLong("gun_id");
					
					Long gun_perm_id = result_set.getLong("gun_perm_id");
					if(result_set.wasNull())
					{
						gun_perm_id=null;
					}					
					
					String gundesc = result_set.getString("gun_desc");
					String gucdesc = result_set.getString("guc_desc");
					
					Long relation_object_type_id = result_set.getLong("relation_object_type_id");
					if(result_set.wasNull())
					{
						relation_object_type_id=null;
					}	
					
					String relation_object_type = result_set.getString("relation_object_type");
					
					Long relationship_type_id = result_set.getLong("relationship_type_id");
					if(result_set.wasNull())
					{
						relationship_type_id=null;
					}	
					
					
					
					String relationship_type = result_set.getString("relationship_type");
					String rcscode = result_set.getString("rcs_code");
					Float latitudenorth = result_set.getFloat("latitude_north");
					if (result_set.wasNull()) {
						latitudenorth = null;
					}

					Float latitudesouth = result_set.getFloat("latitude_south");
					if (result_set.wasNull()) {
						latitudesouth = null;
					}

					Float longitudeeast = result_set.getFloat("longitude_east");
					if (result_set.wasNull()) {
						longitudeeast = null;
					}

					Float longitudewest = result_set.getFloat("longitude_west");
					if (result_set.wasNull()) {
						longitudewest = null;
					}

					Geographic_Unit thisGUN = new Geographic_Unit(gunid,gun_perm_id,
							gundesc,gucdesc, relation_object_type_id,relation_object_type,relationship_type_id,relationship_type, rcscode, latitudenorth, latitudesouth,
							longitudeeast, longitudewest);
					universe_gun_detail_info.put(gunid, thisGUN);
				}

				result_set.close();
				objStatement.close();
				
				SDIConstants.SDILogger.info("universe_gun_detail_info------------------->Done");

				// loading hierarchy
				objStatement = DBConn.prepareCall(SQL_1);
				objStatement.setString("cur_name_in",
						"universe_gun_hierarchy_info");
				objStatement
						.registerOutParameter("cur_out", OracleTypes.CURSOR);
				objStatement.execute();
				result_set = (ResultSet) objStatement.getObject("cur_out");

				while (result_set.next()) {
					Long gunid = result_set.getLong("gun_id");
					int hierarchy_classification_id = result_set
							.getInt("hierarchy_classification_id");
					String gun_id_hierarchy = result_set
							.getString("gun_id_hierarchy");
					String zoom_level_hierarchy = result_set
							.getString("zoom_level_hierarchy");

					Geographic_Unit GUN = universe_gun_detail_info.get(gunid);
					if (GUN != null) {
						GUN.Geographic_Hierarchy_Map
								.put(hierarchy_classification_id,
										new Geographic_Hierarchy(
												hierarchy_classification_id,
												gun_id_hierarchy,
												zoom_level_hierarchy));
					}
				}
				
				result_set.close();
				objStatement.close();
				
				SDIConstants.SDILogger.info("universe_gun_hierarchy_info------------------->Done");
				
				// loading geographic unit editorial mapping
				objStatement = DBConn.prepareCall(SQL_1);
				objStatement.setString("cur_name_in",
						"universe_editorial_plant_gun_rcs_code_mapping");
				objStatement
						.registerOutParameter("cur_out", OracleTypes.CURSOR);
				objStatement.execute();
				result_set = (ResultSet) objStatement.getObject("cur_out");
				
				while (result_set.next()) {
					Long gunid = result_set.getLong("gun_id");
					int hierarchy_classification_id = result_set
					.getInt("hierarchy_classification_id");
					
					String target_rcs_code=result_set.getString("target_rcs_code");
					Long target_id=result_set.getLong("target_id");
					if (result_set.wasNull())
					{
						target_id=null;
					}
					
					Geographic_Unit GUN = universe_gun_detail_info.get(gunid);
					if (GUN != null) {
						GeographicUnit_EditorialRCS[] GER=GUN.Geographic_EditorialRCS_Map.get(hierarchy_classification_id);
						if (GER==null)
						{
							GUN.Geographic_EditorialRCS_Map.put(hierarchy_classification_id, new GeographicUnit_EditorialRCS[]{ new GeographicUnit_EditorialRCS(target_rcs_code,target_id)});					
						}
						else
						{
							GeographicUnit_EditorialRCS[] tempGER=new GeographicUnit_EditorialRCS[GER.length+1];
							System.arraycopy(GER, 0, tempGER, 0, GER.length);
							tempGER[GER.length]=new GeographicUnit_EditorialRCS(target_rcs_code,target_id);
							GUN.Geographic_EditorialRCS_Map.put(hierarchy_classification_id,tempGER);
						}
					}					
				}

				result_set.close();
				objStatement.close();
				
				SDIConstants.SDILogger.info("universe_editorial_plant_gun_rcs_code_mapping------------------->Done");

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				SDIConstants.SDILogger.error(e);

			} finally {
				try {
					DBConn.close();
				} catch (SQLException e) {
					SDIConstants.SDILogger.error(e);
				}
			}

	}	
	
	public UniverseVesZoneDetail getVesselZoneDetail(Long zone_id,String zone_type)
	{
		HashMap<String, UniverseVesZoneDetail> temp_hm=universe_ves_zone_detail_info.get(zone_id);
		
		if (temp_hm!=null)
		{
			return temp_hm.get(zone_type);
		}
		else
		{
			return null;
		}
	}
	
	public void LoadVesselZoneDetail()
	{
		universe_ves_zone_detail_info=new HashMap<Long, HashMap<String, UniverseVesZoneDetail>>();
		Connection DBConn = null;
		
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objStatement = DBConn.prepareCall(SQL_1);
			objStatement.setString("cur_name_in",
					"universe_ves_zone_detail_info");
			objStatement.registerOutParameter("cur_out", OracleTypes.CURSOR);
			objStatement.execute();
			ResultSet result_set = (ResultSet) objStatement.getObject("cur_out");
			
			while (result_set.next()) {

				String zone_type = result_set.getString("zone_type");
				Long zone_id = result_set.getLong("zone_id");
				
				Long zone_perm_id = result_set.getLong("zone_perm_id");
				if(result_set.wasNull())
				{
					zone_perm_id=null;
				}	
				
				String zone_name = result_set.getString("zone_name");
				String zone_code = result_set.getString("zone_code");
				
				Integer zone_rank = result_set.getInt("zone_rank");
				if(result_set.wasNull())
				{
					zone_rank=null;
				}	
				
				Long zone_axsmarine_id = result_set.getLong("zone_axsmarine_id");
				if(result_set.wasNull())
				{
					zone_axsmarine_id=null;
				}	
				
				Long relation_object_type_id = result_set.getLong("relation_object_type_id");
				if(result_set.wasNull())
				{
					relation_object_type_id=null;
				}
				
				String relation_object_type = result_set.getString("relation_object_type");
				
				Long relationship_type_id = result_set.getLong("relationship_type_id");
				if(result_set.wasNull())
				{
					relationship_type_id=null;
				}
				
				String relationship_type = result_set.getString("relationship_type");
				
				Float latitude_north = result_set.getFloat("latitude_north");
				if(result_set.wasNull())
				{
					latitude_north=null;
				}
				
				Float latitude_south = result_set.getFloat("latitude_south");
				if(result_set.wasNull())
				{
					latitude_south=null;
				}
				
				Float longitude_east = result_set.getFloat("longitude_east");
				if(result_set.wasNull())
				{
					longitude_east=null;
				}
				
				Float longitude_west = result_set.getFloat("longitude_west");
				if(result_set.wasNull())
				{
					longitude_west=null;
				}
				
				UniverseVesZoneDetail UVZD=new UniverseVesZoneDetail(zone_type, zone_id, zone_perm_id, zone_name, zone_code, zone_rank, zone_axsmarine_id, relation_object_type_id, relation_object_type, relationship_type_id, relationship_type, latitude_north, latitude_south, longitude_east,  longitude_west);
				
				HashMap<String, UniverseVesZoneDetail> temp_hm= universe_ves_zone_detail_info.get(zone_id);
				if (temp_hm!=null)
				{
					temp_hm.put(zone_type, UVZD);
				}
				else
				{
					temp_hm=new HashMap<String, UniverseVesZoneDetail>();
					temp_hm.put(zone_type, UVZD);
					universe_ves_zone_detail_info.put(zone_id, temp_hm);
				}
				
			}			
			
			result_set.close();
			objStatement.close();
			SDIConstants.SDILogger.info("universe_ves_zone_detail_info------------------->Done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error(e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				SDIConstants.SDILogger.error(e);
			}
		}
		
	}
	
	
	public void LoadRCSSpecialMapping()
	{
		SpecialRCSMapping = new HashMap<Long, HashMap<String, EditorialRCS[]>> ();
		Connection DBConn = null;
		
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objStatement = DBConn.prepareCall(SQL_1);
			objStatement.setString("cur_name_in",
					"universe_editorial_special_mapping");
			objStatement.registerOutParameter("cur_out", OracleTypes.CURSOR);
			objStatement.execute();
			ResultSet result_set = (ResultSet) objStatement.getObject("cur_out");
			
			while (result_set.next()) {
				
				Long perm_id = result_set.getLong("perm_id");
				String for_element = result_set.getString("for_element");
				String target_rcs_code = result_set.getString("target_rcs_code");
				Long target_id = result_set.getLong("target_id");
				if(result_set.wasNull())
				{
					target_id=null;
				}	
				
				HashMap<String, EditorialRCS[]> SPECIAL_RCS_CODE_MAPPING=SpecialRCSMapping.get(perm_id);
				
				if (SPECIAL_RCS_CODE_MAPPING==null)
				{
					SPECIAL_RCS_CODE_MAPPING=new HashMap<String, EditorialRCS[]>();						
					SpecialRCSMapping.put(perm_id, SPECIAL_RCS_CODE_MAPPING);						
				}
				
				EditorialRCS[] ERCSs = SPECIAL_RCS_CODE_MAPPING.get(for_element);
				
				if (ERCSs==null)						
				{
					SPECIAL_RCS_CODE_MAPPING.put(for_element, new EditorialRCS[]{new EditorialRCS(target_rcs_code,target_id)});
				}
				else
				{
					EditorialRCS[] tempERCSs= new EditorialRCS[ERCSs.length+1];
					System.arraycopy(ERCSs, 0, tempERCSs, 0, ERCSs.length);
					tempERCSs[ERCSs.length]=new EditorialRCS(target_rcs_code,target_id);
					SPECIAL_RCS_CODE_MAPPING.put(for_element, tempERCSs);
				}				
			}			
			
			result_set.close();
			objStatement.close();
			SDIConstants.SDILogger.info("universe_editorial_special_mapping------------------->Done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error(e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				SDIConstants.SDILogger.error(e);
			}
		}
		
	}
	
	public void LoadRCSMapping()
	{
		RCSMapping = new HashMap<String, HashMap<String, HashMap<String,EditorialRCS[]>>>();

			Connection DBConn = null;
			try {
				DBConn = new EasyConnection(DBConnNames.CEF_CNR);
				CallableStatement objStatement = DBConn.prepareCall(SQL_1);
				objStatement.setString("cur_name_in",
						"universe_editorial_rcs_code_mapping");
				objStatement
						.registerOutParameter("cur_out", OracleTypes.CURSOR);
				objStatement.execute();
				ResultSet result_set = (ResultSet) objStatement
						.getObject("cur_out");

				while (result_set.next()) {
					String asset_category = result_set.getString("asset_category");
					String source_rcs_code = result_set.getString("source_rcs_code");
					String target_rcs_code = result_set.getString("target_rcs_code");
					String for_element = result_set.getString("for_element");
					
					Long target_id = result_set.getLong("target_id");
					if(result_set.wasNull())
					{
						target_id=null;
					}					

					HashMap<String, HashMap<String,EditorialRCS[]>> RCS_CODE_MAPPING=RCSMapping.get(asset_category);
					
					if (RCS_CODE_MAPPING==null)
					{
						RCS_CODE_MAPPING=new HashMap<String, HashMap<String,EditorialRCS[]>>();						
						RCSMapping.put(asset_category, RCS_CODE_MAPPING);						
					}
					
					HashMap<String,EditorialRCS[]> For_Element_Mapping=RCS_CODE_MAPPING.get(source_rcs_code);
					
					if (For_Element_Mapping==null)
					{
						For_Element_Mapping=new HashMap<String,EditorialRCS[]>();
						RCS_CODE_MAPPING.put(source_rcs_code, For_Element_Mapping);						
					}
					
					
					EditorialRCS[] ERCSs = For_Element_Mapping.get(for_element);
					
					if (ERCSs==null)						
					{
						For_Element_Mapping.put(for_element, new EditorialRCS[]{new EditorialRCS(target_rcs_code,target_id)});
					}
					else
					{
						EditorialRCS[] tempERCSs= new EditorialRCS[ERCSs.length+1];
						System.arraycopy(ERCSs, 0, tempERCSs, 0, ERCSs.length);
						tempERCSs[ERCSs.length]=new EditorialRCS(target_rcs_code,target_id);
						For_Element_Mapping.put(for_element, tempERCSs);
					}
				}
				
				result_set.close();
				objStatement.close();
				SDIConstants.SDILogger.info("universe_editorial_rcs_code_mapping------------------->Done");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				SDIConstants.SDILogger.error(e);
			} finally {
				try {
					DBConn.close();
				} catch (SQLException e) {
					SDIConstants.SDILogger.error(e);
				}
			}
	}
	
	public void LoadAssetZoomCfg()
	{

			VesselZoomCfgArray=new ArrayList<ArrayList<String>>();
			
			String VESSEL_ZOOM_LEVEL_CFG = this.application_configuration.get("VESSEL_ZOOM_LEVEL_CFG");

			StringTokenizer level_tokenizer = new StringTokenizer(VESSEL_ZOOM_LEVEL_CFG,";");

			while(level_tokenizer.hasMoreTokens()) {
				String Level_Cfg= level_tokenizer.nextToken();
				int Equal_Pos=Level_Cfg.indexOf('=');
				String Level_eventtype_cfg=Level_Cfg.substring(Equal_Pos+1);

				ArrayList<String> Level_event=new ArrayList<String>();
				StringTokenizer Plus_tokenizer = new StringTokenizer(Level_eventtype_cfg,"+");
				while(Plus_tokenizer.hasMoreTokens())
				{
					String event=Plus_tokenizer.nextToken();
					Level_event.add(event);
				}			    	
				VesselZoomCfgArray.add(Level_event);
			}
			
			
			ZoomLevelCount= Integer.parseInt(this.application_configuration.get("ZOOM_LEVEL_COUNT_FOR_PLANT"));

	}
	
	public EditorialRCS[] getEditorialRCS(String asset_category,String source_rcs_code, String for_element )
	{
		HashMap<String, HashMap<String,EditorialRCS[]>> RCS_CODE_MAPPING = RCSMapping.get(asset_category);
		
		if (RCS_CODE_MAPPING!=null)
		{
			HashMap<String,EditorialRCS[]> for_element_mapping = RCS_CODE_MAPPING.get(source_rcs_code);
			
			if (for_element_mapping != null)
			{
				return for_element_mapping.get(for_element);
			}
		}
		
		return null;
		
	}
	
	public EditorialRCS[] getSpecialEditorialRCS(Long perm_id, String for_element)
	{
		HashMap<String,EditorialRCS[]> SPECIAL_RCS_CODE_MAPPING = SpecialRCSMapping.get(perm_id);
		
		if (SPECIAL_RCS_CODE_MAPPING != null)
		{
			return SPECIAL_RCS_CODE_MAPPING.get(for_element);
		}
		
		return null;
		
	}
	
	
	
}
