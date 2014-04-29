package com.thomsonreuters.ce.dbor.pasdi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;




import com.thomsonreuters.ce.timing.DateConstants;
import com.thomsonreuters.ce.timing.DateFun;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.cache.ProcessingStatus;
import com.thomsonreuters.ce.dbor.cache.SDIPublishStyle;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorAction;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.FileDataMarker;
import com.thomsonreuters.ce.dbor.pasdi.cursor.GenericCursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.generator.AlertingGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.CommodityPhysicalAssetGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.EditorialGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.GenericAssetGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.GenericAssetGroupGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.GenericAssetMetadataGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.IdentifierGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.IdentifierGeneratorV4;
import com.thomsonreuters.ce.dbor.pasdi.generator.PlantAssetGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.RelationshipGenerator;
import com.thomsonreuters.ce.dbor.pasdi.generator.VesselAssetGenerator;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.queue.MagicPipe;

public class SearchSDIIncrementalLoad {

	private static final String configfile = "../cfg/pasdi/incremental.conf";

	// SQLs to maintain file processing history
	private static final String CheckDBTime = "select to_char(sysdate,'yyyymmddhh24miss') from dual";
	private static final String InsertFileProcessHistory = "insert into file_process_history (id, file_name,dit_file_category_id,start_time,dit_processing_status) values (fph_seq.nextval,?,?,sysdate,?)";
	private static final String GetProcessingDetail = "select count(*) from processing_detail where fph_id = ? and dit_message_category_id in (select id from dimension_item where value='WARNING')";
	private static final String CompleteFileHistory = "update file_process_history set end_time=sysdate, dit_processing_status=? where id=?";
	private static final String InsertSDIDetail = "insert into sdi_file_detail (fph_id, dit_publish_style_id, uuid,sdi_timestamp,incremental_date, previous_date) values (?,?,?,?,to_date(?,'yyyymmddhh24miss'),to_date(?,'yyyymmddhh24miss'))";
	private static final String CheckLastIncrementalTime = "select to_char(incremental_date,'yyyymmddhh24miss') as incremental_date from (select b.incremental_date as incremental_date from file_process_history a, sdi_file_detail b where a.id=b.fph_id and a.dit_file_category_id=? and b.Dit_Publish_Style_Id=? and a.end_time is not null order by b.incremental_date desc) where rownum=1";
	private static final String InitialPermIDList = "{call sdi_util_pkg_v2.initialise_perm_id_lst(?,?,'I')}";

	private static boolean isProd = true;

	public static String GenerateIncrementalSDI() {

		// Get SDIEndDate from DB;
		Date endTimeStamp = getDBDate();

		// TODO Auto-generated method stub
		// Read configuration file
		try {
			FileInputStream TaskFis = new FileInputStream(configfile);
			Properties Prop = new Properties();
			Prop.load(TaskFis);

			String sdiFileLocation = FileUtilities.GetAbsolutePathFromEnv(Prop.getProperty("filelocation"));
			SDIConstants.tempfolder = FileUtilities.GetAbsolutePathFromEnv(Prop.getProperty("tempfolder"));
			int CursorRowDispatcher = Integer.parseInt(Prop
					.getProperty("CursorRowDispatcher"));
			int CommodityPhysicalAssetGenerator = Integer.parseInt(Prop
					.getProperty("CommodityPhysicalAssetGenerator"));
			int PlantAssetGenerator = Integer.parseInt(Prop
					.getProperty("PlantAssetGenerator"));
			int VesselAssetGenerator = Integer.parseInt(Prop
					.getProperty("VesselAssetGenerator"));
			int IdentifierGenerator = Integer.parseInt(Prop
					.getProperty("IdentifierGenerator"));
			int EditorialGenerator = Integer.parseInt(Prop
					.getProperty("EditorialGenerator"));
			int GenericAssetGenerator = Integer.parseInt(Prop
					.getProperty("GenericAssetGenerator"));
			int AlertingGenerator = Integer.parseInt(Prop
					.getProperty("AlertingGenerator"));
			// Calculate start time stamp
			Date startSDITimeStamp = null;

			Connection conn = new EasyConnection(DBConnNames.CEF_CNR);

			try {
				PreparedStatement PreStatement = null;
				ResultSet objResult = null;
				PreStatement = conn.prepareStatement(CheckLastIncrementalTime);
				PreStatement.setInt(1, FileCategory
						.getInstance("Search SDI V2").getID());
				PreStatement.setInt(2, SDIPublishStyle.INCREMENTAL.getID());

				objResult = PreStatement.executeQuery();

				if (objResult.next()) {
					SimpleDateFormat formatter = new SimpleDateFormat(
							DateConstants.FULLTIMEFORMAT);
					ParsePosition pos = new ParsePosition(0);
					startSDITimeStamp = formatter.parse(
							objResult.getString("incremental_date"), pos);
				} else {
					// if this is the first time to generate incremental SDI
					// v2 then hardcode it for capturing data change in past
					// hour
					startSDITimeStamp = new Date(
							endTimeStamp.getTime() - 3600000);
				}

			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				SDIConstants.SDILogger.error( "SQL exception", e1);
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					SDIConstants.SDILogger.error( "SQL exception", e);
				}
			}

			SDIConstants.SDILogger.info(
					"[Physical Assets SDI V2]: Start Incremental SDI generation for timestamp: "
							+ DateFun.getStringDate(startSDITimeStamp) + "."
							+ DateFun.getStringDate(endTimeStamp));
			// Clear PERMID_CURSOR_MAP;
			HashMap<Long, HashMap<CursorType, FileDataMarker>> PERMID_CURSOR_MAP = new HashMap<Long, HashMap<CursorType, FileDataMarker>>();

			// Generate Manifest
			String temp_FileSuffix = getStringDate(startSDITimeStamp) + "."
					+ getStringDate(endTimeStamp) + ".Incremental";
			String ManiFest_Name = SDIConstants.ManiFest_Prefix
					+ temp_FileSuffix;
			String CommodityPhysicalAsset_Name = SDIConstants.CommodityPhysicalAsset_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String PhysicalAssetIdentifier_Name = SDIConstants.PhysicalAssetIdentifier_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String PlantAsset_Name = SDIConstants.PlantAsset_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String VesselAsset_Name = SDIConstants.VesselAsset_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String Editorial_Name = SDIConstants.Editorial_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String GenericAssetMetadata_Name = SDIConstants.GenericAssetMetaData_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String GenericAssetGroup_Name = SDIConstants.GenericAssetGroup_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String GenericAsset_Name = SDIConstants.GenericAsset_Prefix
					+ temp_FileSuffix + ".xml.gz";
			String Relationship_Name = SDIConstants.Relationship_Prefix
					+ temp_FileSuffix + ".xml.gz";

			String Identifier_Name_v4 = SDIConstants.PhysicalAssetIdentifier_Prefix_v4
					+ temp_FileSuffix + ".xml.gz";
			String ManiFest_Name_Inventory = SDIConstants.ManiFest_Prefix_Inventory
					+ temp_FileSuffix + ".Inventory";

			// Log a file processing record in table
			long FPH_ID = CreateFileProcessHistory(ManiFest_Name);

			// ////////////////////////////////////////////
			// Added by Jing Wang
			// create new incremental record in table EM_SDI_HISTORY
			if (isProd) {
				conn = new EasyConnection(DBConnNames.CEF_CNR);
				try {
					PreparedStatement objPreStatement = conn
							.prepareStatement(InsertSDIDetail);
					objPreStatement.setLong(1, FPH_ID);
					objPreStatement.setInt(2,
							SDIPublishStyle.INCREMENTAL.getID());
					objPreStatement.setString(3, temp_FileSuffix);
					objPreStatement.setDate(4,
							new java.sql.Date(new Date().getTime()));
					SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
					objPreStatement.setString(5, f.format(endTimeStamp));
					objPreStatement.setString(6, f.format(startSDITimeStamp));
					objPreStatement.executeUpdate();
					conn.commit();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					SDIConstants.SDILogger.error(e);
				} finally {
					try {
						conn.close();
					} catch (SQLException e) {
						SDIConstants.SDILogger.error(e);
					}
				}
			}
			long StartTime = new Date().getTime();

			// ////////////////////////////////////////////
			// Step 1 read asset underlying data used for assembling XML from
			// cursors into location disk files
			if (isProd) {
				try {
					conn = new EasyConnection(DBConnNames.CEF_CNR);

					// Initialize PermID List
					CallableStatement objStatement = conn
							.prepareCall(InitialPermIDList);
					objStatement.setTimestamp(1, new Timestamp(
							startSDITimeStamp.getTime()));
					objStatement.setTimestamp(2,
							new Timestamp(endTimeStamp.getTime()));
					objStatement.execute();
					objStatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					SDIConstants.SDILogger.error(e);
				} finally {
					try {
						conn.close();
					} catch (SQLException e) {
						SDIConstants.SDILogger.error(e);
					}
				}
			}
			// /////////////////////////////////////
			// unload cursor data to local dump files
			final Counter CursorActionCT = new Counter();

			CursorAction CA_AST_BASE_INFO = new CursorAction(
					CursorType.AST_BASE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_AST_NAME_INFO = new CursorAction(
					CursorType.AST_NAME_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_AST_ORG_ASS_INFO = new CursorAction(
					CursorType.AST_ORG_ASS_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_AST_TYPE_COMMODITY_INFO = new CursorAction(
					CursorType.AST_TYPE_COMMODITY_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_AST_STATUS_INFO = new CursorAction(
					CursorType.AST_STATUS_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_AST_LOCATION_INFO = new CursorAction(
					CursorType.AST_LOCATION_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_AST_COORDINATE_INFO = new CursorAction(
					CursorType.AST_COORDINATE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_AST_IDENTIFIER_INFO = new CursorAction(
					CursorType.AST_IDENTIFIER_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_COMMON_BASE_INFO = new CursorAction(
					CursorType.PLANT_COMMON_BASE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_COMMON_NOTE_INFO = new CursorAction(
					CursorType.PLANT_COMMON_NOTE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_COMMON_STATUS_INFO = new CursorAction(
					CursorType.PLANT_COMMON_STATUS_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_PLA_OUTAGE_INFO = new CursorAction(
					CursorType.PLANT_PLA_OUTAGE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_PLA_COAL_MINE_INFO = new CursorAction(
					CursorType.PLANT_PLA_COAL_MINE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_PLA_STATISTIC_INFO = new CursorAction(
					CursorType.PLANT_PLA_STATISTIC_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_PGE_OUTAGE_INFO = new CursorAction(
					CursorType.PLANT_PGE_OUTAGE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_PGE_OPERATOR_INFO = new CursorAction(
					CursorType.PLANT_PGE_OPERATOR_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_PGE_STATISTIC_INFO = new CursorAction(
					CursorType.PLANT_PGE_STATISTIC_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_PLANT_PGE_ANALYTICS_INFO = new CursorAction(
					CursorType.PLANT_PGE_ANALYTICS_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_VESSEL_BASE_INFO = new CursorAction(
					CursorType.VESSEL_BASE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_VESSEL_LATEST_LOC_INFO = new CursorAction(
					CursorType.VESSEL_LATEST_LOC_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_VESSEL_OPEN_EVENT_INFO = new CursorAction(
					CursorType.VESSEL_OPEN_EVENT_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_VESSEL_ORIGIN_INFO = new CursorAction(
					CursorType.VESSEL_ORIGIN_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_VESSEL_DESTINATION_INFO = new CursorAction(
					CursorType.VESSEL_DESTINATION_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			CursorAction CA_GENERIC_DATA_AGS_FACILITY = new CursorAction(
					GenericCursorType
							.GetGenericCursorType("generic_data_ags_facility"),
					PERMID_CURSOR_MAP, CursorActionCT, false);
			CursorAction CA_GENERIC_DATA_AGS_STATISTIC = new CursorAction(
					GenericCursorType
							.GetGenericCursorType("generic_data_ags_statistic"),
					PERMID_CURSOR_MAP, CursorActionCT, false);
			CursorAction CA_GENERIC_DATA_BERTH = new CursorAction(
					GenericCursorType
							.GetGenericCursorType("generic_data_berth"),
					PERMID_CURSOR_MAP, CursorActionCT, false);
			CursorAction CA_VESSEL_EVENT_FACT = new CursorAction(
					CursorType.VESSEL_EVENT_FACT_DETAIL_INFO,
					PERMID_CURSOR_MAP, CursorActionCT, false);
			CursorAction CA_VESSEL_LOCATION_FACT = new CursorAction(
					CursorType.VESSEL_LOCATION_FACT_DETAIL_INFO,
					PERMID_CURSOR_MAP, CursorActionCT, false);
			CursorAction CA_AST_IDENTIFIER_VALUE = new CursorAction(
					CursorType.AST_IDENTIFIER_VALUE_INFO, PERMID_CURSOR_MAP,
					CursorActionCT, false);
			
			

			new Thread(CA_AST_BASE_INFO).start();
			new Thread(CA_AST_NAME_INFO).start();
			new Thread(CA_AST_ORG_ASS_INFO).start();
			new Thread(CA_AST_TYPE_COMMODITY_INFO).start();
			new Thread(CA_AST_STATUS_INFO).start();
			new Thread(CA_AST_LOCATION_INFO).start();
			new Thread(CA_AST_COORDINATE_INFO).start();
			new Thread(CA_AST_IDENTIFIER_INFO).start();
			new Thread(CA_PLANT_COMMON_BASE_INFO).start();
			new Thread(CA_PLANT_COMMON_NOTE_INFO).start();
			new Thread(CA_PLANT_COMMON_STATUS_INFO).start();
			new Thread(CA_PLANT_PLA_OUTAGE_INFO).start();
			new Thread(CA_PLANT_PLA_COAL_MINE_INFO).start();
			new Thread(CA_PLANT_PLA_STATISTIC_INFO).start();
			new Thread(CA_PLANT_PGE_OUTAGE_INFO).start();
			new Thread(CA_PLANT_PGE_OPERATOR_INFO).start();
			new Thread(CA_PLANT_PGE_STATISTIC_INFO).start();
			new Thread(CA_PLANT_PGE_ANALYTICS_INFO).start();
			new Thread(CA_VESSEL_BASE_INFO).start();
			new Thread(CA_VESSEL_LATEST_LOC_INFO).start();
			new Thread(CA_VESSEL_OPEN_EVENT_INFO).start();
			new Thread(CA_VESSEL_ORIGIN_INFO).start();
			new Thread(CA_VESSEL_DESTINATION_INFO).start();
			new Thread(CA_GENERIC_DATA_AGS_FACILITY).start();
			new Thread(CA_GENERIC_DATA_AGS_STATISTIC).start();
			new Thread(CA_GENERIC_DATA_BERTH).start();
			new Thread(CA_VESSEL_EVENT_FACT).start();
			new Thread(CA_VESSEL_LOCATION_FACT).start();
			new Thread(CA_AST_IDENTIFIER_VALUE).start();
			
			////////////////////////////////////////
			//Start pre load cache
			
			final SDIPreLoadCache SDIPLC=new SDIPreLoadCache();
			
			CursorActionCT.Increase();
			new Thread(new Runnable()
			{
				public void run()
				{
					try {
						SDIPLC.LoadGeographicUnit();
					} finally {
						// TODO Auto-generated catch block
						CursorActionCT.Decrease();
					}
				}
			}
			).start();
			
			///////////////////////////////////////
			CursorActionCT.Increase();
			new Thread(new Runnable()
			{
				public void run()
				{
					try {
						SDIPLC.Load_Application_Configuration();
						SDIPLC.LoadAssetZoomCfg();
					} finally {
						// TODO Auto-generated catch block
						CursorActionCT.Decrease();
					}
				}
			}
			).start();
			///////////////////////////////////////
			CursorActionCT.Increase();
			new Thread(new Runnable()
			{
				public void run()
				{
					try {
						SDIPLC.LoadRCSMapping();
					} finally {
						// TODO Auto-generated catch block
						CursorActionCT.Decrease();
					}
				}
			}
			).start();
			///////////////////////////////////////
			CursorActionCT.Increase();
			new Thread(new Runnable()
			{
				public void run()
				{
					try {
						SDIPLC.LoadRCSSpecialMapping();
					} finally {
						// TODO Auto-generated catch block
						CursorActionCT.Decrease();
					}
				}
			}
			).start();

			///////////////////////////////////////
			CursorActionCT.Increase();
			new Thread(new Runnable()
			{
				public void run()
				{
					try {
						SDIPLC.LoadGenericSDIMetaData();
					} finally {
						// TODO Auto-generated catch block
						CursorActionCT.Decrease();
					}
				}
			}
			).start();
			
			///////////////////////////////////////
			CursorActionCT.Increase();
			new Thread(new Runnable()
			{
				public void run()
				{
					try {
						SDIPLC.LoadAssetRelations(false);
					} finally {
						// TODO Auto-generated catch block
						CursorActionCT.Decrease();
					}
				}
			}
			).start();
			
			///////////////////////////////////////
			CursorActionCT.Increase();
			new Thread(new Runnable()
			{
				public void run()
				{
					try {
						SDIPLC.LoadVesselZoneDetail();
					} finally {
						// TODO Auto-generated catch block
						CursorActionCT.Decrease();
					}
				}
			}
			).start();
			

			// /////////////////////////////////////
			// waiting for cursor unload to complete
			CursorActionCT.WaitToDone();

			//
			long TimeSpent = new Date().getTime() - StartTime;
			SDIConstants.SDILogger.info(
					"[Physical Assets SDI V2]: Time spent on unloading cursors-------------------"
							+ TimeSpent);

			// start dispatcher
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_CommodityPhysicalAsset = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_PlantAsset = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_VesselAsset = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_Identifier = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);
			// leo
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_Identifier_v4 = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_Editorial = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_GenericAsset = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);

			MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_AlertingMsg = new MagicPipe<HashMap<CursorType, SDICursorRow[]>>(
					500, 1000, 1024, 50, SDIConstants.tempfolder);
			
			CursorRowDispatcher CRD = new CursorRowDispatcher(
					CursorRowDispatcher, MP_CommodityPhysicalAsset,
					MP_PlantAsset, MP_VesselAsset, MP_Identifier,
					MP_Identifier_v4, MP_Editorial, MP_GenericAsset,MP_AlertingMsg);
			CRD.start(PERMID_CURSOR_MAP.entrySet().iterator());

			// clear thread counter
			Counter GeneratorCT = new Counter();

			CommodityPhysicalAssetGenerator CPAG = new CommodityPhysicalAssetGenerator(
					sdiFileLocation, CommodityPhysicalAsset_Name,
					startSDITimeStamp, endTimeStamp,
					CommodityPhysicalAssetGenerator, MP_CommodityPhysicalAsset,
					GeneratorCT, SDIPLC);
			CPAG.Start();

			PlantAssetGenerator PAG = new PlantAssetGenerator(sdiFileLocation,
					PlantAsset_Name, startSDITimeStamp, endTimeStamp,
					PlantAssetGenerator, MP_PlantAsset, GeneratorCT, SDIPLC);
			PAG.Start();

			VesselAssetGenerator VAG = new VesselAssetGenerator(
					sdiFileLocation, VesselAsset_Name, startSDITimeStamp,
					endTimeStamp, VesselAssetGenerator, MP_VesselAsset,
					GeneratorCT, SDIPLC);
			VAG.Start();

			IdentifierGenerator IG = new IdentifierGenerator(sdiFileLocation,
					PhysicalAssetIdentifier_Name, startSDITimeStamp,
					endTimeStamp, IdentifierGenerator, MP_Identifier,
					GeneratorCT);
			IG.Start();

			IdentifierGeneratorV4 idenGeneratorV41 = new IdentifierGeneratorV4(
					sdiFileLocation, Identifier_Name_v4, startSDITimeStamp,
					endTimeStamp, IdentifierGenerator, MP_Identifier_v4,
					GeneratorCT);
			idenGeneratorV41.Start();

			EditorialGenerator EG = new EditorialGenerator(sdiFileLocation,
					Editorial_Name, startSDITimeStamp, endTimeStamp,
					EditorialGenerator, MP_Editorial, GeneratorCT, SDIPLC);
			EG.Start();

			GenericAssetGenerator GAG = new GenericAssetGenerator(
					sdiFileLocation, GenericAsset_Name, startSDITimeStamp,
					endTimeStamp, GenericAssetGenerator, MP_GenericAsset,
					GeneratorCT, SDIPLC);
			GAG.Start();

			
			AlertingGenerator AG = new AlertingGenerator(startSDITimeStamp,
					endTimeStamp, AlertingGenerator, MP_AlertingMsg,
					GeneratorCT);
			AG.Start();
			

			GenericAssetMetadataGenerator gaMetadataGenerator = new GenericAssetMetadataGenerator(
					sdiFileLocation, GenericAssetMetadata_Name,
					startSDITimeStamp, endTimeStamp, SDIPLC, GeneratorCT);
			new Thread(gaMetadataGenerator).start();

			GenericAssetGroupGenerator gaGroupGenerator = new GenericAssetGroupGenerator(
					sdiFileLocation, GenericAssetGroup_Name, startSDITimeStamp,
					endTimeStamp, SDIPLC, GeneratorCT);
			new Thread(gaGroupGenerator).start();

			RelationshipGenerator relationshipGenerator = new RelationshipGenerator(
					sdiFileLocation, Relationship_Name, startSDITimeStamp,
					endTimeStamp, SDIPLC, GeneratorCT);
			new Thread(relationshipGenerator).start();
			
			// Wait for all all file generators finishing work.
			GeneratorCT.WaitToDone();

			CA_AST_BASE_INFO.Delete();
			CA_AST_NAME_INFO.Delete();
			CA_AST_ORG_ASS_INFO.Delete();
			CA_AST_TYPE_COMMODITY_INFO.Delete();
			CA_AST_STATUS_INFO.Delete();
			CA_AST_LOCATION_INFO.Delete();
			CA_AST_COORDINATE_INFO.Delete();
			CA_AST_IDENTIFIER_INFO.Delete();
			CA_AST_IDENTIFIER_VALUE.Delete();
			CA_PLANT_COMMON_BASE_INFO.Delete();
			CA_PLANT_COMMON_NOTE_INFO.Delete();
			CA_PLANT_COMMON_STATUS_INFO.Delete();
			CA_PLANT_PLA_OUTAGE_INFO.Delete();
			CA_PLANT_PLA_COAL_MINE_INFO.Delete();
			CA_PLANT_PLA_STATISTIC_INFO.Delete();
			CA_PLANT_PGE_OUTAGE_INFO.Delete();
			CA_PLANT_PGE_OPERATOR_INFO.Delete();
			CA_PLANT_PGE_STATISTIC_INFO.Delete();
			CA_PLANT_PGE_ANALYTICS_INFO.Delete();
			CA_VESSEL_BASE_INFO.Delete();
			CA_VESSEL_LATEST_LOC_INFO.Delete();
			CA_VESSEL_OPEN_EVENT_INFO.Delete();
			CA_VESSEL_ORIGIN_INFO.Delete();
			CA_VESSEL_DESTINATION_INFO.Delete();
			CA_GENERIC_DATA_AGS_FACILITY.Delete();
			CA_GENERIC_DATA_AGS_STATISTIC.Delete();
			CA_GENERIC_DATA_BERTH.Delete();
			SDIPLC.AssetRelations.Delete();
			CA_VESSEL_EVENT_FACT.Delete();
			CA_VESSEL_LOCATION_FACT.Delete();

			SDIConstants.SDILogger.info(
							"[Physical Assets SDI V2]: All cursor dump files are removed");

			if (isProd) {
				// update status to success
				CompleteFileHis(FPH_ID);
			}

			BufferedWriter out = new BufferedWriter(new FileWriter(new File(
					sdiFileLocation, ManiFest_Name)));
			out.write(CommodityPhysicalAsset_Name);
			out.newLine();
			out.write(PhysicalAssetIdentifier_Name);
			out.newLine();
			out.write(PlantAsset_Name);
			out.newLine();
			out.write(VesselAsset_Name);
			out.newLine();
			out.write(GenericAsset_Name);
			out.newLine();
			out.write(GenericAssetMetadata_Name);
			out.newLine();
			out.write(GenericAssetGroup_Name);
			out.newLine();
			out.write(Relationship_Name);
			out.close();

			// ManiFest_Name_Inventory
			out = new BufferedWriter(new FileWriter(new File(sdiFileLocation,
					ManiFest_Name_Inventory)));
			out.write(CommodityPhysicalAsset_Name);
			out.newLine();
			out.write(Identifier_Name_v4);
			out.newLine();
			out.write(PlantAsset_Name);
			out.newLine();
			out.write(VesselAsset_Name);
			out.newLine();
			out.write(GenericAsset_Name);
			out.newLine();
			out.write(GenericAssetMetadata_Name);
			out.newLine();
			out.write(GenericAssetGroup_Name);
			out.newLine();
			out.write(Relationship_Name);
			out.close();

			TimeSpent = new Date().getTime() - StartTime;
			SDIConstants.SDILogger.info("[Physical Assets SDI V2]: Time spent on generating SDI files-------------------"
									+ TimeSpent);

			SearchSDIFullLoad.GenerateFullSDI(endTimeStamp);
			
			return ManiFest_Name;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "IOException", e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "Unknown Exception", e);
		}
		
		return "";

	}

	public static long CreateFileProcessHistory(String filename) {
		Connection conn = new EasyConnection(DBConnNames.CEF_CNR);
		long FPH_ID = 0;
		if (isProd) {
			try {
				DatabaseMetaData dmd = conn.getMetaData();
				PreparedStatement objPreStatement = conn.prepareStatement(
						InsertFileProcessHistory, new String[] { "ID" });
				objPreStatement.setString(1, filename);
				objPreStatement.setInt(2,
						FileCategory.getInstance("Search SDI V2").getID());
				objPreStatement.setInt(3, ProcessingStatus.PROCESSING.getID());

				objPreStatement.executeUpdate();

				// get ID
				if (dmd.supportsGetGeneratedKeys()) {
					ResultSet rs = objPreStatement.getGeneratedKeys();
					while (rs.next()) {
						FPH_ID = rs.getLong(1);
					}
				}

				conn.commit();
				objPreStatement.close();

			} catch (SQLException e) {
				SDIConstants.SDILogger.error(e);

			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					SDIConstants.SDILogger.error(e);
				}
			}
		}
		return FPH_ID;
	}

	private static Date getDBDate() {
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		PreparedStatement objPreStatement = null;
		ResultSet objResult = null;

		try {
			// //////////////////////////////////////////////////////////////////////
			// get attributes
			objPreStatement = DBConn.prepareStatement(CheckDBTime);
			objResult = objPreStatement.executeQuery();

			objResult.next();
			String strDBDate = objResult.getString(1);

			SimpleDateFormat formatter = new SimpleDateFormat(
					DateConstants.FULLTIMEFORMAT);
			ParsePosition pos = new ParsePosition(0);
			Date DBDate = formatter.parse(strDBDate, pos);

			objResult.close();
			objPreStatement.close();

			return DBDate;

		} catch (SQLException e) {
			SDIConstants.SDILogger.error(e);
			return null;

		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				SDIConstants.SDILogger.error(e);
			}
		}
	}

	public static String getStringDate(Date time) {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HHmm");
		String dateString = formatter.format(time);
		return dateString;
	}

	public static void CompleteFileHis(long FPH_ID) {
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

		try {
			// if processint_detail table has records, then it's
			// COMPLETEDWITHWARNING
			int pdeCount = -1;
			PreparedStatement getPdetPreStatement = DBConn
					.prepareStatement(GetProcessingDetail);
			getPdetPreStatement.setLong(1, FPH_ID);
			ResultSet objResultSet = getPdetPreStatement.executeQuery();
			if (objResultSet.next()) {
				pdeCount = objResultSet.getInt(1);
			}

			objResultSet.close();
			getPdetPreStatement.close();

			PreparedStatement objPreStatement = null;
			objPreStatement = DBConn.prepareStatement(CompleteFileHistory);

			if (pdeCount <= 0) {
				objPreStatement.setInt(1, ProcessingStatus.COMPLETED.getID());
			} else {
				objPreStatement.setInt(1,
						ProcessingStatus.COMPLETEDWITHWARNING.getID());
			}

			objPreStatement.setLong(2, FPH_ID);
			objPreStatement.executeUpdate();
			DBConn.commit();
			objPreStatement.close();

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

}
