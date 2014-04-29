package com.thomsonreuters.ce.OilUnit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.dbor.file.ExcelProcessor;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
 
public class OilUnitLoader extends FileProcessor {
	private Logger logger =Starter.thisLogger;
	public void ProcessFile(File FeedFile) { 

		Connection DBConn = null;	
		String file_name=FeedFile.getName();		
		CallableStatement callableInsRefSM = null;
		FileReader FR= null;
		try {
				DBConn = new EasyConnection(DBConnNames.CEF_CNR);
				callableInsRefSM = DBConn.prepareCall("{? = call mpd2_cnr.IIR_OILREFINERY_LOAD_PKG.insert_refinery_proc(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
//				int rowsOfCommit = 200;
//				int processedRowsNumber = 0;
				boolean new_task = false;
				java.sql.Timestamp startTime = null;
//				ZipFile ZippedFile = new ZipFile(FeedFile);
//				Enumeration ZippedFiles = ZippedFile.entries();
//				
//				while (ZippedFiles.hasMoreElements()) {
//					ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();
//					
//					InputStream IS = ZippedFile.getInputStream(entry);
//					BufferedReader FR = new BufferedReader(new InputStreamReader(IS));
					FR=new FileReader(FeedFile);
					CSVDataSet CDS=new CSVDataSet(FR,',','"','\b',0,false);
					startTime = getDBDate(DBConn);		
					while(CDS.next())
					{									
						
//						CallableStatement callableInsRefSM = DBConn.prepareCall("{call mpd2_cnr.IIR_OILREFINERY_LOAD_PKG.insert_units_proc(?,?,?,?,?,?,?,?,?,?,?)}");
						try{
							Long unit_id=null;				 
							String temp_unit_id=CDS.getValue("UNIT_ID");
							if (temp_unit_id != null && !temp_unit_id.equals(""))
							{
								unit_id=Long.valueOf(temp_unit_id);
							}
							String unit_name = CDS.getValue("UNIT_NAME");
							Long area_id=null;						 
							String temp_area_id=CDS.getValue("AREA_ID");
							if (temp_area_id != null && !temp_area_id.equals(""))
							{
								area_id=Long.valueOf(temp_area_id);
							}
							String area_name = CDS.getValue("AREA_NAME");
							Long plant_id=null;						 
							String temp_plant_id=CDS.getValue("PLANT_ID");
							if (temp_plant_id != null && !temp_plant_id.equals(""))
							{
								plant_id=Long.valueOf(temp_plant_id);
							}
							String plant_name = CDS.getValue("PLANT_NAME");
							Long owner_id=null;						 
							String temp_owner_id=CDS.getValue("OWNER_ID");
							if (temp_owner_id != null && !temp_owner_id.equals(""))
							{
								owner_id=Long.valueOf(temp_owner_id);
							}
							String owner_name = CDS.getValue("OWNER_NAME");
							String county_id = CDS.getValue("COUNTY_ID");
							String unit_state = CDS.getValue("UNIT_STATE");
							String u_status = CDS.getValue("U_STATUS");
							String temp_startup = CDS.getValue("STARTUP");
							Date startup=null;
							if (temp_startup != null && !temp_startup.equals(""))
							{
								startup=strToDate(temp_startup);
							}
							String unit_type = CDS.getValue("UNIT_TYPE");						
							Float f_capacity=null;						 
							String temp_cap_id=CDS.getValue("U_CAPACITY");
							if (temp_cap_id !=null && !temp_cap_id.equals(""))
							{
								f_capacity=Float.valueOf(temp_cap_id);
							}
							String cap_uom = CDS.getValue("CAP_UOM");
							String sic_code = CDS.getValue("SIC_CODE");
							String temp_pad_dist = CDS.getValue("PAD_DIST");
							Integer pad_dist=null;
							if(temp_pad_dist!=null && !temp_pad_dist.equals(""))
							{
								pad_dist = Integer.valueOf(temp_pad_dist);
							}
	//						String temp_count = CDS.getValue("COUNT");
	//						Integer count =null;
	//						if(!temp_count.equals(""))
	//						{
	//							count = Integer.valueOf(temp_count);
	//						}
							String temp_lat = CDS.getValue("LATITUDE");
						    Double latitude = null;
						    if(temp_lat != null && !temp_lat.equals(""))
						    {
						    	latitude = Double.valueOf(temp_lat);
						    }
						    String temp_lon = CDS.getValue("LONGITUDE");
						    Double longitude = null;
						    if(temp_lon !=null && !temp_lon.equals(""))
						    {
						    	longitude = Double.valueOf(temp_lon);
						    }
						    String[] temp = file_name.split("_");
						    String temp_date = temp[2].substring(0, 8);
						    Date file_date_in = strToDate(temp_date);
						    String status = CDS.getValue("STATUS");
						    String temp_shutdownDate = CDS.getValue("SHUTDOWN");
						    Date shutdownDate=null;
							if (temp_shutdownDate != null && !temp_shutdownDate.equals(""))
							{
								shutdownDate=strToDate(temp_shutdownDate);
							}
						    
						    callableInsRefSM.registerOutParameter(1, Types.BIGINT);
						    
						    if(plant_id != null)
						    	callableInsRefSM.setLong(2, plant_id.longValue());
						    else
						    	callableInsRefSM.setNull(2, Types.BIGINT);
						    callableInsRefSM.setString(3, plant_name);
						    if(owner_id != null)
						    	callableInsRefSM.setLong(4, owner_id.longValue());
						    else
						    	callableInsRefSM.setNull(4, Types.BIGINT);
						    callableInsRefSM.setString(5, owner_name);	
						    
						    callableInsRefSM.setString(6, county_id);
						    callableInsRefSM.setString(7, sic_code);
						    if(pad_dist!=null)
						    	callableInsRefSM.setInt(8, pad_dist.intValue());
						    else
						    	callableInsRefSM.setNull(8, Types.INTEGER);
						    if(latitude!=null)
						    	callableInsRefSM.setDouble(9, latitude.doubleValue());
						    else
						    	callableInsRefSM.setNull(9, Types.DOUBLE);
						    if(longitude!=null)
						    	callableInsRefSM.setDouble(10, longitude.doubleValue());
						    else
						    	callableInsRefSM.setNull(10, Types.DOUBLE);						    
						   
						    
						    if(unit_id != null)
						    {
						    	callableInsRefSM.setLong(11, unit_id.longValue());
						    }
						    else 
						    {
						    	callableInsRefSM.setNull(11, Types.BIGINT);
						    }
						    callableInsRefSM.setString(12, unit_name);
						    
						    if(area_id != null)
						    {
						    	callableInsRefSM.setLong(13, area_id.longValue());
						    }
						    else
						    {
						    	callableInsRefSM.setNull(13, Types.BIGINT);
						    }
						    callableInsRefSM.setString(14, area_name);
						    callableInsRefSM.setString(15, u_status);
						    if (startup!=null)
							{
						    	callableInsRefSM.setDate(16, startup);
							}
							else
							{
								callableInsRefSM.setNull(16, Types.DATE);		    	
							}	    					
						    callableInsRefSM.setString(17, unit_type);
						    if(f_capacity != null)
						    {
						    	callableInsRefSM.setFloat(18, f_capacity.floatValue());
						    }
						    else
						    {
						    	callableInsRefSM.setNull(18, Types.FLOAT);
						    }
						    
						    callableInsRefSM.setString(19, cap_uom);
						    callableInsRefSM.setDate(20,file_date_in);
						    callableInsRefSM.setString(21,file_name);
						    callableInsRefSM.setDate(22,shutdownDate);
						    callableInsRefSM.execute();						  
						    
						    long tmp_tad_id = -1;
						    tmp_tad_id = callableInsRefSM.getLong(1);
						    
							if(tmp_tad_id != -1) new_task = true;
							DBConn.commit();	
//							processedRowsNumber++; 
//							if (processedRowsNumber == rowsOfCommit) {
//								DBConn.commit();				 
//								processedRowsNumber = 0;
//							}						 
							
						}catch (Exception e) {
							// TODO Auto-generated catch block
							if(e.getMessage().indexOf("-20999")>0){
								this.LogDetails(MsgCategory.WARN, "Failed to insert row:" +CDS.toString()+ e.getMessage());
								
							}else{
							this.LogDetails(MsgCategory.WARN, "Failed to insert row:" +CDS.toString()+ " because of exception: "+e.getMessage());
							logger.error("Failed to insert row:" +CDS.toString(), e);
							}
						} 
					}
//				DBConn.commit(); 
				callableInsRefSM.close();					
				FR.close();
				
				java.sql.Timestamp sysdate = currentDate();
				CallableStatement csSetTime = DBConn.prepareCall("{call cef_cnr.REPORT_MAINTAIN_PKG.UPDATE_DATA_SET_TIMESTAMP_PROC(?,?)}"); 
				csSetTime.setString(1,"IIR OIL REFINERY");
				csSetTime.setTimestamp(2,sysdate);
				csSetTime.execute();
				csSetTime.close();
				DBConn.commit();
				
				if(new_task) {
					CallableStatement InsTaskCs = DBConn.prepareCall("{? = call cef_cnr.TASK_MAINTAIN_PKG.INSERT_TASK_QUEUE_FUNC(?,?,?)}");

					java.sql.Timestamp nowTime = getDBDate(DBConn);
					InsTaskCs.registerOutParameter(1, Types.BIGINT);
					InsTaskCs.setString(2, "IIR_TS");
					InsTaskCs.setTimestamp(3, startTime);
					InsTaskCs.setTimestamp(4, nowTime);
					InsTaskCs.execute();
					DBConn.commit();	
//					Long taqID = InsTaskCs.getLong(1);
					InsTaskCs.close();
				}
				 	
		}catch (Exception e) {
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + file_name + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + file_name, e);
			throw new SystemException(e.getMessage(), e);
		}
		finally
		{
			try {
				DBConn.close();
				if(callableInsRefSM!=null) callableInsRefSM.close();					
				FR.close();				
			} catch (Exception e) {
				throw new SystemException(e.getMessage(),e);
			}	
		}
		
	}
	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("OIL UNIT");
	}
	private Date strToDate(String strDate) { 
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd"); 
		ParsePosition pos = new ParsePosition(0); 
		Date d = new Date(formatter.parse(strDate, pos).getTime()); 
		return d; 
	} 	
	
	public java.sql.Timestamp currentDate() {
		java.util.Date longdate = new java.util.Date();
		java.sql.Date date = new java.sql.Date(longdate.getTime());
		java.sql.Time time = new java.sql.Time(longdate.getTime());
		java.sql.Timestamp curdate = java.sql.Timestamp.valueOf(date.toString()
				+ " " + time.toString());

		return curdate;
	}
	
public java.sql.Timestamp getDBDate(Connection DBConn) {
		
		String SQL_1 = "select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual";
		String strDate = null;
		try {
			PreparedStatement ps = DBConn.prepareStatement(SQL_1);
			ResultSet objResult = ps.executeQuery();
			if (objResult.next()) {
				strDate = objResult.getString(1);
			}
			objResult.close();
			ps.close();
		} catch (SQLException e) {
			throw new LogicException(
					"Get sysdate from database error.");
		}
		java.sql.Timestamp curdate = java.sql.Timestamp.valueOf(strDate);
        return curdate;
    }
}