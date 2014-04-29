package com.thomsonreuters.ce.IIROutageLoader;

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
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.IIROutageLoader.Starter;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

import org.apache.log4j.Logger;

public class IIROutageLoader extends FileProcessor{
	private Logger logger  = Starter.thisLogger;
	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("IIROutage");
	}
	
	
	public void ProcessFile(File FeedFile) { 
		
		Connection DBConn = null;	
	
		String file_name=FeedFile.getName();
//		int rowsOfCommit = 1;
//		int processedRowsNumber = 0;
		CallableStatement callableInsUnitOutageSM = null;
		boolean new_task = false;
		java.sql.Timestamp startTime = null;
		FileReader FR= null;
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
//			ZipFile ZippedFile = new ZipFile(FeedFile);
//			Enumeration ZippedFiles = ZippedFile.entries();
			callableInsUnitOutageSM = DBConn.prepareCall("{ ? = call mpd2_cnr.IIR_OILREFINERY_LOAD_PKG.insert_unit_outage_func(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");

			FR=new FileReader(FeedFile);
			CSVDataSet CDS=new CSVDataSet(FR,',','"','\b',0,false);
//			while (ZippedFiles.hasMoreElements()) {
//				ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();
//				
//				InputStream IS = ZippedFile.getInputStream(entry);
//				BufferedReader FR = new BufferedReader(new InputStreamReader(IS));
//				
//				CSVDataSet CDS=new CSVDataSet(FR,',','"','\b',0,false);
				startTime = getDBDate(DBConn);
				while(CDS.next())
				{
					try{
						Long outage_id=null;
						String temp_outage_id=CDS.getValue("OUTAGE_ID");
						if (temp_outage_id != null && !temp_outage_id.equals(""))
						{
							outage_id=Long.valueOf(temp_outage_id);
						}
						
	//					String UNIT_NAME = CDS.getValue("UNIT_NAME");
						
						Long unit_id=null;
						String temp_unit_id=CDS.getValue("UNIT_ID");
						if (temp_unit_id != null && !temp_unit_id.equals(""))
						{
							unit_id=Long.valueOf(temp_unit_id);
						}
						
						String OUTAGE_TYPE = CDS.getValue("OUTAGE_TYPE").toUpperCase();
						
						String OUTAGE_STATUS = CDS.getValue("OUTAGE_STATUS").toUpperCase();
						
						Date ta_start=null;
						String temp_begin_date=CDS.getValue("TA_START");
						if (temp_begin_date!=null && !temp_begin_date.equals(""))
						{
							try{
								ta_start=strToDate(temp_begin_date);
							}catch(Exception e){
								this.LogDetails(MsgCategory.WARN, "StrToDate failed :" +temp_begin_date+ "  is not in format: yyyy-MM-dd." );
				    			logger.error("StrToDate failed :" +temp_begin_date+ "  is not in format: yyyy-MM-dd. "+e.getMessage(), e);
							}
						}
						 
	
						Date ta_end=null;
						String temp_end_date=CDS.getValue("TA_END");
						if (temp_end_date!=null && !temp_end_date.equals(""))
						{
							try{
								ta_end=strToDate(temp_end_date);
							}catch(Exception e){
								this.LogDetails(MsgCategory.WARN, "StrToDate failed :" +temp_end_date+ "  is not in format: yyyy-MM-dd." );
								logger.error("StrToDate failed :" +temp_end_date+ "  is not in format: yyyy-MM-dd. "+e.getMessage(), e);
							}
						}				 
						
						Integer ta_duration=null;
						String temp_ta_duration=CDS.getValue("TA_DURATION");
						if (temp_ta_duration!=null && !temp_ta_duration.equals(""))
						{
							ta_duration=Integer.valueOf(temp_ta_duration);
						}
						
						String PRECISION = CDS.getValue("PRECISION").toUpperCase();
											
						Long chargerate=null;
						String temp_chargegrate=CDS.getValue("CHARGERATE");
						if (temp_chargegrate!=null && !temp_chargegrate.equals(""))
						{
							chargerate=Long.valueOf(temp_chargegrate);
						}					
						
						Long capacityoff=null;
						String temp_capacity=CDS.getValue("CAPACITY_OFF");
						if (temp_capacity!=null && !temp_capacity.equals(""))
						{
							capacityoff=Long.valueOf(temp_capacity);
						}
						
						Double derate = null; 
						String temp_derate = CDS.getValue("DERATE");
						if(temp_derate!=null && !temp_derate.equals(""))
						{
							derate = Double.valueOf(temp_derate);
						}
						
						String OUT_CAUSE = CDS.getValue("OUT_CAUSE");
											
						Date delv_date=null;
						String temp_delv_date=CDS.getValue("DELV_DATE");
						if (temp_delv_date!=null && !temp_delv_date.equals(""))
						{
							try{
								delv_date=strToDate(temp_delv_date);
							}catch(Exception e){
								this.LogDetails(MsgCategory.WARN, "StrToDate failed :" +temp_delv_date+ "  is not in format: yyyy-MM-dd." );
								logger.error("StrToDate failed :" +temp_delv_date+ "  is not in format: yyyy-MM-dd. "+e.getMessage(), e);
							}
						}
						 
						String unit_type_group=CDS.getValue("UNIT_TYPE_GROUP");;
						
						//////////////////////////////////////////////////
						//Parsing file done
						//////////////////////////////////////////////////

						callableInsUnitOutageSM.registerOutParameter(1, Types.BIGINT);
						
						if (outage_id!=null)
						{
						callableInsUnitOutageSM.setLong(2,-outage_id.longValue());
						}
						else
						{
						callableInsUnitOutageSM.setNull(2, Types.BIGINT);		    	
						}
						
						if (unit_id!=null)
						{
						callableInsUnitOutageSM.setLong(3,unit_id.longValue());
						}
						else
						{
						callableInsUnitOutageSM.setNull(3, Types.BIGINT);		    	
						}
															
						callableInsUnitOutageSM.setString(4, OUTAGE_TYPE);
						
						callableInsUnitOutageSM.setString(5, OUTAGE_STATUS);
						
						callableInsUnitOutageSM.setDate(6, ta_start);
						    	
						callableInsUnitOutageSM.setDate(7, ta_end);
							 
						if (ta_duration!=null)
						{
						callableInsUnitOutageSM.setInt(8,ta_duration.intValue());
						}
						else
						{
						callableInsUnitOutageSM.setNull(8, Types.INTEGER);		    	
						}
						
						callableInsUnitOutageSM.setString(9, PRECISION);
						
						if (chargerate!=null)
						{
						callableInsUnitOutageSM.setLong(10,chargerate.longValue());
						}
						else
						{
						callableInsUnitOutageSM.setNull(10, Types.BIGINT);		    	
						}
						
						if (capacityoff!=null)
						{
						callableInsUnitOutageSM.setLong(11,capacityoff.longValue());
						}
						else
						{
						callableInsUnitOutageSM.setNull(11, Types.BIGINT);		    	
						}
						
						if(derate != null)
						{
							callableInsUnitOutageSM.setDouble(12, derate);
						}
						else
						{ 
							callableInsUnitOutageSM.setNull(12, Types.DOUBLE);		   
						}
						
						callableInsUnitOutageSM.setString(13, OUT_CAUSE);
						callableInsUnitOutageSM.setDate(14, delv_date);
						 callableInsUnitOutageSM.setString(15, unit_type_group);
												
						callableInsUnitOutageSM.execute();
						long tmp_tad_id = 0;
						tmp_tad_id = callableInsUnitOutageSM.getLong(1);
						if( tmp_tad_id != 0) new_task = true; 
						DBConn.commit();	 
//						processedRowsNumber++;
//						if (processedRowsNumber == rowsOfCommit) {
//							DBConn.commit();				
//							processedRowsNumber = 0;
//						}
						
				} catch (Exception e) {
					// TODO Auto-generated catch block
					this.LogDetails(MsgCategory.WARN, "Failed to insert row:" +CDS.toString()+ " because of exception: "+e.getMessage());
					logger.error("Failed to insert row:" +CDS.toString(), e);
				}												 
			}
//			DBConn.commit();
			callableInsUnitOutageSM.close();
			FR.close();
			
			java.sql.Timestamp sysdate = getDBDate(DBConn);
			CallableStatement csSetTime = DBConn.prepareCall("{call cef_cnr.REPORT_MAINTAIN_PKG.UPDATE_DATA_SET_TIMESTAMP_PROC(?,?)}"); 
			csSetTime.setString(1,"IIR OIL OUTAGE");
			csSetTime.setTimestamp(2,sysdate);
			csSetTime.execute();			
			DBConn.commit();
			csSetTime.close();
			
			if(new_task) {
				CallableStatement InsTaskCs = DBConn.prepareCall("{? = call cef_cnr.TASK_MAINTAIN_PKG.INSERT_TASK_QUEUE_FUNC(?,?,?)}");

				java.sql.Timestamp nowTime = getDBDate(DBConn);
				InsTaskCs.registerOutParameter(1, Types.BIGINT);
				InsTaskCs.setString(2, "IIR_TS");
				InsTaskCs.setTimestamp(3, startTime);
				InsTaskCs.setTimestamp(4, nowTime);
				InsTaskCs.execute();
				DBConn.commit();	
//				Long taqID = InsTaskCs.getLong(1);
				InsTaskCs.close();
			}

//		 }
	   }catch (Exception e) {
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + file_name + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + file_name, e);
			throw new SystemException(e.getMessage(), e);
		}
		finally
		{
			try {								
				DBConn.close();
				if(callableInsUnitOutageSM!=null) callableInsUnitOutageSM.close();					
				FR.close();	
			} catch (Exception e) {
				throw new SystemException(e.getMessage(),e);
			}	
		}
	}
	
	private Date strToDate(String strDate) throws ParseException{ 
		 
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
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
