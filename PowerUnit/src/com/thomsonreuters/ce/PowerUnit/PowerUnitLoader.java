package com.thomsonreuters.ce.PowerUnit;

import java.io.File;
import java.io.FileReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Vector;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.file.FileProcessor;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

public class PowerUnitLoader extends FileProcessor {
	private Logger logger = Starter.thisLogger;
	Vector<Object> orgVector = new Vector<Object>();
	Vector<Object> percentVector = new Vector<Object>();
	String facility_id=null;	
	String last_cds=null;	
	Long perm_id=null;
	Long parent_perm_id=null;		
	String temp_perm_id=null;
	String temp_parent_id=null;
	String facility_name=null;
	String gun=null;
	String status=null;
	String ult_owner=null;
	String owner_name=null;
	String commodity=null;
	String facility_type=null;
	Double latitude = null;
	Double longitude = null;
	String fuel=null;
	String technology=null;
	Long fuel_id=null;		
	Long technology_id=null;		
	Float f_value=null;
	Date valid_from_date=null;
	Date valid_to_date=null;
	String shareholder =null;
	String percent=null;
	String temp_facility_id= null;

	
	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("POWER GENERATOR");
	}

	public void ProcessFile(File FeedFile) { 
		// TODO Auto-generated method stub
		Connection DBConn = null;	
		String file_name=FeedFile.getName();		

		try {
				DBConn = new EasyConnection(DBConnNames.CEF_CNR);
//				FileReader FR=new FileReader(FeedFile);
//				CSVDataSet CDS1 =new CSVDataSet(new InputStreamReader(new FileInputStream(FeedFile), "ISO8859_1"),',','"','\b',0,false);
//				CSVDataSet CDS2 =new CSVDataSet(new InputStreamReader(new FileInputStream(FeedFile), "ISO8859_1"),',','"','\b',0,false);
				CSVDataSet CDS1 =new CSVDataSet(new InputStreamReader(new FileInputStream(FeedFile), "UTF-8"),',','"','\b',0,false);
				CSVDataSet CDS2 =new CSVDataSet(new InputStreamReader(new FileInputStream(FeedFile), "UTF-8"),',','"','\b',0,false);
//				CSVDataSet CDS1=new CSVDataSet(FR,',','"','\b',0,false);
//				CSVDataSet CDS2=new CSVDataSet(FR,',','"','\b',0,false);
				ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor(
						"CEF_CNR.CE_STR_LST_T", DBConn);
				//first loop, only process non-generator rows
				while(CDS1.next())
				{	
							facility_type = CDS1.getValue("FACILITY_TYPE").toUpperCase();
							if(!facility_type.equals("GENERATOR"))
							{
								temp_facility_id=CDS1.getValue("FACILITY_ID");
								executeSM(DBConn,descriptor);
								parseData(CDS1);
							}
							
				 }//end of while
				//insert last record
				if(!facility_type.equals("GENERATOR"))
				{
					executeSM(DBConn,descriptor);
				}
				CDS1.close();
				//second loop the file, only process generator
				while(CDS2.next())
				{	
							facility_type = CDS2.getValue("FACILITY_TYPE").toUpperCase();
							if(facility_type.equals("GENERATOR"))
							{
								temp_facility_id=CDS2.getValue("FACILITY_ID");
								executeSM(DBConn,descriptor);
								parseData(CDS2);
							}
							
				}//end of while
				//insert last record
				if(facility_type.equals("GENERATOR"))
				{
					executeSM(DBConn,descriptor);
				}
				CDS2.close();
				
		}catch (Exception e) {
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + file_name + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + file_name, e);
			throw new SystemException(e.getMessage(), e);
		}finally
		{
			try {
				DBConn.close();
				
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(),e);
			}	
		}
	}
	
	private void parseData(CSVDataSet CDS){
		
		temp_perm_id=CDS.getValue("PERM_ID");
		if (temp_perm_id != null && !temp_perm_id.equals(""))
		{
			perm_id=Long.valueOf(temp_perm_id);
		} 
				 
		temp_parent_id=CDS.getValue("PARENT_ID");
	 
		String temp_parent_perm_id=CDS.getValue("PARENT_PERM_ID");
		if (temp_parent_perm_id != null && !temp_parent_perm_id.equals(""))
		{
			parent_perm_id=Long.valueOf(temp_parent_perm_id);
		}
		facility_name = CDS.getValue("FACILITY_NAME");
		 
//		System.out.println(facility_name);
		String short_name = CDS.getValue("SHORT_NAME");
		String temp_facility_desc = CDS.getValue("FACILITY_DESCRIPTION");

		if(temp_facility_desc!=null && temp_facility_desc.startsWith("U_STATUS")){
			String[] temp_status = temp_facility_desc.split(";");
			status = (temp_status[0].split("-"))[1];
		}

		String temp_facility_comments = CDS.getValue("FACILITY_COMMENTS");
		if(temp_facility_comments != null && !temp_facility_comments.equals("") && temp_facility_comments.startsWith("ULT_OWNER")){
			String[] temp_owner = temp_facility_comments.split(";");
			if(temp_owner[0]!=null && !temp_owner[0].equals("") )
			{
				String[] temp0 = temp_owner[0].split("-");
				ult_owner = temp0[1];
			}
			if(temp_owner.length>1 && temp_owner[1]!=null && !temp_owner[1].equals("") )
			{
				String[] temp1 = temp_owner[1].split("-");
				owner_name = temp1[1];
			}							
		} 
		
		shareholder = CDS.getValue("ORGANISATION");
		percent = CDS.getValue("PERCENTAGE");		
		if(shareholder != null && !shareholder.equals(""))
		{
		   orgVector.add(shareholder);
		   percentVector.add(percent);
		}

		
		String temp_lat = CDS.getValue("LATITUDE");

		if(temp_lat!=null && !temp_lat.equals(""))
		    {
		    	latitude = Double.valueOf(temp_lat.replace(",", ""));
		    }
		String temp_lon = CDS.getValue("LONGITUDE");
		
		if(temp_lon!=null && !temp_lon.equals(""))
		    {
		    	longitude = Double.valueOf(temp_lon.replace(",", ""));
		    }

		commodity = CDS.getValue("COMMODITY").toUpperCase();
								 		 
		String temp_fuel_id=CDS.getValue("FUEL_ID");
		if (temp_fuel_id!=null && !temp_fuel_id.equals(""))
		{
			fuel_id=Long.valueOf(temp_fuel_id);
		}
		fuel = CDS.getValue("FUEL");
				 
		String temp_technology_id=CDS.getValue("TECHNOLOGY_ID");
		if (temp_technology_id!=null && !temp_technology_id.equals(""))
		{
			technology_id=Long.valueOf(temp_technology_id);
		}
		technology = CDS.getValue("TECHNOLOGY");
		
		String temp_value=CDS.getValue("VALUE");
		if (temp_value!=null && !temp_value.equals(""))
		{
			f_value=Float.valueOf(temp_value.replace(",", "."));
		}
		String temp_valid_from_date = CDS.getValue("VALID_FROM_DATE");

		if (temp_valid_from_date!=null && !temp_valid_from_date.equals(""))
		{
			valid_from_date=strToDate(temp_valid_from_date.substring(0,10));
		}
		String temp_valid_to_date = CDS.getValue("VALID_TO_DATE");
		
		if (temp_valid_to_date!=null && !temp_valid_to_date.equals(""))
		{
			valid_to_date=strToDate(temp_valid_to_date.substring(0,10));
			
		}														
		
		gun = CDS.getValue("GEOGRAPHIC_UNIT");
		last_cds = CDS.toString();
	}
	
	private void executeSM(Connection DBConn,ArrayDescriptor descriptor){
		
			if(facility_id == null ) facility_id = temp_facility_id;
			if(!facility_id.equals(temp_facility_id))
			{
				CallableStatement callableInsPwrSM = null;
				try{
				callableInsPwrSM = DBConn.prepareCall("{call mpd2_cnr.iir_power_plant_load_pkg.insert_power_facility_proc(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
				callableInsPwrSM.setString(1, facility_id);
				if(perm_id != null)
					callableInsPwrSM.setLong(2, perm_id.longValue());
			    else
			    	callableInsPwrSM.setNull(2, Types.BIGINT);
				callableInsPwrSM.setString(3, temp_parent_id);
				if(parent_perm_id != null)
					callableInsPwrSM.setLong(4, parent_perm_id.longValue());
			    else
			    	callableInsPwrSM.setNull(4, Types.BIGINT);
				
				callableInsPwrSM.setString(5, facility_name);
				callableInsPwrSM.setString(6, gun);
				callableInsPwrSM.setString(7, status);
				callableInsPwrSM.setString(8, ult_owner);
				callableInsPwrSM.setString(9, owner_name);
				callableInsPwrSM.setString(10, commodity);
				Object obj_array[] = orgVector.toArray();
				ARRAY orgArray = new ARRAY(descriptor, DBConn, obj_array);
				Object pct_array[] = percentVector.toArray();
				ARRAY pctArray = new ARRAY(descriptor, DBConn, pct_array);
				callableInsPwrSM.setArray(11, orgArray);
				callableInsPwrSM.setArray(12, pctArray);

				callableInsPwrSM.setString(13, facility_type);
				
				if(longitude!=null)
					callableInsPwrSM.setDouble(14, longitude.doubleValue());
			    else
			    	callableInsPwrSM.setNull(14, Types.DOUBLE);
				if(latitude!=null)
					callableInsPwrSM.setDouble(15, latitude.doubleValue());
			    else
			    	callableInsPwrSM.setNull(15, Types.DOUBLE);
				if(fuel_id != null)
					callableInsPwrSM.setLong(16, fuel_id);
				else
					callableInsPwrSM.setNull(16, Types.BIGINT);
				callableInsPwrSM.setString(17, fuel);
				if(technology_id != null)
					callableInsPwrSM.setLong(18, technology_id);
				else
					callableInsPwrSM.setNull(18, Types.BIGINT);
				callableInsPwrSM.setString(19, technology);
				if(f_value != null)
					  callableInsPwrSM.setFloat(20, f_value.floatValue());
			    else
				      callableInsPwrSM.setNull(20, Types.FLOAT);
				if(valid_from_date != null)
				      callableInsPwrSM.setDate(21, valid_from_date);
				else
					  callableInsPwrSM.setNull(21, Types.DATE);
				if(valid_to_date != null)
				      callableInsPwrSM.setDate(22, valid_to_date);
				else
					  callableInsPwrSM.setNull(22, Types.DATE);   										
				
					callableInsPwrSM.execute();
					callableInsPwrSM.close();
					DBConn.commit();	
				}catch (Exception e) {
					// TODO Auto-generated catch block
					this.LogDetails(MsgCategory.WARN, "Failed to insert row:"+last_cds+ " because of exception: "+e.getMessage());
					if(e.getMessage().indexOf("-20003")<0)//too many 'no valid asset type errors', not log it in file
						logger.error("Failed to insert row: " +last_cds, e);
				}finally{
					percentVector.clear();
					orgVector.clear();			
					facility_id = temp_facility_id;		
					temp_facility_id = null;
					perm_id = null;
					parent_perm_id = null ;
					status = null;
					ult_owner = null;
					owner_name = null;
					latitude = null;
					longitude = null;
					fuel_id = null;
					technology_id = null;
					f_value = null;
					valid_from_date = null;
					valid_to_date = null;
					try{
						callableInsPwrSM.close();
					}catch(SQLException e){
						this.LogDetails(MsgCategory.WARN, "Failed to close statement:"+e.getMessage());
						logger.error("Failed to close statement:" +last_cds, e);
					}
				}
			}
		
	}
	private Date strToDate(String strDate) { 
	  try{
		SimpleDateFormat formatter = new SimpleDateFormat("dd.MM.yyyy",
				Locale.ENGLISH);
		java.util.Date date = formatter.parse(strDate);
		java.sql.Date sqlDate = new java.sql.Date(date.getTime());
		
		return sqlDate; 
	  }catch(Exception e){
		    this.LogDetails(MsgCategory.ERROR, "strToDate failed:" + strDate + " is not in format: dd.MM.yyyy"+e.getMessage());
		    logger.error("strToDate failed:" + strDate + " is not in format: dd.MM.yyyy", e);
			return null;
	  }
	} 	
}
