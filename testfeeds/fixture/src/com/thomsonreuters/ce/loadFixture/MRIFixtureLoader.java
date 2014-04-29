package com.thomsonreuters.ce.loadFixture;
import java.io.*;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import com.linuxense.javadbf.*;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;

public class MRIFixtureLoader extends FileProcessor{

	private Logger logger;
	public void ProcessFile(File FeedFile) {
		InputStream inputStream;
		Connection DBConn = null;	
		String filename = FeedFile.getName();

		try {

			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement callableInitialize = DBConn
					.prepareCall("{call  cef_cnr.fixture_load_pkg.initialize_session_variables}");			
			callableInitialize.execute();
			callableInitialize.close();
			 
 
			inputStream = new FileInputStream( FeedFile);
			DBFReader reader = new DBFReader( inputStream); 
			int numberOfFields = reader.getFieldCount();
			String[] headerArray = new String[numberOfFields];
			for( int i=0; i<numberOfFields; i++) {

				DBFField field = reader.getField( i);
				field.setDataType((byte)'C');
				headerArray[i] = field.getName();
//				System.out.println( field.getName());
			}

			Object []rowObjects;
			int rownum=0;
//			String format = "MM/dd/yyyy";
//			SimpleDateFormat stf = new SimpleDateFormat(format);
			  
			while( (rowObjects = reader.nextRecord()) != null) {				
				try{
					ArrayList<Object> parmVector = new ArrayList<Object>();
					STRUCT parmStruct = null;
					StructDescriptor structdesc;				
					structdesc = StructDescriptor
							.createDescriptor("CEF_CNR.CE_VAR_NV_TYPE",
									DBConn);

					ArrayDescriptor descriptor = ArrayDescriptor
							.createDescriptor(
									"CEF_CNR.CE_VAR_NV_LST_T", DBConn);

					String[] parmsArray = new String[2];			 

					for( int i=0; i<rowObjects.length; i++) {
						try {
//							System.out.println( rowObjects[i]);
							
//							String dateContentS = null;
//							if(headerArray[i].equals("WEEK_ENDIN")) {
//								parmsArray[1] = stf.format(rowObjects[i]);
//							}else{
//								parmsArray[1] = rowObjects[i].toString();
//							}
							 
							parmsArray[0] = headerArray[i];
							parmsArray[1] = rowObjects[i].toString();
							parmStruct = new STRUCT(structdesc, DBConn,
									parmsArray);
							parmVector.add(parmStruct);
//							System.out.println(parmsArray[0]+":"+parmsArray[1]);
//							if(parmsArray[0].equals("COMMODITY")) {
//								System.out.println(parmsArray[0]+":"+parmsArray[1]);
//
//							}

						} catch (SQLException e) {

							this.LogDetails(MsgCategory.ERROR, "Failed to parse field:" +  rowObjects[i] + " because of exception: "+e.getMessage());
							logger.error("Failed to parse field:" +  rowObjects[i] , e);
						}
					}

					Object obj_array[] = parmVector.toArray();
					ARRAY array = new ARRAY(descriptor, DBConn,
							obj_array);

					CallableStatement InsTaskCs = DBConn
							.prepareCall("{call  cef_cnr.fixture_load_pkg.insert_fixture_proc(?,?)}");
					InsTaskCs.setString(1, "MRI");
					InsTaskCs.setArray(2, array);
					InsTaskCs.execute();
					DBConn.commit();
					InsTaskCs.close();
				} catch (Exception e) {
					this.LogDetails(MsgCategory.ERROR, "Failed to parse row " +rownum  + " because of exception: "+e.getMessage());
					logger.error("Failed to parse row:" +rownum , e);
				}
				rownum++;				
			}
			inputStream.close();
			CallableStatement callableUpdatedate = DBConn
					.prepareCall("{call  ce_refresh_maintain_pkg.update_last_upd_date_proc('FIXTURE')}");			
			callableUpdatedate.execute();
			callableUpdatedate.close();
			CallableStatement callableUpdateTs = DBConn
					.prepareCall("{call  fixture_load_pkg.update_refresh_timestamp_proc}");			
			callableUpdateTs.execute();
			callableUpdateTs.close();
			DBConn.commit();
		}catch(Exception e){
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + filename + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + filename, e);
		}finally{
			try {				
				CallableStatement callableReleasee = DBConn
						.prepareCall("{call  cef_cnr.fixture_load_pkg.release_session_variables}");			
				callableReleasee.execute();
				callableReleasee.close();
				DBConn.commit();
				DBConn.close();
				
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(),e);
			}	
		}
	}


	public FileCategory getFileCatory(File FeedFile) {

		return FileCategory.getInstance("MRIFixtureLoader");
	}
	
 

}


