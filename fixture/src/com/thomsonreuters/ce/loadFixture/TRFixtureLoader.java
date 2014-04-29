package com.thomsonreuters.ce.loadFixture;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.Logger;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.dbor.server.DBConnNames;


public class TRFixtureLoader extends FileProcessor{
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

			ZipFile ZippedFile = new ZipFile(FeedFile);
			Enumeration ZippedFiles = ZippedFile.entries();
			Object []rowObjects;
			int rownum=0;
			while (ZippedFiles.hasMoreElements()) {

				ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();

				InputStream IS = ZippedFile.getInputStream(entry);
				BufferedReader FR = new BufferedReader(new InputStreamReader(IS));
				CSVDataSet CDS=new CSVDataSet(FR,',','"','\b',0,false);
				String[] headers = CDS.columnnamelist;
				while(CDS.next())
				{
					try{

						rowObjects = CDS.valuelistofcurrentrow;

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
								parmsArray[0] = headers[i];
								parmsArray[1] = rowObjects[i].toString();
								parmStruct = new STRUCT(structdesc, DBConn,
										parmsArray);
								parmVector.add(parmStruct);
							}catch (SQLException e) {

								this.LogDetails(MsgCategory.ERROR, "Failed to parse field:" +  rowObjects[i] + " because of exception: "+e.getMessage());
								logger.error("Failed to parse field:" +  rowObjects[i] , e);
							}
						}
						Object obj_array[] = parmVector.toArray();
						ARRAY array = new ARRAY(descriptor, DBConn,
								obj_array);

						CallableStatement InsTaskCs = DBConn
								.prepareCall("{call  cef_cnr.fixture_load_pkg.insert_fixture_proc(?,?)}");
						InsTaskCs.setString(1, "TR");
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
				CallableStatement callableUpdatedate = DBConn
						.prepareCall("{call  ce_refresh_maintain_pkg.update_last_upd_date_proc('FIXTURE')}");			
				callableUpdatedate.execute();
				callableUpdatedate.close();
				CallableStatement callableUpdateTs = DBConn
						.prepareCall("{call  fixture_load_pkg.update_refresh_timestamp_proc}");			
				callableUpdateTs.execute();
				callableUpdateTs.close();
				DBConn.commit();
			}
			ZippedFile.close();
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

	@Override
	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("FIXTURE");
	}
}