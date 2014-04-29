package com.thomsonreuters.ce.taskcontroller.task.file.vtvessel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.Logger;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import au.com.bytecode.opencsv.CSVReader;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.file.FileProcessor;


public class VTVesselLoader extends FileProcessor {
	private Logger logger;

	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("VESSEL");
	}

	public void ProcessFile(File FeedFile) { 
		// TODO Auto-generated method stub
		String filename=FeedFile.getName();
		Connection DBConn = null;	
		CallableStatement callableorderSM=null;
		//		CallableStatement callableDelOrderSM=null;
		String file_name=FeedFile.getName();

		String[] columnnamelist;
		String[] sectionnamelist;
		String[] valuelistofcurrentrow = null;
		CallableStatement InsTaskCs = null;
		try {
			CSVReader thisreader = null;
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			ZipFile ZippedFile = new ZipFile(FeedFile);
			Enumeration ZippedFiles = ZippedFile.entries();			

			CallableStatement initializeCS = DBConn
					.prepareCall("{call vessel_load_pkg.initialize_vessel_cfg_proc}");
			initializeCS.execute();
			DBConn.commit();
			initializeCS.close();

			while (ZippedFiles.hasMoreElements()) {
				ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();

				InputStream IS = ZippedFile.getInputStream(entry);
				BufferedReader FR = new BufferedReader(new InputStreamReader(IS));			
				thisreader=new CSVReader(FR,',','"','\b',0,false);		
				columnnamelist = thisreader.readNext();
				sectionnamelist = thisreader.readNext();

int r=0;
				while(true){ 
					try{
						valuelistofcurrentrow = thisreader.readNext();
						if(valuelistofcurrentrow==null) break;
						ArrayList<Object> parmVector = new ArrayList<Object>();
						STRUCT parmStruct = null;
						StructDescriptor structdesc = StructDescriptor
								.createDescriptor("CEF_CNR.CE_VAR_NV_TYPE",
										DBConn);
						ArrayDescriptor descriptor = ArrayDescriptor
								.createDescriptor(
										"CEF_CNR.CE_VAR_NV_LST_T", DBConn);

						String[] parmsArray = new String[2];			

						for(int i=0; i<columnnamelist.length;i++)
						{
							parmsArray[0] = columnnamelist[i].trim().toUpperCase()+":"+sectionnamelist[i].trim().toUpperCase();
							parmsArray[1] = valuelistofcurrentrow[i].trim();
														
							if(parmsArray[1]!=null && !parmsArray[1].equals("")){
								parmStruct = new STRUCT(structdesc, DBConn,parmsArray);
								parmVector.add(parmStruct);	 
//								System.out.println(parmsArray[0]+":"+parmsArray[1]);
							}
						}
						parmsArray[0] = "VT DATA SOURCE:SOURCE";
						if(filename.toUpperCase().indexOf("MASTER")>0) parmsArray[1] = "MASTER DATA";
						else if(filename.toUpperCase().indexOf("NEW_BUILDING")>0) parmsArray[1] = "ORDERBOOK";
						parmStruct = new STRUCT(structdesc, DBConn,parmsArray);
						parmVector.add(parmStruct);	 
//						System.out.println(parmsArray[0]+":"+parmsArray[1]);
						Object obj_array[] = parmVector.toArray();
						ARRAY array = new ARRAY(descriptor, DBConn,
								obj_array);

						InsTaskCs = DBConn
								.prepareCall("{call cef_cnr.vessel_load_pkg.insert_vessel_proc(?)}");

						InsTaskCs.setArray(1, array);
						InsTaskCs.execute();
						DBConn.commit();
						InsTaskCs.close();
						r++;
//						System.out.println(r+" row inserted.");
					} catch (Exception e) {
						if(callableorderSM != null) callableorderSM.close();
						// TODO Auto-generated catch block
						this.LogDetails(MsgCategory.WARN, "VTVessel:Failed to insert row:" +toString(valuelistofcurrentrow)+ " because of exception: "+e.getMessage());
						logger.warn("VTVessel:Failed to insert row:" +toString(valuelistofcurrentrow)+ " because of exception: "+e.getMessage());
					}
				}

				thisreader.close();
				FR.close();				
			}
			ZippedFile.close();
		}catch (Exception e) {
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + file_name + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + file_name, e);
			//			throw new SystemException(e.getMessage(), e);
		}finally{
			try {				
				if(InsTaskCs!=null) InsTaskCs.close();
				DBConn.close();
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(),e);
			}	
		}

	}



	public String toString(String[] valuelistofcurrentrow) {
		// TODO Auto-generated method stub
		String row="";

		for(String i : valuelistofcurrentrow)
		{
			row=row+"\""+i+"\""+",";
		}

		return row;
	}
}

