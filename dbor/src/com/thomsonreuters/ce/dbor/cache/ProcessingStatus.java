package com.thomsonreuters.ce.dbor.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.HashMap;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exception.SystemException;


public class ProcessingStatus {
	
	public static ProcessingStatus PROCESSING=ProcessingStatus.getInstance("PROCESSING");
	public static ProcessingStatus COMPLETED=ProcessingStatus.getInstance("COMPLETED");
	public static ProcessingStatus FAILED=ProcessingStatus.getInstance("FAILED");
	public static ProcessingStatus COMPLETEDWITHWARNING=ProcessingStatus.getInstance("COMPLETEDWITHWARNING");
	
	private final static String SQL_1 = "select ID,VALUE from table(CSC_DISTRIBUTION_PKG.get_dimension_items_fn('FEED PROCESSING STATUS'))";
	
	private static HashMap<String, ProcessingStatus> StatusList = null;
	
	protected int ID;
	protected String STATUS_NAME;
	
	private ProcessingStatus(int id, String statusname)
	{
		this.ID=id;
		this.STATUS_NAME=statusname;		
	}
	
	public int getID() {
		return this.ID;
	}
	
	public String getStatusName() {
		return this.STATUS_NAME;
	}
	
	private static void Loaddata() {
		
		StatusList=new HashMap<String, ProcessingStatus>();
		
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		PreparedStatement objPreStatement = null;
		ResultSet objResult = null;

		try {
			////////////////////////////////////////////////////////////////////////
			//get attributes
			objPreStatement = DBConn.prepareStatement(SQL_1);
			objResult = objPreStatement.executeQuery();

			while(objResult.next())
			{
				int ID=objResult.getInt(1);
				String Name=objResult.getString(2);
				ProcessingStatus thisSDI=new ProcessingStatus(ID,Name);
				StatusList.put(Name, thisSDI);
			}
			
			objResult.close();
			objPreStatement.close();
			
		} catch (SQLException ex) {
			throw new SystemException("Database error",ex);
		} finally
		{
			try {
				DBConn.close();
			} catch (SQLException ex) {
				// TODO Auto-generated catch block
				throw new SystemException("Database error",ex);
			}		
		}
	}
	
	public static ProcessingStatus getInstance(String STATUSNAME) {
		synchronized (SQL_1) {
			
			if (StatusList==null)
			{
				Loaddata();
			}
						
			ProcessingStatus temp =  StatusList.get(STATUSNAME);

			return temp;
		}
	}

	public static void ClearCache() {
		synchronized (SQL_1) {
			StatusList=null;
		}
	}	
	
	////////////////////////////////////////////////////////////////////////
	//overwrite equals() method
	////////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof ProcessingStatus) {
			if (((ProcessingStatus) x).getID() == this.ID) {
				return true;
			}
		}
		return false;
	}

	////////////////////////////////////////////////////////////////////////
	//overwrite hashCode() method
	////////////////////////////////////////////////////////////////////////
	public int hashCode() {
		return this.ID;
	}	
}