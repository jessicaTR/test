package com.thomsonreuters.ce.dbor.vessel.location;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.database.EasyConnection;

public class VesselDataSource {
	private final static String SQL_1 = "select ID,VALUE from table(csc_distribution_pkg.get_dimension_items_fn('VESSEL LOCATION DATA SOURCE'))";
	private static HashMap<String, VesselDataSource> DataSourceList = null;
	
	protected int ID;
	protected String Data_Source_Name;
		
	private VesselDataSource(int id, String dsn)
	{
		this.ID=id;
		this.Data_Source_Name=dsn;	
	}
	
	public int getID() {
		return this.ID;
	}
	
	public String getDataSourceName() {
		return this.Data_Source_Name;
	}
	
	private static void Loaddata() {
		
		DataSourceList = new HashMap<String, VesselDataSource>();

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
				VesselDataSource thisSDI=new VesselDataSource(ID,Name);
				DataSourceList.put(Name, thisSDI);
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
	
	public static VesselDataSource getInstance(String SourceName) {
		synchronized (SQL_1) {
			
			if (DataSourceList==null)
			{
				Loaddata();
			}
			else if (!DataSourceList.containsKey(SourceName))
			{
				Loaddata();
			}
						
			VesselDataSource temp =  DataSourceList.get(SourceName);

			return temp;
		}
	}

	public static void ClearCache() {
		synchronized (SQL_1) {
			DataSourceList=null;
		}
	}	
	
	////////////////////////////////////////////////////////////////////////
	//overwrite equals() method
	////////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof VesselDataSource) {
			if (((VesselDataSource) x).getID() == this.ID) {
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
