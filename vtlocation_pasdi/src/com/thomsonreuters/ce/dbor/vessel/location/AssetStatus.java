package com.thomsonreuters.ce.dbor.vessel.location;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;



import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class AssetStatus {
	private final static String SQL_1 = "select ID,VALUE from table(csc_distribution_pkg.get_dimension_items_fn('ASSET STATUS VALUE'))";

	private static HashMap<String, AssetStatus> StatusList = null;
	protected int ID;
	protected String Status_Name;
		
	private AssetStatus(int id, String sn)
	{
		this.ID=id;
		this.Status_Name=sn;	
	}
	
	public int getID() {
		return this.ID;
	}
	
	public String getStatusName() {
		return this.Status_Name;
	}
	
	private static void Loaddata() {
		
		StatusList = new HashMap<String, AssetStatus>();

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
				AssetStatus thisSDI=new AssetStatus(ID,Name);
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
	
	public static AssetStatus getInstance(String Status_Name) {
		synchronized (SQL_1) {
			
			if (StatusList==null)
			{
				Loaddata();
			}
			else if (!StatusList.containsKey(Status_Name))
			{
				Loaddata();
			}
						
			AssetStatus temp =  StatusList.get(Status_Name);

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
		if (x instanceof AssetStatus) {
			if (((AssetStatus) x).getID() == this.ID) {
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
