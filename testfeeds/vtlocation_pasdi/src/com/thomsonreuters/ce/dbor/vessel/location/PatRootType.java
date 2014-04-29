package com.thomsonreuters.ce.dbor.vessel.location;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.database.EasyConnection;

public class PatRootType {
	private final static String SQL_1 = "select "
		+"pat.id pat_id, "
		+"pat_map.root_type root_type "
		+"from "
		+"physical_asset_type pat, "
		+"(select " 
		+"pat.id leaf_id, "
		+"pat.name leaf_type, "
		+"connect_by_root pat.name root_type "
		+"from "
		+"physical_asset_type pat, "
		+"physical_asset_type_rship par "
		+"where pat.id = par.pat_child_id "
		+"connect by prior par.pat_child_id = par.pat_parent_id "
		+"start with par.pat_parent_id = (select id from physical_asset_type where name = 'Vessels')) "
		+"pat_map "
		+"where "
		+"pat.id = pat_map.leaf_id";
	
	private static HashMap<Integer, PatRootType> PatRootTypeList = null;
	
	protected int ID;
	protected String RootType;
		
	private PatRootType(int id, String rt)
	{
		this.ID=id;
		this.RootType=rt;	
	}
	
	public int getID() {
		return this.ID;
	}
	
	public String getRootType() {
		return this.RootType;
	}
	
	private static void Loaddata() {
		
		PatRootTypeList = new HashMap<Integer, PatRootType>();

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
				PatRootType thisSDI=new PatRootType(ID,Name);
				PatRootTypeList.put(ID, thisSDI);
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
	
	public static PatRootType getInstance(int PatId) {
		synchronized (SQL_1) {
			
			if (PatRootTypeList==null)
			{
				Loaddata();
			}
			else if (!PatRootTypeList.containsKey(PatId))
			{
				Loaddata();
			}
						
			PatRootType temp =  PatRootTypeList.get(PatId);

			return temp;
		}
	}

	public static void ClearCache() {
		synchronized (SQL_1) {
			PatRootTypeList=null;
		}
	}	
	
	////////////////////////////////////////////////////////////////////////
	//overwrite equals() method
	////////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof PatRootType) {
			if (((PatRootType) x).getID() == this.ID) {
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
