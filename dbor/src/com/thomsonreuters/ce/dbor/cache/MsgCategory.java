package com.thomsonreuters.ce.dbor.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.HashMap;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exception.SystemException;


public class MsgCategory {
	
	public static MsgCategory INFO=MsgCategory.getInstance("INFO");
	public static MsgCategory WARN=MsgCategory.getInstance("WARNING");
	public static MsgCategory ERROR=MsgCategory.getInstance("ERROR");
	
	
	private final static String SQL_1 = "select ID,VALUE from table(CSC_DISTRIBUTION_PKG.get_dimension_items_fn('FEED LOG CATEGORY'))";
	
	private static HashMap<String, MsgCategory> CategoryList = null;
	
	protected int ID;
	protected String CATEGORY_NAME;
	
	private MsgCategory(int id, String categoryname)
	{
		this.ID=id;
		this.CATEGORY_NAME=categoryname;		
	}
	
	public int getID() {
		return this.ID;
	}
	
	public String getCategoryName() {
		return this.CATEGORY_NAME;
	}
	
	private static void Loaddata() {
		
		CategoryList=new HashMap<String, MsgCategory>();
		
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
				MsgCategory thisSDI=new MsgCategory(ID,Name);
				CategoryList.put(Name, thisSDI);
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
	
	public static MsgCategory getInstance(String SDINAME) {
		synchronized (SQL_1) {
			
			if (CategoryList==null)
			{
				Loaddata();
			}
						
			MsgCategory temp =  CategoryList.get(SDINAME);

			return temp;
		}
	}

	public static void ClearCache() {
		synchronized (SQL_1) {
			CategoryList=null;
		}
	}	
	
	////////////////////////////////////////////////////////////////////////
	//overwrite equals() method
	////////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof MsgCategory) {
			if (((MsgCategory) x).getID() == this.ID) {
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
