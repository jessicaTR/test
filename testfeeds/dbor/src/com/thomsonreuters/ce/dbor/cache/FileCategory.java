package com.thomsonreuters.ce.dbor.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.HashMap;


import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exception.SystemException;

public class FileCategory {

	private final static String SQL_1 = "select ID,VALUE from table(csc_distribution_pkg.get_dimension_items_fn('FEED PROCESSING FILE CATEGORY'))";
	
	private static HashMap<String, FileCategory> CategoryList = null;
	
	protected int ID;
	protected String CATEGORY_NAME;
		
	private FileCategory(int id, String categoryname)
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
		
		CategoryList = new HashMap<String, FileCategory>();

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
				FileCategory thisSDI=new FileCategory(ID,Name);
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
	
	public static FileCategory getInstance(String CATEGORYNAME) {
		synchronized (SQL_1) {
			
			if (CategoryList==null)
			{
				Loaddata();
			}
			else if (!CategoryList.containsKey(CATEGORYNAME))
			{
				Loaddata();
			}
						
			FileCategory temp =  CategoryList.get(CATEGORYNAME);

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
		if (x instanceof FileCategory) {
			if (((FileCategory) x).getID() == this.ID) {
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
