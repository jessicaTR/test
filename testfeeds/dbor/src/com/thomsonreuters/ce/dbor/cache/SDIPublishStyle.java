package com.thomsonreuters.ce.dbor.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class SDIPublishStyle {
	
	public static SDIPublishStyle INCREMENTAL=SDIPublishStyle.getInstance("INCREMENTAL");
	public static SDIPublishStyle FULL=SDIPublishStyle.getInstance("FULL");
	public static SDIPublishStyle CUMULATIVE=SDIPublishStyle.getInstance("CUMULATIVE");
	
	private final static String SQL_1 = "select ID,VALUE from table(csc_distribution_pkg.get_dimension_items_fn('SDI FILE PUBLISH STYLE'))";
	
	private static HashMap<String, SDIPublishStyle> StyleList = null;
	
	protected int ID;
	protected String PUBLISH_STYLE;
	
	private SDIPublishStyle(int id, String stylename)
	{
		this.ID=id;
		this.PUBLISH_STYLE=stylename;		
	}
	
	public int getID() {
		return this.ID;
	}
	
	public String getStyleName() {
		return this.PUBLISH_STYLE;
	}
	
	private static void Loaddata() {
		
		StyleList=new HashMap<String, SDIPublishStyle>();
		
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
				SDIPublishStyle thisSDI=new SDIPublishStyle(ID,Name);
				StyleList.put(Name, thisSDI);
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
	
	public static SDIPublishStyle getInstance(String STYLENAME) {
		synchronized (SQL_1) {
			
			if (StyleList==null)
			{
				Loaddata();
			}
						
			SDIPublishStyle temp =  StyleList.get(STYLENAME);

			return temp;
		}
	}

	public static void ClearCache() {
		synchronized (SQL_1) {
			StyleList=null;
		}
	}	
	
	////////////////////////////////////////////////////////////////////////
	//overwrite equals() method
	////////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof SDIPublishStyle) {
			if (((SDIPublishStyle) x).getID() == this.ID) {
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
