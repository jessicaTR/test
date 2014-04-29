package com.thomsonreuters.ce.dbor.pasdi.cursor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericSDICursorRow;

public class GenericCursorType extends CursorType {
	
	private static HashMap<String, GenericCursorType> GCTList= new HashMap<String, GenericCursorType>();
	private HashMap<String,GenericCursorColumn> ColumnNameList=null;
	
	private GenericCursorType(String Name)
	{
		super(Name);
	}
	
	public static GenericCursorType GetGenericCursorType(String Name)
	{
		
		synchronized(GCTList)
		{
			GenericCursorType GCT=GCTList.get(Name);
			if (GCT==null)
			{
				GCT=new GenericCursorType(Name);
				GCTList.put(Name, GCT);
			}
			
			return GCT;
			
		}
	}
	
	
	public GenericCursorColumn GetColumn(String ColumnName)
	{
		return ColumnNameList.get(ColumnName);
		
	}
	
	
	protected SDICursorRow GetDataFromRS(ResultSet RS) throws SQLException {
		
		if (ColumnNameList==null)
		{
			//populate ColumnNameList
			ColumnNameList=new HashMap<String,GenericCursorColumn>();
			
			ResultSetMetaData rsmd = RS.getMetaData(); 
			int count = rsmd.getColumnCount();
			
			for (int i = 1; i <= count; i++) { 
				String columnname=rsmd.getColumnName(i);
				ColumnNameList.put(columnname, new GenericCursorColumn(columnname));
			}
		}
				
		HashMap<GenericCursorColumn, Object> RowValues=new HashMap<GenericCursorColumn, Object>();
	
		Iterator<Entry<String,GenericCursorColumn>> iter = ColumnNameList.entrySet().iterator();	
		
		while (iter.hasNext()) 
		{
			Entry<String,GenericCursorColumn> entry = iter.next();
			String ThisColumnName = entry.getKey();
			GenericCursorColumn GCC = entry.getValue();
			RowValues.put(GCC, RS.getObject(ThisColumnName));
		}
		
		return new GenericSDICursorRow(RowValues, this);
		
		
	}	
}
