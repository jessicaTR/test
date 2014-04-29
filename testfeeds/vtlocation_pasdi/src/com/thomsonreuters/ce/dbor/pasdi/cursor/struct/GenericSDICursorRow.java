package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;


import java.util.HashMap;
import com.thomsonreuters.ce.dbor.pasdi.cursor.GenericCursorColumn;
import com.thomsonreuters.ce.dbor.pasdi.cursor.GenericCursorType;

public class GenericSDICursorRow extends SDICursorRow {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<GenericCursorColumn, Object> RowValues=new HashMap<GenericCursorColumn, Object>();
	private GenericCursorType generic_cursor_type;
	
	public GenericSDICursorRow(HashMap<GenericCursorColumn, Object> RowValues,GenericCursorType GCT) {
		
		super(((java.math.BigDecimal)RowValues.get(GCT.GetColumn("PERM_ID"))).longValue());
		this.RowValues=RowValues;
		this.generic_cursor_type=GCT;
	}
	
	public Object getObject(String colum_name)
	{
		return RowValues.get(generic_cursor_type.GetColumn(colum_name));
	}

}
