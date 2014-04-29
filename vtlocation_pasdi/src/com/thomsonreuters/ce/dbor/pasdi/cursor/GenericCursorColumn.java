package com.thomsonreuters.ce.dbor.pasdi.cursor;

import java.io.Serializable;

public class GenericCursorColumn implements Serializable{

	private String ColumnName;

	public GenericCursorColumn(String columnName) {
		ColumnName = columnName;
	}

	public String getColumnName() {
		return ColumnName;
	}
	
	
}
