package com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject;

import java.io.Serializable;

public class PERMANENT_ID_IDENTIFIER_TYPE implements Serializable {
	
	 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long object_id;
	 private Long object_type_id;
	 private String object_type;

	public PERMANENT_ID_IDENTIFIER_TYPE(Object[] attrs)
	{
		this.object_id=getLong(attrs[0]);
		this.object_type_id=getLong(attrs[1]);
		this.object_type=getString(attrs[2]); 
	}
	
	private Long getLong(Object value)
	{
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).longValue();
		}
			
		return null;
	}
	
	public String getString(Object value)
	{
		if (value!=null)
		{
			return (String)value;
		}
			
		return null;
	}

	public Long getObject_id() {
		return object_id;
	}

	public String getObject_type() {
		return object_type;
	}

	public Long getObject_type_id() {
		return object_type_id;
	}	
}
