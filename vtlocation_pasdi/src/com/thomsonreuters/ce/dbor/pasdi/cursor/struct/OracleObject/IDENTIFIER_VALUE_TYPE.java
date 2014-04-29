package com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject;

import java.io.Serializable;
import java.sql.Timestamp;

public class IDENTIFIER_VALUE_TYPE implements Serializable {

	private Long identifier_entity_type_id;
	private Long identifier_entity_id;
	private Long identifier_type_id ;
	private String identifier_entity_type;
	private String identifier_type_code;
	private String identifier_value;
	private Long identifier_order;
	private String identifier_is_primary;
	private Timestamp  effective_from;
	private Timestamp  effective_to;
	private String effective_to_na_code;
	
	public IDENTIFIER_VALUE_TYPE(Object[] attrs)
	{
		this.identifier_entity_type_id=getLong(attrs[0]);
		this.identifier_entity_id=getLong(attrs[1]);
		this.identifier_type_id=getLong(attrs[2]);
		this.identifier_entity_type=getString(attrs[3]);
		this.identifier_type_code=getString(attrs[4]);
		this.identifier_value=getString(attrs[5]);
		this.identifier_order=getLong(attrs[6]);
		this.identifier_is_primary=getString(attrs[7]);
		this.effective_from=getTimestamp(attrs[8]);
		this.effective_to=getTimestamp(attrs[9]);
		this.effective_to_na_code=getString(attrs[10]);
		
	}
	
	private Long getLong(Object value)
	{
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).longValue();
		}
			
		return null;
	}
	
	
	private Float getFloat(Object value)
	{
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).floatValue();
		}
			
		return null;
	}	
	
	private Timestamp getTimestamp(Object value)
	{
		if (value!=null)
		{
			return (Timestamp)value;
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

	public Timestamp getEffective_from() {
		return effective_from;
	}

	public Timestamp getEffective_to() {
		return effective_to;
	}

	public String getEffective_to_na_code() {
		return effective_to_na_code;
	}

	public Long getIdentifier_entity_id() {
		return identifier_entity_id;
	}

	public String getIdentifier_entity_type() {
		return identifier_entity_type;
	}

	public Long getIdentifier_entity_type_id() {
		return identifier_entity_type_id;
	}

	public String getIdentifier_is_primary() {
		return identifier_is_primary;
	}

	public Long getIdentifier_order() {
		return identifier_order;
	}

	public String getIdentifier_type_code() {
		return identifier_type_code;
	}

	public Long getIdentifier_type_id() {
		return identifier_type_id;
	}

	public String getIdentifier_value() {
		return identifier_value;
	}		
}
