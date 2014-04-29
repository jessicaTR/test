package com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject;

import java.io.Serializable;
import java.sql.Timestamp;

public class RELATION_OBJECT_ID_TYPE implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long object_id;
	private Long relation_object_type_id;
	private String relation_object_type;
	private Long relationship_id;
	private Long relationship_type_id;
	private Long related_object_id;
	private Long related_object_type_id;
	private String related_object_type;
	private Long relation_role;
	private String relationship_type;
	private Float relationship_confidence;
	private String relation_object_na_code;
	private Long related_object_order;
	private Long relation_object_order;
	private Timestamp effective_from;
	private Timestamp effective_to;
	
	
	public RELATION_OBJECT_ID_TYPE(Object[] attrs)
	{
		this.object_id=getLong(attrs[0]);
		this.relation_object_type_id=getLong(attrs[1]);
		this.relation_object_type=getString(attrs[2]);
		this.relationship_id=getLong(attrs[3]);
		this.relationship_type_id=getLong(attrs[4]);
		this.related_object_id=getLong(attrs[5]);
		this.related_object_type_id=getLong(attrs[6]);
		this.related_object_type=getString(attrs[7]);
		this.relation_role=getLong(attrs[8]);
		this.relationship_type=getString(attrs[9]);
		this.relationship_confidence=getFloat(attrs[10]);
		this.relation_object_na_code=getString(attrs[11]);
		this.related_object_order=getLong(attrs[12]);
		this.relation_object_order=getLong(attrs[13]);
		this.effective_from=getTimestamp(attrs[14]);
		this.effective_to=getTimestamp(attrs[15]);
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
	public Long getObject_id() {
		return object_id;
	}
	public Long getRelated_object_id() {
		return related_object_id;
	}
	public Long getRelated_object_order() {
		return related_object_order;
	}
	public String getRelated_object_type() {
		return related_object_type;
	}
	public Long getRelated_object_type_id() {
		return related_object_type_id;
	}
	public String getRelation_object_na_code() {
		return relation_object_na_code;
	}
	public Long getRelation_object_order() {
		return relation_object_order;
	}
	public String getRelation_object_type() {
		return relation_object_type;
	}
	public Long getRelation_object_type_id() {
		return relation_object_type_id;
	}
	public Long getRelation_role() {
		return relation_role;
	}
	public Float getRelationship_confidence() {
		return relationship_confidence;
	}
	public Long getRelationship_id() {
		return relationship_id;
	}
	public String getRelationship_type() {
		return relationship_type;
	}
	public Long getRelationship_type_id() {
		return relationship_type_id;
	}
	
	
}
