package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.util.Date;

public class GenericMetaDataGroupDetail {
	public Long list_id;
	public String table_name;
	public GenericMetaDataEleDetail GMED; 
	public String GAGroupID;
	public String GAGroupType;
	public String GAMetadataSearchable;
	public String GAMetadataID;
	public String GAMetadataEnumerationValue;
	public Date entity_created_date;
	public Date entity_modified_date;
	public String is_active;	
	
	public GenericMetaDataGroupDetail(Long list_id,String table_name, GenericMetaDataEleDetail gmed, String groupID, String groupType,String GAMetadataSearchable, String metadataID, String metadataEnumerationValue, Date entity_created_date, Date entity_modified_date,String is_active) {
		this.list_id = list_id;
		this.table_name=table_name;
		this.GMED = gmed;
		this.GAGroupID = groupID;
		this.GAGroupType = groupType;
		this.GAMetadataSearchable=GAMetadataSearchable;
		this.GAMetadataID = metadataID;
		this.GAMetadataEnumerationValue = metadataEnumerationValue;
		this.entity_created_date=entity_created_date;
		this.entity_modified_date=entity_modified_date;
		this.is_active=is_active;
		
	}

	public String getGAGroupID() {
		return GAGroupID;
	}

	public String getGAGroupType() {
		return GAGroupType;
	}

	public String getGAMetadataEnumerationValue() {
		return GAMetadataEnumerationValue;
	}

	public String getGAMetadataID() {
		return GAMetadataID;
	}

	public GenericMetaDataEleDetail getGMED() {
		return GMED;
	}

	public Long getList_id() {
		return list_id;
	}

	public String getTable_name() {
		return table_name;
	}

	public String getGAMetadataSearchable() {
		return GAMetadataSearchable;
	}

	public Date getEntity_created_date() {
		return entity_created_date;
	}

	public Date getEntity_modified_date() {
		return entity_modified_date;
	}

	public String getIs_active() {
		return is_active;
	}
	
	


}
