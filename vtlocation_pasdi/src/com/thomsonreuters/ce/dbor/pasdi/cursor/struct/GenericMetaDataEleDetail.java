package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.util.Date;

public class GenericMetaDataEleDetail {
	
	public Long sco_id; 
	public String GAMetadataSearchable;
	public Date GAMetadata_effective_from;
	public String GAMetadataID; 
	public String GAMetadataEnglishLongName;
	public String GAMetadataDescription;
	public String GAMetadataDataType;
	public Integer GAMetadataMinLength; 
	public Integer GAMetadataMaxLength; 
	public Integer GAMetadataLength; 
	public Integer GAMetadataTotalDigits; 
	public Integer GAMetadataFractionDigits;
	public Float GAMetadataMinInclusive; 
	public Float GAMetadataMaxExclusive;
	public String GAMetadataNullable; 
	public String GAMetadataSortable;
	public String GAMetadataAssetGroups;
	public String GAMetadataEnumerationValues;
	public String column_name;
	public String table_name;
	public Date entity_created_date;
	public Date entity_modified_date;
	public String is_active;
	
    
	public GenericMetaDataEleDetail(Long sco_id, String GAMetadataSearchable,Date metadata_effective_from, String metadataID, String metadataEnglishLongName, String metadataDescription, String metadataDataType, Integer metadataMinLength, Integer metadataMaxLength, Integer metadataLength, Integer metadataTotalDigits, Integer metadataFractionDigits, Float metadataMinInclusive, Float metadataMaxExclusive, String metadataNullable, String metadataSortable, String metadataAssetGroups,String GAMetadataEnumerationValues, String column_name, String table_name, Date entity_created_date, Date entity_modified_date,String is_active) {
		
		this.sco_id = sco_id;
		this.GAMetadataSearchable=GAMetadataSearchable;
		this.GAMetadata_effective_from = metadata_effective_from;
		this.GAMetadataID = metadataID;
		this.GAMetadataEnglishLongName = metadataEnglishLongName;
		this.GAMetadataDescription = metadataDescription;
		this.GAMetadataDataType = metadataDataType;
		this.GAMetadataMinLength = metadataMinLength;
		this.GAMetadataMaxLength = metadataMaxLength;
		this.GAMetadataLength = metadataLength;
		this.GAMetadataTotalDigits = metadataTotalDigits;
		this.GAMetadataFractionDigits = metadataFractionDigits;
		this.GAMetadataMinInclusive = metadataMinInclusive;
		this.GAMetadataMaxExclusive = metadataMaxExclusive;
		this.GAMetadataNullable = metadataNullable;
		this.GAMetadataSortable = metadataSortable;
		this.GAMetadataAssetGroups = metadataAssetGroups;
		this.GAMetadataEnumerationValues=GAMetadataEnumerationValues;
		this.column_name = column_name;
		this.table_name = table_name;
		this.entity_created_date=entity_created_date;
		this.entity_modified_date=entity_modified_date;
		this.is_active=is_active;
	}

	public String getColumn_name() {
		return column_name;
	}

	public Date getGAMetadata_effective_from() {
		return GAMetadata_effective_from;
	}

	public String getGAMetadataAssetGroups() {
		return GAMetadataAssetGroups;
	}

	public String getGAMetadataDataType() {
		return GAMetadataDataType;
	}

	public String getGAMetadataDescription() {
		return GAMetadataDescription;
	}

	public String getGAMetadataEnglishLongName() {
		return GAMetadataEnglishLongName;
	}

	public Integer getGAMetadataFractionDigits() {
		return GAMetadataFractionDigits;
	}

	public String getGAMetadataID() {
		return GAMetadataID;
	}

	public Integer getGAMetadataLength() {
		return GAMetadataLength;
	}

	public Float getGAMetadataMaxExclusive() {
		return GAMetadataMaxExclusive;
	}

	public Integer getGAMetadataMaxLength() {
		return GAMetadataMaxLength;
	}

	public Float getGAMetadataMinInclusive() {
		return GAMetadataMinInclusive;
	}

	public Integer getGAMetadataMinLength() {
		return GAMetadataMinLength;
	}

	public String getGAMetadataNullable() {
		return GAMetadataNullable;
	}

	public String getGAMetadataSortable() {
		return GAMetadataSortable;
	}

	public Integer getGAMetadataTotalDigits() {
		return GAMetadataTotalDigits;
	}

	public Long getSco_id() {
		return sco_id;
	}

	public String getTable_name() {
		return table_name;
	}

	public String getGAMetadataEnumerationValues() {
		return GAMetadataEnumerationValues;
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
