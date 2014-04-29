package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

public class GenericCfgOutput {

	public String pas_type;
	public String is_group;
	public Long item_id;
	
	public GenericCfgOutput(String pas_type, String is_group, Long item_id) {
		this.pas_type = pas_type;
		this.is_group = is_group;
		this.item_id = item_id;
	}

	public String getIs_group() {
		return is_group;
	}

	public Long getItem_id() {
		return item_id;
	}

	public String getPas_type() {
		return pas_type;
	}
	
	

}
