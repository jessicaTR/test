package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

public class EditorialRCS {
	
	private String target_rcs_code;
	private Long target_id;
	
	public EditorialRCS(String target_rcs_code, Long target_id) {
		super();
		this.target_rcs_code = target_rcs_code;
		this.target_id = target_id;
	}

	public Long getTarget_id() {
		return target_id;
	}

	public String getTarget_rcs_code() {
		return target_rcs_code;
	}
	
	
}
