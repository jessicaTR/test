package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

public class Geographic_Hierarchy {
	
	private int hierarchy_classification_id;
	private String gun_id_hierarchy;
	private String zoom_level_hierarchy;
	
	public Geographic_Hierarchy(int hierarchy_classification_id, String gun_id_hierarchy, String zoom_level_hierarchy) {
		super();
		this.hierarchy_classification_id = hierarchy_classification_id;
		this.gun_id_hierarchy = gun_id_hierarchy;
		this.zoom_level_hierarchy = zoom_level_hierarchy;
	}

	public String getGun_id_hierarchy() {
		return gun_id_hierarchy;
	}

	public int getHierarchy_classification_id() {
		return hierarchy_classification_id;
	}

	public String getZoom_level_hierarchy() {
		return zoom_level_hierarchy;
	}

}
