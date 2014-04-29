package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.util.HashMap;

public class Geographic_Unit {

	public HashMap<Integer, Geographic_Hierarchy> Geographic_Hierarchy_Map = new HashMap<Integer, Geographic_Hierarchy>();

	public HashMap<Integer, GeographicUnit_EditorialRCS[]> Geographic_EditorialRCS_Map = new HashMap<Integer, GeographicUnit_EditorialRCS[]>();
	
	private Long gun_id;

	private Long gun_perm_id;
		
	private String gun_desc;
	
	private String guc_desc;
	
	private Long relation_object_type_id;
	
	private String relation_object_type;
	
	private Long relationship_type_id;
	
	private String relationship_type;

	private String rcs_code;

	private Float latitude_north;

	private Float latitude_south;

	private Float longitude_east;

	private Float longitude_west;


	public Geographic_Unit(Long gun_id, Long gun_perm_id, String gun_desc,String guc_desc, Long relation_object_type_id, String relation_object_type, Long relationship_type_id, String relationship_type, String rcs_code,
			Float latitude_north, Float latitude_south, Float longitude_east,
			Float longitude_west) {
		super();
		this.gun_id = gun_id;
		this.gun_perm_id=gun_perm_id;
		this.gun_desc = gun_desc;
		this.guc_desc = guc_desc;
		this.rcs_code = rcs_code;
		this.relation_object_type_id=relation_object_type_id;
		this.relation_object_type=relation_object_type;
		this.relationship_type_id=relationship_type_id;
		this.relationship_type = relationship_type;
		this.latitude_north = latitude_north;
		this.latitude_south = latitude_south;
		this.longitude_east = longitude_east;
		this.longitude_west = longitude_west;
	}

	public String getGun_desc() {
		return gun_desc;
	}

	public String getGuc_desc() {
		return this.guc_desc;
	}
	
	public Long getGun_id() {
		return gun_id;
	}
	
	public Long getGun_perm_id() {
		return gun_perm_id;
	}	

	public Float getLatitude_north() {
		return latitude_north;
	}

	public Float getLatitude_south() {
		return latitude_south;
	}

	public Float getLongitude_east() {
		return longitude_east;
	}

	public Float getLongitude_west() {
		return longitude_west;
	}

	public String getRcs_code() {
		return rcs_code;
	}

	public Geographic_Hierarchy getGeographic_Hierarchy(
			int hierarchy_classification_id) {
		return this.Geographic_Hierarchy_Map.get(hierarchy_classification_id);
	}
	
	public GeographicUnit_EditorialRCS[] getGunEditorialRCS(
			int hierarchy_classification_id) {
		return this.Geographic_EditorialRCS_Map.get(hierarchy_classification_id);
	}

	public boolean equals(Object x) {
		if (x instanceof Geographic_Unit) {
			if (((Geographic_Unit) x).getGun_id() == this.getGun_id()) {
				return true;
			}
		}
		return false;
	}

//	public Long hashCode() {
//		return this.getGun_id();
//	}


	public HashMap<Integer, GeographicUnit_EditorialRCS[]> getGeographic_EditorialRCS_Map() {
		return Geographic_EditorialRCS_Map;
	}

	public HashMap<Integer, Geographic_Hierarchy> getGeographic_Hierarchy_Map() {
		return Geographic_Hierarchy_Map;
	}

	public String getRelation_object_type() {
		return relation_object_type;
	}

	public Long getRelation_object_type_id() {
		return relation_object_type_id;
	}

	public Long getRelationship_type_id() {
		return relationship_type_id;
	}
	
	public String getRelationship_type() {
		return relationship_type;
	}

}
