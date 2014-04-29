package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

public class UniverseVesZoneDetail {
	
	public String zone_type;
	public Long zone_id;
	public Long zone_perm_id;
	public String zone_name;
	public String zone_code;
	public Integer zone_rank;
	public Long zone_axsmarine_id;
	public Long relation_object_type_id;
	public String relation_object_type;
	public Long relationship_type_id;
	public String relationship_type;	
	public Float latitude_north;
	public Float latitude_south;
	public Float longitude_east;
	public Float longitude_west;
	
	public UniverseVesZoneDetail(String zone_type, Long zone_id, Long zone_perm_id, String zone_name, String zone_code, Integer zone_rank, Long zone_axsmarine_id, Long relation_object_type_id, String relation_object_type, Long relationship_type_id, String relationship_type, Float latitude_north, Float latitude_south, Float longitude_east, Float longitude_west) {
		super();
		this.zone_type = zone_type;
		this.zone_id = zone_id;
		this.zone_perm_id = zone_perm_id;
		this.zone_name = zone_name;
		this.zone_code = zone_code;
		this.zone_rank = zone_rank;
		this.zone_axsmarine_id = zone_axsmarine_id;
		this.relation_object_type_id = relation_object_type_id;
		this.relation_object_type = relation_object_type;
		this.relationship_type_id = relationship_type_id;
		this.relationship_type = relationship_type;
		this.latitude_north = latitude_north;
		this.latitude_south = latitude_south;
		this.longitude_east = longitude_east;
		this.longitude_west = longitude_west;
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

	public String getRelation_object_type() {
		return relation_object_type;
	}

	public Long getRelation_object_type_id() {
		return relation_object_type_id;
	}

	public String getRelationship_type() {
		return relationship_type;
	}

	public Long getRelationship_type_id() {
		return relationship_type_id;
	}

	public Long getZone_axsmarine_id() {
		return zone_axsmarine_id;
	}

	public String getZone_code() {
		return zone_code;
	}

	public Long getZone_id() {
		return zone_id;
	}

	public String getZone_name() {
		return zone_name;
	}

	public Long getZone_perm_id() {
		return zone_perm_id;
	}

	public Integer getZone_rank() {
		return zone_rank;
	}

	public String getZone_type() {
		return zone_type;
	}
	
	

}
