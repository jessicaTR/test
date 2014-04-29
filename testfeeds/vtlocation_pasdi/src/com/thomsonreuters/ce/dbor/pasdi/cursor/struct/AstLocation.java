package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AstLocation extends SDICursorRow {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long gun_id;
	private Integer hierarchy_classification_id;

	public AstLocation(long Perm_ID, Long gun_id,Integer hierarchy_classification_id) {
		super(Perm_ID);
		this.gun_id = gun_id;
		this.hierarchy_classification_id=hierarchy_classification_id;
	}

	public static AstLocation LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID=RS.getLong("PERM_ID");
		
		
		Long gun_id=RS.getLong("gun_id");		
		if(RS.wasNull())
		{
			gun_id=null;
		}
		
		Integer hierarchy_classification_id=RS.getInt("hierarchy_classification_id");		
		if(RS.wasNull())
		{
			hierarchy_classification_id=null;
		}
		
		return new AstLocation(Perm_ID, gun_id,hierarchy_classification_id);
	}

	public Long getGun_id() {
		return gun_id;
	}

	public Integer getHierarchy_classification_id() {
		return hierarchy_classification_id;
	}







	
	
}
