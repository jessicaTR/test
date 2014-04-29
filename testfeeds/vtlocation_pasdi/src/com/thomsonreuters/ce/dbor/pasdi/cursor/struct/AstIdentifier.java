package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AstIdentifier extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String identifier_name;
	private String identifier_id;
	
	public AstIdentifier(long Perm_ID, String identifier_name, String identifier_id) {
		super(Perm_ID);
		this.identifier_name = identifier_name;
		this.identifier_id = identifier_id;
	}

	public String getIdentifier_id() {
		return identifier_id;
	}

	public String getIdentifier_name() {
		return identifier_name;
	}
	
	public static AstIdentifier LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long perm_id=RS.getLong("PERM_ID");
		
		String identifier_name=RS.getString("identifier_name");
		
		String identifier_id=RS.getString("identifier_id");
		
		return new AstIdentifier(perm_id,identifier_name,identifier_id);
		
	}
	

}
