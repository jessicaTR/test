package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.*;

public class PlantPgeOperator extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String system_operator_type;
	private RELATION_OBJECT_ID_TYPE system_operator_id;
	private String system_operator_name;
	
	public PlantPgeOperator(long Perm_ID, String system_operator_type, RELATION_OBJECT_ID_TYPE system_operator_id, String system_operator_name) {
		super(Perm_ID);
		this.system_operator_type = system_operator_type;
		this.system_operator_id = system_operator_id;
		this.system_operator_name = system_operator_name;
	}

	public static PlantPgeOperator LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID= RS.getLong("perm_id");
		String system_operator_type=RS.getString("system_operator_type");
		RELATION_OBJECT_ID_TYPE system_operator_id=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)RS.getObject("system_operator_id")).getAttributes());
		if(RS.wasNull())
		{
			system_operator_id=null;
		}
		
		String system_operator_name=RS.getString("system_operator_name");
		
		return new PlantPgeOperator( Perm_ID, system_operator_type, system_operator_id, system_operator_name);
		
		
	}
	public RELATION_OBJECT_ID_TYPE getSystem_operator_id() {
		return system_operator_id;
	}

	public String getSystem_operator_name() {
		return system_operator_name;
	}

	public String getSystem_operator_type() {
		return system_operator_type;
	}
	
	

}
