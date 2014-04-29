package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.*;

public class AstTypeCommodity extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private RELATION_OBJECT_ID_TYPE commodity_type_id;
	private String commodity_type_value;
	private String commodity_typ_rcs;
	private String commodity_type_io;
	private Integer commodity_type_rank;
	

	public AstTypeCommodity(long Perm_ID, RELATION_OBJECT_ID_TYPE commodity_type_id, String commodity_type_value, String commodity_typ_rcs,String commodity_type_io,Integer commodity_type_rank) {
		super(Perm_ID);

		this.commodity_type_id = commodity_type_id;
		this.commodity_type_value = commodity_type_value;
		this.commodity_typ_rcs = commodity_typ_rcs;
		this.commodity_type_io=commodity_type_io;
		this.commodity_type_rank=commodity_type_rank;
	}


	public static AstTypeCommodity LoadDataFromRS(ResultSet RS) throws SQLException
	{
		
		long Perm_ID=RS.getLong("PERM_ID");
		
		RELATION_OBJECT_ID_TYPE commodity_type_id=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)RS.getObject("commodity_type_id")).getAttributes());
		if (RS.wasNull())
		{
			commodity_type_id=null;
		}
		
		String commodity_type_value=RS.getString("commodity_type_value");
		String commodity_typ_rcs=RS.getString("commodity_type_rcs");
		String commodity_type_io=RS.getString("commodity_type_io");
		
		Integer commodity_type_rank=RS.getInt("commodity_type_rank");
		if (RS.wasNull())
		{
			commodity_type_rank=null;
		}
		
		return new AstTypeCommodity(Perm_ID,commodity_type_id, commodity_type_value, commodity_typ_rcs, commodity_type_io, commodity_type_rank) ;
	}


	public String getCommodity_typ_rcs() {
		return commodity_typ_rcs;
	}




	public RELATION_OBJECT_ID_TYPE getCommodity_type_id() {
		return commodity_type_id;
	}




	public String getCommodity_type_io() {
		return commodity_type_io;
	}




	public Integer getCommodity_type_rank() {
		return commodity_type_rank;
	}




	public String getCommodity_type_value() {
		return commodity_type_value;
	}



	
	
}
