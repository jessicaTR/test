package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.PERMANENT_ID_IDENTIFIER_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.RELATION_OBJECT_ID_TYPE;

public class AstBase extends SDICursorRow{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String asset_category;
	private PERMANENT_ID_IDENTIFIER_TYPE cne_asset_id;
	private RELATION_OBJECT_ID_TYPE asset_type_id;
	private String asset_type_value;
	private String asset_type_rcs;
	private String pas_type;
	private String asset_ric;
	private Date entity_created_date;
	private Date entity_modified_date;
	private String permissionable_object;
	private String admin_status;
	private Integer asset_rank;
	private Date effective_from;
	
	


	public AstBase(long Perm_ID,String asset_category,PERMANENT_ID_IDENTIFIER_TYPE cne_asset_id,RELATION_OBJECT_ID_TYPE asset_type_id, String asset_type_value, String asset_type_rcs, String pas_type, String asset_ric, Date entity_created_date, Date entity_modified_date, String permissionable_object, String admin_status,Integer asset_rank, Date effective_from) {
		super(Perm_ID);
		this.asset_category = asset_category;
		this.cne_asset_id=cne_asset_id;
		this.asset_type_id = asset_type_id;
		this.asset_type_value = asset_type_value;
		this.asset_type_rcs = asset_type_rcs;
		this.pas_type = pas_type;
		this.asset_ric = asset_ric;
		this.entity_created_date = entity_created_date;
		this.entity_modified_date = entity_modified_date;
		this.permissionable_object = permissionable_object;
		this.admin_status = admin_status;
		this.asset_rank=asset_rank;
		this.effective_from = effective_from;
	}



	public static AstBase LoadDataFromRS(ResultSet RS) throws SQLException
	{

		long perm_id=RS.getLong("PERM_ID");
		
		String asset_category=RS.getString("asset_category");
		
		PERMANENT_ID_IDENTIFIER_TYPE cne_asset_id= new PERMANENT_ID_IDENTIFIER_TYPE(((oracle.sql.STRUCT)RS.getObject("cne_asset_id")).getAttributes());
		if (RS.wasNull())
		{
			cne_asset_id=null;
		}
		
		RELATION_OBJECT_ID_TYPE asset_type_id=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)RS.getObject("asset_type_id")).getAttributes());
		
		if (RS.wasNull())
		{
			asset_type_id=null;
		}
				
		String asset_type_value=RS.getString("asset_type_value");
		String asset_type_rcs=RS.getString("asset_type_rcs");
		
		
		String pas_type = RS.getString("pas_type");
		String asset_ric=RS.getString("asset_ric");
		
		Date entity_created_date=null;
		Timestamp Ts=RS.getTimestamp("entity_created_date");
		if (Ts!=null)
		{
			entity_created_date = new Date(Ts.getTime());
		}
		
		Date entity_modified_date=null;
		Ts=RS.getTimestamp("entity_modified_date");
		if (Ts!=null)
		{
			entity_modified_date = new Date(Ts.getTime());
		}
		String permissionable_object=RS.getString("permissionable_object");
		String admin_status=RS.getString("admin_status");
		
		Integer asset_rank=RS.getInt("asset_rank");		
		if (RS.wasNull())
		{
			asset_rank=null;
		}
		
		
		Date effective_from=null;
		Ts=RS.getTimestamp("effective_from");
		if (Ts!=null)
		{
			effective_from = new Date(Ts.getTime());
		}
		
		return new AstBase(perm_id,asset_category,cne_asset_id, asset_type_id, asset_type_value, asset_type_rcs, pas_type, asset_ric, entity_created_date, entity_modified_date, permissionable_object, admin_status,asset_rank, effective_from);
		
	}



	public String getAdmin_status() {
		return admin_status;
	}

	public String getAsset_ric() {
		return asset_ric;
	}



	public Date getEffective_from() {
		return effective_from;
	}



	public Date getEntity_created_date() {
		return entity_created_date;
	}



	public Date getEntity_modified_date() {
		return entity_modified_date;
	}



	public String getPas_type() {
		return pas_type;
	}



	public String getPermissionable_object() {
		return permissionable_object;
	}
	
	
	public String getAsset_category() {
		return asset_category;
	}




	public RELATION_OBJECT_ID_TYPE getAsset_type_id() {
		return asset_type_id;
	}




	public String getAsset_type_rcs() {
		return asset_type_rcs;
	}




	public String getAsset_type_value() {
		return asset_type_value;
	}



	public Integer getAsset_rank() {
		return asset_rank;
	}



	public PERMANENT_ID_IDENTIFIER_TYPE getCne_asset_id() {
		return cne_asset_id;
	}



	
	
}
