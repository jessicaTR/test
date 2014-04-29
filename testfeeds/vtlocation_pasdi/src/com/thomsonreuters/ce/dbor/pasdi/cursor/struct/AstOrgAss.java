package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.*;

/**
 * Organization
 * @author lei.yang
 *
 */
public class AstOrgAss extends SDICursorRow {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String aoa_type;
	private RELATION_OBJECT_ID_TYPE org_perm_id;
	private String organisation_name;
	private Float percentage;
	private Integer aoa_rank;	
	
	
	public AstOrgAss(long Perm_ID, String aoa_type, RELATION_OBJECT_ID_TYPE org_perm_id, String organisation_name, Float percentage,Integer aoa_rank) {
		super(Perm_ID);
		this.aoa_type = aoa_type;
		this.org_perm_id = org_perm_id;
		this.organisation_name = organisation_name;
		this.percentage = percentage;
		this.aoa_rank=aoa_rank;
	}



	public static AstOrgAss LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID=RS.getLong("PERM_ID");
		String aoa_type=RS.getString("aoa_type");
		RELATION_OBJECT_ID_TYPE org_perm_id=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)RS.getObject("org_perm_id")).getAttributes());
		if(RS.wasNull())
		{
			org_perm_id=null;
		}
		
		String organisation_name=RS.getString("organisation_name");
		Float percentage=RS.getFloat("percentage");
		if(RS.wasNull())
		{
			percentage=null;
		}
		
		Integer aoa_rank=RS.getInt("aoa_rank");
		if(RS.wasNull())
		{
			aoa_rank=null;
		}
						
		
		return new AstOrgAss(Perm_ID, aoa_type, org_perm_id, organisation_name, percentage,aoa_rank);
		
	}



	public String getAoa_type() {
		return aoa_type;
	}



	public RELATION_OBJECT_ID_TYPE getOrg_perm_id() {
		return org_perm_id;
	}



	public String getOrganisation_name() {
		return organisation_name;
	}



	public Float getPercentage() {
		return percentage;
	}



	public Integer getAoa_rank() {
		return aoa_rank;
	}
	
	
	
	
}
