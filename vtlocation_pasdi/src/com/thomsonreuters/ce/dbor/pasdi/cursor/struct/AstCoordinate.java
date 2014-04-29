package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.RELATION_OBJECT_ID_TYPE;

public class AstCoordinate extends SDICursorRow {
		
	private static final long serialVersionUID = 1L;
	private Float longitude;
	private Float latitude;
	private Integer precision;
	private RELATION_OBJECT_ID_TYPE supplier_id;
	private String supplier;
	private String type;
	private String accuracy;
	


	public AstCoordinate(long Perm_ID, Float longitude, Float latitude, Integer precision, RELATION_OBJECT_ID_TYPE supplier_id, String supplier, String type, String accuracy) {
		super(Perm_ID);
		this.longitude = longitude;
		this.latitude = latitude;
		this.precision=precision;
		this.supplier_id=supplier_id;
		this.supplier=supplier;
		this.type=type;
		this.accuracy=accuracy;
	}

	public static AstCoordinate LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_ID=RS.getLong("PERM_ID");
		Float longitude=RS.getFloat("longitude");		
		if(RS.wasNull())
		{
			longitude=null;
		}
		
		Float latitude=RS.getFloat("latitude");
		if(RS.wasNull())
		{
			latitude=null;
		}
		
		Integer precision=RS.getInt("precision");
		if(RS.wasNull())
		{
			precision=null;
		}
		
		RELATION_OBJECT_ID_TYPE supplier_id=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)RS.getObject("supplier_id")).getAttributes());
		if(RS.wasNull())
		{
			supplier_id=null;
		}		
		
		String supplier=RS.getString("supplier");
		String type=RS.getString("type");
		String accuracy=RS.getString("accuracy");
		
		return new AstCoordinate(Perm_ID, longitude, latitude,precision,supplier_id,supplier,type,accuracy);
	}

	public Float getLatitude() {
		return latitude;
	}

	public Float getLongitude() {
		return longitude;
	}

	public String getAccuracy() {
		return accuracy;
	}

	public Integer getPrecision() {
		return precision;
	}

	public String getSupplier() {
		return supplier;
	}

	public RELATION_OBJECT_ID_TYPE getSupplier_id() {
		return supplier_id;
	}

	public String getType() {
		return type;
	}	
	

	
}
