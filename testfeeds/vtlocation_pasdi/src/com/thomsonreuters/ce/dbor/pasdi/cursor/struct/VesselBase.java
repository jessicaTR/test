package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.*;

public class VesselBase extends SDICursorRow {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String imo;
	private Integer mmsi;
	private Float dwt;
	private Float loa;
	private Float cubic_capacity;
	private Float beam;
	private Float draft;
	private Float teu;
	private String hull_style;
	private String bottom_style;
	private String side_style;
	private String coated;
	private String is_deactived;
	private String product;
	private Float tpc;
	private Date built;
	private Date built_initial;
	private Date built_demolition;
	private RELATION_OBJECT_ID_TYPE flag_id;
	private String flag_name;
	private String flag_iso;
	private String flag_rcs;
	

	public VesselBase(long Perm_ID, String imo, Integer mmsi, Float dwt, Float loa, Float cubic_capacity, Float beam, Float draft, Float teu, String hull_style, String bottom_style, String side_style, String coated, String is_deactived, String product, Float tpc, Date built, Date built_initial, Date built_demolition, RELATION_OBJECT_ID_TYPE flag_id, String flag_name, String flag_iso, String flag_rcs) {
		super(Perm_ID);
		this.imo = imo;
		this.mmsi = mmsi;
		this.dwt = dwt;
		this.loa = loa;
		this.cubic_capacity = cubic_capacity;
		this.beam = beam;
		this.draft = draft;
		this.teu = teu;
		this.hull_style = hull_style;
		this.bottom_style = bottom_style;
		this.side_style = side_style;
		this.coated = coated;
		this.is_deactived = is_deactived;
		this.product = product;
		this.tpc = tpc;
		this.built = built;
		this.built_initial = built_initial;
		this.built_demolition = built_demolition;
		this.flag_id = flag_id;
		this.flag_name = flag_name;
		this.flag_iso = flag_iso;
		this.flag_rcs = flag_rcs;
	}



	public static VesselBase LoadDataFromRS(ResultSet RS) throws SQLException
	{
		long Perm_id=RS.getLong("perm_id");
		
		String imo=RS.getString("imo");
		
		Integer mmsi=RS.getInt("mmsi");
		if(RS.wasNull())
		{
			mmsi=null;
		}
		
		Float dwt=RS.getFloat("dwt");
		if(RS.wasNull())
		{
			dwt=null;
		}		
		
		Float loa=RS.getFloat("loa");
		if(RS.wasNull())
		{
			loa=null;
		}		
		
		Float cubic_capacity=RS.getFloat("cubic_capacity");
		if(RS.wasNull())
		{
			cubic_capacity=null;
		}		
		
		Float beam=RS.getFloat("beam");
		if(RS.wasNull())
		{
			beam=null;
		}		
		
		Float draft=RS.getFloat("draft");
		if(RS.wasNull())
		{
			draft=null;
		}		
		
		Float teu=RS.getFloat("teu");
		if(RS.wasNull())
		{
			teu=null;
		}		
		
		String hull_style=RS.getString("hull_style");
		String bottom_style=RS.getString("bottom_style");
		String side_style=RS.getString("side_style");
		String coated=RS.getString("coated");
		String is_deactived=RS.getString("is_deactived");
		String product=RS.getString("product");
		
		Float tpc=RS.getFloat("tpc");
		if(RS.wasNull())
		{
			tpc=null;
		}	
		
		Date built=null;
		Timestamp Ts=RS.getTimestamp("built");
		if (Ts!=null)
		{
			built = new Date(Ts.getTime());
		}
		Date built_initial=null;
		Ts=RS.getTimestamp("built_initial");
		if (Ts!=null)
		{
			built_initial = new Date(Ts.getTime());
		}
		
		Date built_demolition=null;
		Ts=RS.getTimestamp("built_demolition");
		if (Ts!=null)
		{
			built_demolition = new Date(Ts.getTime());
		}
		RELATION_OBJECT_ID_TYPE flag_id=new RELATION_OBJECT_ID_TYPE(((oracle.sql.STRUCT)RS.getObject("flag_id")).getAttributes());
		if(RS.wasNull())
		{
			flag_id=null;
		}		
		
		String flag_name=RS.getString("flag_name");
		String flag_iso=RS.getString("flag_iso");
		String flag_rcs=RS.getString("flag_rcs");
		
		return new VesselBase(Perm_id,imo, mmsi, dwt, loa, cubic_capacity, beam, draft, teu, hull_style, bottom_style, side_style, coated, is_deactived,product, tpc, built, built_initial, built_demolition, flag_id, flag_name, flag_iso, flag_rcs);
		
		
	}



	public Float getBeam() {
		return beam;
	}



	public String getBottom_style() {
		return bottom_style;
	}



	public Date getBuilt() {
		return built;
	}



	public Date getBuilt_demolition() {
		return built_demolition;
	}



	public Date getBuilt_initial() {
		return built_initial;
	}



	public String getCoated() {
		return coated;
	}



	public Float getCubic_capacity() {
		return cubic_capacity;
	}



	public Float getDraft() {
		return draft;
	}



	public Float getDwt() {
		return dwt;
	}



	public RELATION_OBJECT_ID_TYPE getFlag_id() {
		return flag_id;
	}



	public String getFlag_iso() {
		return flag_iso;
	}



	public String getFlag_name() {
		return flag_name;
	}



	public String getFlag_rcs() {
		return flag_rcs;
	}



	public String getHull_style() {
		return hull_style;
	}



	public String getImo() {
		return imo;
	}



	public String getIs_deactived() {
		return is_deactived;
	}



	public Float getLoa() {
		return loa;
	}



	public Integer getMmsi() {
		return mmsi;
	}



	public String getSide_style() {
		return side_style;
	}



	public Float getTeu() {
		return teu;
	}



	public String getProduct() {
		return product;
	}



	public Float getTpc() {
		return tpc;
	}



	

}
