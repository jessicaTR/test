package com.thomsonreuters.ce.dbor.vessel.location;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import oracle.jdbc.OracleTypes;
import oracle.sql.CLOB;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.WKTReader2;

import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.database.EasyConnection;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class Zone implements java.io.Serializable {
	
	public static Geometry[] GlobalZones;
	
	static 
	{
		GlobalZones = new Geometry[8];
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
		
		try {
			GlobalZones[0]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, 0.0 90.0, -90 90, -90.0 0.0, 0.0 0.0))");
			GlobalZones[1]=createPolygonByWKT(geometryFactory, "POLYGON ((-90.0 0.0, -90.0 90.0, -180.0 90.0, -180.0 0.0, -90.0 0.0))");
			GlobalZones[2]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, 90.0 0.0, 90.0 90.0, 0 90, 0.0 0.0))");
			GlobalZones[3]=createPolygonByWKT(geometryFactory, "POLYGON ((90.0 0.0, 180.0 0.0, 180.0 90.0, 90.0 90.0, 90.0 0.0))");
			GlobalZones[4]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, -90.0 0.0, -90.0 -90.0, 0 -90, 0.0 0.0))");
			GlobalZones[5]=createPolygonByWKT(geometryFactory, "POLYGON ((-90.0 0.0, -180.0 0.0, -180.0 -90.0, -90.0 -90.0, -90.0 0.0))");
			GlobalZones[6]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, 0.0 -90.0, 90 -90, 90.0 0.0, 0.0 0.0))");
			GlobalZones[7]=createPolygonByWKT(geometryFactory, "POLYGON ((90.0 0.0, 90.0 -90.0, 180.0 -90.0, 180.0 0.0, 90.0 0.0))");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			throw new SystemException("GlobalZones can not be created", e);
		}
	}
	
	private static final String GetVesselZones = "select czo_id, axs_id axsmarine_id, zone_name name, zone_type, vzc.axsmarine_name classification_name "
		+"from vessel_zone_v vzo, product_mapping pma, vessel_zone_classification vzc "
		+"where vzo.vzo_id is not null "
		+"and vzo.vzo_id = pma.vzo_id "
		+"and pma.vzc_id = vzc.id "
		+"and exists ( "
		+"select 1 from vessel_zone_geometry t where t.id = vzo.vzo_id and t.polygon.st_isvalid() = 1 ) "
		+"union all "
		+"select czo_id, axs_id axsmarine_id, zone_name name, zone_type, null classification_name "
		+"from vessel_zone_v vzo "
		+"where exists ( "
		+"select 1 from port_geometry t where t.id = vzo.por_id and t.polygon.st_isvalid() = 1 "
		+"union all "
		+"select 1 from anchorage_geometry t where t.id = vzo.anc_id and t.polygon.st_isvalid() = 1 "
		+"union all "
		+"select 1 from berth_geometry t where t.id = vzo.ber_id and t.polygon.st_isvalid() = 1 ) ";
	
	private static final String GetWKT = "{call cef_cnr.kml_util_pkg.get_ves_placemark_info_proc(?, ?, ?, ?)}";

	private final static String Get_Skip_Zone_Types = "select value from application_parm_cfg where application_name='VESSEL_LOAD_PKG' and parameter_name='SKIP_EVENT_TYPES'";
	
	public static HashMap<Integer, Zone> ZoneMap = null;
	
	public int getCzo_ID() {
		return Czo_ID;
	}
	
	public int getAxsmarine_ID() {
		return Axsmarine_ID;
	}
	public String getName() {
		return Name;
	}
	public Geometry getPolygon() {
		return Polygon;
	}
	
	public String getZone_type() {
		return Zone_type;
	}	
	
	public boolean IntersectedWithGlobalZone(Integer GZoneIdx)
	{
		return IntersectedGlobalZones.contains(GZoneIdx);
	}
	
	public Zone(int czo_id, int axsmarine_id, String name, String zone_type, Geometry polygon, ArrayList<Integer> intersectedglobalzones) {
		super();
		this.Czo_ID = czo_id;
		this.Axsmarine_ID = axsmarine_id;
		this.Zone_type=zone_type;
		this.Name = name;
		this.Polygon = polygon;
		this.IntersectedGlobalZones = intersectedglobalzones;
	}
	
	private int Czo_ID;
	private int Axsmarine_ID;
	private String Name;
	private String Zone_type;
	private Geometry Polygon;
	private ArrayList<String> Zone_Classifications=new ArrayList<String>();
	private ArrayList<Integer> IntersectedGlobalZones = new ArrayList<Integer>();
	
	public void AddClassification(String cf)
	{
		if (!Zone_Classifications.contains(cf))
		{
			Zone_Classifications.add(cf);
		}
	}
	
	public boolean HasClassification(String cf)
	{
		return this.Zone_Classifications.contains(cf);
	}
	////////////////////////////////////////////////////////////////////////
	//overwrite equals() method
	////////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof Zone) {
			if (((Zone) x).getCzo_ID() == this.getCzo_ID()) {
				return true;
			}
		}
		return false;
	}

	////////////////////////////////////////////////////////////////////////
	//overwrite hashCode() method
	////////////////////////////////////////////////////////////////////////
	public int hashCode() {
		return this.Czo_ID;
	}
	
	//////////////////////////////
	//Load Vessel Zones from database
	////////////////////////////
	public static void LoadZonePolygons() throws IOException
	{
		DiskInstance<HashMap<Integer, Zone>> DI=new DiskInstance<HashMap<Integer, Zone>>(VTLocationLoader.ZoneDumpFileName);

		synchronized(DBConnNames.CEF_CNR)
		{
			if (DI.Exists() && !CheckZoneUpdate())
			{
				try {
					ZoneMap=DI.GetInstance();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				ZoneMap=GetAllZonesFromDB();
				DI.SaveInstance(ZoneMap);

			}
		}

	}
	
	public static boolean CheckZoneUpdate()
	{
	    Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);
	    CallableStatement callableeventSm=null;
	    
	    try {
		callableeventSm = DBConn.prepareCall("{ ? = call zone_maintain_pkg.reset_zone_change_status_fn(?) }");
		callableeventSm.registerOutParameter(1, OracleTypes.VARCHAR);
		callableeventSm.setNull(2, OracleTypes.VARCHAR);
		callableeventSm.execute();
		String HasUpdate=callableeventSm.getString(1);
		callableeventSm.close();
		
		boolean NeedReload=HasUpdate.equals("Y") ? true : false;
		
		DBConn.commit();
		return NeedReload;

	    } catch (SQLException e) {
	    	VTLocationLoader.VTLocationLogger.warn(
			"Zone:  checking zone update faild:",  e);
	    }finally
	    {
		try {
		    DBConn.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
			VTLocationLoader.VTLocationLogger.warn("SQL Exception",  e);
		}
	    }
	    
	    return false;
	    
	}
	
	
	public static HashMap<Integer, Zone> GetAllZonesFromDB()
	{
		
		ArrayList<String> Skip_Zone_Type=Get_Skip_Zone_Types();

		Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);

		try {
			PreparedStatement objPreStatement = DBConn.prepareStatement(GetVesselZones);
			ResultSet objResult = objPreStatement.executeQuery();

			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);			
			HashMap<Integer, Zone> Zones = new HashMap<Integer, Zone>();	

			while (objResult.next()) {
				
				int CZO_ID = objResult.getInt("czo_id");
				
				if (!Zones.containsKey(CZO_ID))
				{
					String zone_type = objResult.getString("zone_type");
					
					
					int AXSMARINE_ID = objResult.getInt("axsmarine_id");

					////////////////////////
					//check and skip zone types
					if (Skip_Zone_Type.contains(zone_type))
					{
						continue;
					}

					String Name = objResult.getString("name");
					String Vessel_Classification=objResult.getString("classification_name");

					CallableStatement objStatement=DBConn.prepareCall(GetWKT);
					objStatement.setInt("axsmarine_id_in", AXSMARINE_ID);
					objStatement.registerOutParameter("name_out", OracleTypes.VARCHAR);
					objStatement.registerOutParameter("cur_ves_extended_data_out", OracleTypes.CURSOR);
					objStatement.registerOutParameter("polygon_out", OracleTypes.CLOB);
					objStatement.execute();

					CLOB clob = (CLOB) objStatement.getClob("polygon_out");
					
					BufferedReader br = new BufferedReader(
							clob.getCharacterStream());
					String WKT = "";
					String temp_WKT = br.readLine();
					while (temp_WKT != null) {
						WKT = WKT + temp_WKT;
						temp_WKT = br.readLine();
					}

					ResultSet result_set = (ResultSet) objStatement.getObject("cur_ves_extended_data_out");
					result_set.close();
					
					br.close();
					objStatement.close();

					Geometry polygon=null;
					try {
						polygon = createPolygonByWKT(geometryFactory, WKT);
						
						ArrayList<Integer> IntersectedGlobalZones = new ArrayList<Integer>();
						
						for (int i=0 ; i<GlobalZones.length;i++)
						{
							if (polygon.intersects(GlobalZones[i]))
							{
								IntersectedGlobalZones.add(i);
							}
						}
						
						Zone thisZone = new Zone(CZO_ID,AXSMARINE_ID, Name, zone_type, polygon, IntersectedGlobalZones);
						
						if (Vessel_Classification!=null)
						{
							thisZone.AddClassification(Vessel_Classification);
						}
						
						Zones.put(CZO_ID, thisZone);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						VTLocationLoader.VTLocationLogger.warn("Can't parse WKT:"+WKT+" for zone:"+AXSMARINE_ID+"--"+Name,e);
					}


				}
				else
				{
					Zone thisZone=Zones.get(CZO_ID);
					String Vessel_Classification=objResult.getString("classification_name");
					if (Vessel_Classification!=null)
					{
						thisZone.AddClassification(Vessel_Classification);
					}
					thisZone.AddClassification(Vessel_Classification);
				}
			}

			objPreStatement.close();
			
			return Zones;


		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new SystemException("SQL exception", e);
			
		} catch (Exception e)
		{
			throw new SystemException("Unexpected exception", e);
		}
		finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				VTLocationLoader.VTLocationLogger.warn("SQL exception",e);
			}
		}
		
	}
	
	private static Geometry createPolygonByWKT(GeometryFactory GF, String WKT)
	throws ParseException {
		WKTReader2 reader = new WKTReader2(GF);
		Geometry polygon = reader.read(WKT);
		return polygon;
	}
	
	private static ArrayList<String> Get_Skip_Zone_Types()
	{
		ArrayList<String> Skip_Zone_Types = new ArrayList<String>();

		Connection DBConn = null;
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			Statement objStatement = DBConn.createStatement();
			ResultSet result_set = objStatement.executeQuery(Get_Skip_Zone_Types);
			
			if (result_set.next()) {
				String skip_types = result_set.getString("value");
				StringTokenizer zone_type_tokenizer = new StringTokenizer(skip_types,";");

				while(zone_type_tokenizer.hasMoreTokens()) {
					String zone_type= zone_type_tokenizer.nextToken();
					Skip_Zone_Types.add(zone_type);
				}
			}
			result_set.close();
			objStatement.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			VTLocationLoader.VTLocationLogger.error(e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				VTLocationLoader.VTLocationLogger.error(e);
			}
		}
		
		return Skip_Zone_Types;
	}

}
