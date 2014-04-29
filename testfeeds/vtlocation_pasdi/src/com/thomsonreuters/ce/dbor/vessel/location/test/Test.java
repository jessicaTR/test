package com.thomsonreuters.ce.dbor.vessel.location.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import oracle.jdbc.OracleTypes;
import oracle.sql.CLOB;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.WKTReader2;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.vessel.location.Zone;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

public class Test {

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

	
	private static ArrayList<String> Get_Skip_Zone_Types()
	{
		ArrayList<String> Skip_Zone_Types = new ArrayList<String>();

		Connection DBConn = null;
		try {
			DBConn = new EasyConnection("processing_history");
			Statement objStatement = DBConn.createStatement();
			ResultSet result_set = objStatement.executeQuery("select value from application_parm_cfg where application_name='VESSEL_LOAD_PKG' and parameter_name='SKIP_EVENT_TYPES'");
			
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
			e.printStackTrace();
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return Skip_Zone_Types;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		EasyConnection.configPool("D:\\mywork\\development\\javaworkspace\\BootstrapV2\\taskcontroller\\cfg\\dbpool.conf.blue");
		HashMap<Integer, Zone> ZoneMap;
		
		
		ArrayList<String> Skip_Zone_Type=Get_Skip_Zone_Types();
		
		
		Connection DBConn= new EasyConnection("processing_history");
		
		try {
			PreparedStatement objPreStatement = DBConn.prepareStatement(GetVesselZones);
			ResultSet objResult = objPreStatement.executeQuery();

			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);			
			ZoneMap = new HashMap<Integer, Zone>();	
			
			while (objResult.next()) {
				int AXSMARINE_ID = objResult.getInt("axsmarine_id");
				
				if (!ZoneMap.containsKey(AXSMARINE_ID))
				{

					String zone_type = objResult.getString("zone_type");
					
					////////////////////////
					//check and skip zone types
					if (Skip_Zone_Type.contains(zone_type))
					{
						continue;
					}

					String Name = objResult.getString("name");
					if (!Name.equals("Jamnagar"))
					{
					    continue;
					}
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

					br.close();
					objStatement.close();

					Geometry polygon=null;
					try {
						polygon = createPolygonByWKT(geometryFactory, WKT);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}


					//Zone thisZone = new Zone(AXSMARINE_ID, Name, zone_type, polygon);
					//thisZone.AddClassification(Vessel_Classification);
					
					//ZoneMap.put(AXSMARINE_ID, thisZone);
				}
				else
				{
					Zone thisZone=ZoneMap.get(AXSMARINE_ID);
					String Vessel_Classification=objResult.getString("classification_name");
					thisZone.AddClassification(Vessel_Classification);
				}
			}

			objPreStatement.close();
			
			File tempFile = new File("D:\\mywork\\development\\javaworkspace\\vessel\\ZoneCache");	
			ObjectOutputStream OOS= new ObjectOutputStream(new FileOutputStream(tempFile));
			OOS.writeObject(ZoneMap);
			OOS.close();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		////////////////////////////////////////
		
		String VesselProductType="Passenger";
		Double Lat=61.8615D;
		Double Long=-5.703833333333334D;
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
		
		try {
			
			File TempFile=new File("D:\\mywork\\development\\javaworkspace\\new_vessel\\zonedump");
			ObjectInputStream OIS= new ObjectInputStream(new FileInputStream(TempFile));
			ZoneMap = (HashMap<Integer, Zone>)OIS.readObject();
			OIS.close();
			
			
			Coordinate coord = new Coordinate(Long, Lat);
			Point point = geometryFactory.createPoint(coord);
			
			ArrayList<Integer> CurrentZones = new ArrayList<Integer>();
			
			Integer BelongedGlobalZoneIndex=null;
			
			for (int i=0 ; i<Zone.GlobalZones.length ; i++)
			{
				if (Zone.GlobalZones[i].covers(point))
				{
					BelongedGlobalZoneIndex=i;
					break;
				}
			}
			
			
			for (Map.Entry<Integer, Zone> thisEntry : ZoneMap.entrySet()) {
				
				Zone thisZone=thisEntry.getValue();
				if (thisZone.getZone_type().equals("ZONE"))
				{
					if (VesselProductType.equals("Tankers"))
					{
						if (!thisZone.HasClassification("TANKER"))
						{
							continue;
						}
					}
					else if (VesselProductType.equals("Bulkers"))
					{
						if (!thisZone.HasClassification("DRY"))
						{
							continue;
						}
					}
					else if (VesselProductType.equals("Container / Roro"))
					{
						if (!thisZone.HasClassification("LINER"))
						{
							continue;
						}
					}
					else if (VesselProductType.equals("Miscellaneous") || VesselProductType.equals("Passenger"))
					{
						continue;
					}
				}
				
				if (thisZone.IntersectedWithGlobalZone(BelongedGlobalZoneIndex))
				{
				    if (thisZone.getAxsmarine_ID()==19269)
				    {
					System.out.println("English Channel");
				    }

				    if (thisZone.getPolygon().covers(point)) {
					CurrentZones.add(thisZone.getCzo_ID());
					System.out.println(thisZone.getCzo_ID() + ":"
						+ thisZone.getName());
				    }	
				}					
			}
			
			System.out.println("Done");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static Geometry createPolygonByWKT(GeometryFactory GF, String WKT)
	throws ParseException {
		WKTReader2 reader = new WKTReader2(GF);
		Geometry polygon = reader.read(WKT);
		return polygon;
	}
}
