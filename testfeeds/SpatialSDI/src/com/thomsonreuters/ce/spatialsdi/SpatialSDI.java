package com.thomsonreuters.ce.spatialsdi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import oracle.jdbc.OracleCallableStatement;
import oracle.sql.CLOB;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;





import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.cache.ProcessingStatus;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.dbor.file.ExcelProcessor;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.dbor.schedule.DateFun;
import com.thomsonreuters.ce.dbor.schedule.Schedule;
import com.thomsonreuters.ce.dbor.schedule.ScheduleType;
import com.thomsonreuters.ce.dbor.server.DBConnNames;


public class SpatialSDI extends ControlledThread {

	public SpatialSDI(ThreadController tc) {
		super(tc);
		// TODO Auto-generated constructor stub
	}

	private long FPH_ID;

	public Document doc = null;
	
	private Logger logger  = Starter.thisLogger;

	private static final String configfile = "taskcontroller/cfg/SpatialSDI/SpatialSDIFullLoad.conf";

	private static final String InsertFileProcessHistory = "insert into file_process_history (id, file_name,dit_file_category_id,start_time,dit_processing_status) values (fph_seq.nextval,?,?,sysdate,?)";

	private static final String GetProcessingDetail = "select count(*) from processing_detail where fph_id = ? and dit_message_category_id in (select id from dimension_item where value='WARNING')";

	private static final String CompleteFileHistory = "update file_process_history set end_time=sysdate, dit_processing_status=? where id=?";

	private static final String GetGenFullFlag = "{ ?= call kml_util_pkg.get_generate_full_kml_fn()}";

	private static final String GetGenSupportFlag = "{ ?= call kml_util_pkg.get_generate_support_kml_fn()}";

	private static final String SetGenFullFlag = "{ call kml_util_pkg.set_generate_full_kml_proc(?)}";

	private static final String SetGenSupportFlag = "{ call kml_util_pkg.set_generate_support_kml_proc(?)}";

	private static final String GetGunContent = "{call kml_util_pkg.get_geo_placemark_info_proc(?,?,?,?)}";

	private static final String GetVesContent = "{call kml_util_pkg.get_ves_placemark_info_proc(?,?,?,?)}";

	private static final String GetGudFull = "{call kml_util_pkg.get_gun_id_proc(?)}";

	private static final String GetVesFull = "{call kml_util_pkg.get_vessel_zone_id_proc(?)}";
	
	private static final String GetPortBerthAnchFull = "{call kml_util_pkg.get_vessel_zone_id_proc(?)}";

	private static Date SDITimeStamp = null;

	private char scheduletype;

	private String scheduletime;

	private long interval;

	private long offset;

	public String filelocation;

	public String tempfilelocation;

	String SupportFlag = "";

	BufferedWriter bw = null;

	File thisTempFile = null;

	private String geographicunitheader = "";

	private String vesselzoneheader = "";

	public void ControlledProcess() {
		
		ReadConfiguration();
		tempfilelocation = Starter.TempFolder;
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		if (SDITimeStamp == null) {
			Date FirstRunningTime = CalculateFirstRunningTime();
			System.out.println("First execute time:"
					+ DateFun.getStringDate(FirstRunningTime));
			SpatialSDI SS=new SpatialSDI(this.TC);
//			SS.setThreadController(this.TC);
			Starter.TimerService
					.createTimer(FirstRunningTime, interval,SS);
		} else {
			try {
				String filename = "";
				String FullFlag = getFullFlag(DBConn);
				SupportFlag = getSupportFlag(DBConn);

				if (SupportFlag != null && !SupportFlag.trim().equals("")) {
					// Generate file
					filename = "GeographicUnit_" + getStringDate(SDITimeStamp)
							+ ".Part.kml.gz";
					CreateFileProcessHistory(filename);
					SDIProducer("GeographicUnit", filename, "SUPPORT",DBConn);
					CallableStatement SetGenSupportFlagSt = DBConn
							.prepareCall(SetGenSupportFlag);
					SetGenSupportFlagSt.setString(1, "N");
					SetGenSupportFlagSt.execute();
					SetGenSupportFlagSt.close();

					DBConn.commit();
				}
				
				if (FullFlag != null && FullFlag.equals("Y")) {
					//Generate port/berth/anchorage kml file
					filename = "PortBerthAnchorage_" + getStringDate(SDITimeStamp)
							+ ".Full.kml.gz";
					CreateFileProcessHistory(filename);
					System.out
							.println("Start to generate port/berth/anchorage kml file!");
					SDIProducer("PortBerthAnchorage", filename, "FULL",DBConn);
//					 Generate vessel zone file
					filename = "VesselZone_" + getStringDate(SDITimeStamp)
							+ ".Full.kml.gz";
					CreateFileProcessHistory(filename);
					System.out
							.println("Start to generate vessel zone kml file!");
					SDIProducer("VesselZone", filename, "FULL",DBConn);

					// Generate geographic unit file
					filename = "GeographicUnit_" + getStringDate(SDITimeStamp)
							+ ".Full.kml.gz";
					CreateFileProcessHistory(filename);
					System.out
							.println("Start to generate geographic unit kml file!");
					SDIProducer("GeographicUnit", filename, "FULL",DBConn);
					
					CallableStatement SetGenFullFlagSt = DBConn
							.prepareCall(SetGenFullFlag);
					SetGenFullFlagSt.setString(1, "N");
					SetGenFullFlagSt.execute();
					SetGenFullFlagSt.close();

					DBConn.commit();
				}				

			} catch (Exception e) {

				if (e instanceof LogicException) {
					logger.warn(
							"Logic Exception is captured while producing Spatial SDI file for "
									+ getStringDate(SDITimeStamp), e);

				} else if (e instanceof SystemException) {
					SystemException se = (SystemException) e;
					logger.warn(
									"System Exception: "
											+ se.getEventID()
											+ " is captured while producing Spatial SDI file for "
											+ getStringDate(SDITimeStamp), se);
				} else {
					SystemException se = new SystemException(e);
					logger.warn(
									"System Exception: "
											+ se.getEventID()
											+ " is captured while producing Spatial SDI file for "
											+ getStringDate(SDITimeStamp), se);
				}
			} finally {
				try {

					DBConn.close();
				} catch (SQLException e) {
					throw new SystemException(e.getMessage(), e);
				}

			}

			CompleteFileHis();
 
			// Calculate next running time
			SDITimeStamp = new Date(SDITimeStamp.getTime() + interval);

			Date nextExecuteTime = new Date(SDITimeStamp.getTime() + offset);
			System.out.println("Next execute time for Fullload scan:"
					+ DateFun.getStringDate(nextExecuteTime));
			logger.info(
					"Next execute time for Fullload scan:"
							+ DateFun.getStringDate(nextExecuteTime));
			
			SpatialSDI SS=new SpatialSDI(this.TC);
//			SS.setThreadController(this.TC);
			Starter.TimerService.createTimer(nextExecuteTime, interval,SS);
		}

	}

	public void SDIProducer(String FileType, String fileName, String flag,Connection DBConn) {
		// flag: FULL or SUPPORT
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			doc = null;
			doc = builder.newDocument();
			doc.setStrictErrorChecking(false);

			vesselzoneheader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
					+ "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\r\n"
					+ "<Document>\r\n"
					+ "<Style id=\"SEA\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"DRY ZONE\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"LINER ZONE\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"TANKER ZONE\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"WAYPOINT\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"PORT\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"RIVER\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"BERTH\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"General\">"
					+ "<LineStyle><color>ff0000ff</color><width>1</width></LineStyle><PolyStyle><color>ff0000cc</color><fill>0</fill></PolyStyle>"
					+ "</Style>";
			// "<Folder>";

			geographicunitheader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
					+ "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\r\n"
					+ "<Document>\r\n"
					+ "<Style id=\"Country_GroupStyles\">"
					+ "<LineStyle><color>ff0000ff</color></LineStyle>  <PolyStyle><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"CountryStyles\">"
					+ "<LineStyle><color>ff0000ff</color></LineStyle>  <PolyStyle><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"Sub_CountryStyles\">"
					+ "<LineStyle><color>ff0000ff</color></LineStyle>  <PolyStyle><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"CityStyles\">"
					+ "<LineStyle><color>ff0000ff</color></LineStyle>  <PolyStyle><fill>0</fill></PolyStyle>"
					+ "</Style>\r\n"
					+ "<Style id=\"ZoneStyles\">"
					+ "<LineStyle><color>ff0000ff</color></LineStyle>  <PolyStyle><fill>0</fill></PolyStyle>"
					+ "</Style>";
			// +"<Folder>";

			String content = null;
			if (FileType.equals("VesselZone"))
				content = vesselzoneheader;
			else if (FileType.equals("GeographicUnit"))
				content = geographicunitheader;
			else if(FileType.equals("PortBerthAnchorage"))
				content = vesselzoneheader;

			GenZipFile(content, fileName);// just write header this time

			genPolygonLoop(FileType, flag,DBConn);

			CloseZipfile(thisTempFile, fileName);

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			throw new SystemException(e);
		}
	}

	private String getFullFlag(Connection DBConn) {

		CallableStatement getGenFullFlagSt;
		String FullFlag;
		try {
			getGenFullFlagSt = DBConn.prepareCall(GetGenFullFlag);
			getGenFullFlagSt.registerOutParameter(1, Types.VARCHAR);
			getGenFullFlagSt.execute();
			FullFlag = getGenFullFlagSt.getString(1);
			getGenFullFlagSt.close();
			return FullFlag;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new LogicException(e);
		}
	}

	private String getSupportFlag(Connection DBConn) {

		CallableStatement getGenSupportFlagSt;
		String SupportFlag;
		try {
			getGenSupportFlagSt = DBConn.prepareCall(GetGenSupportFlag);
			getGenSupportFlagSt.registerOutParameter(1, Types.VARCHAR);
			getGenSupportFlagSt.execute();
			SupportFlag = getGenSupportFlagSt.getString(1);
			getGenSupportFlagSt.close();
			return SupportFlag;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new LogicException(e);
		}

	}

	private void genPolygonLoop(String FileType, String flag,Connection DBConn) {

		try {

			if (flag.equals("FULL")) {
				CallableStatement getIdSt = null;
				if (FileType.equals("GeographicUnit")) {
					getIdSt = DBConn.prepareCall(GetGudFull);
				} else if (FileType.equals("VesselZone")) {
					getIdSt = DBConn.prepareCall(GetVesFull);
				} else if (FileType.equals("PortBerthAnchorage")){//generate port/berth/anchorage kml and save in table
					getIdSt = DBConn.prepareCall(GetPortBerthAnchFull);
				}

				getIdSt.registerOutParameter(1, oracle.jdbc.OracleTypes.CURSOR);
				getIdSt.execute();
				ResultSet rs = ((OracleCallableStatement) getIdSt).getCursor(1);
				String folderName = null;
				String nextFolderName = null;
				String tmpName = null;
				String style = null;
				while (rs.next()) {
					tmpName = rs.getString(1);
					if( FileType.equals("PortBerthAnchorage")){
						if(!tmpName.equals("PORT")&&!tmpName.equals("BERTH")&&!tmpName.equals("ANCHORAGE"))
							continue;
					}
					
//					if(tmpName.equals("DRY")||tmpName.equals("TANKER")||tmpName.equals("LINER"))
//						tmpName = tmpName+" ZONE";
					if (folderName == null && nextFolderName == null)
						folderName = tmpName;
					else
						nextFolderName = tmpName;

					if (nextFolderName == null
							|| !nextFolderName.equals(folderName)) {
						if (nextFolderName != null) {
							folderName = nextFolderName;
							AppendZipFile("  </Folder>");
						}
						
						AppendZipFile("  <Folder><name>" + folderName
								+ "</name>");
						if (FileType.equals("VesselZone") || FileType.equals("PortBerthAnchorage")){
							if(folderName.equals("DRY ZONE") || folderName.equals("LINER ZONE")  || folderName.equals("TANKER ZONE")||folderName.equals("PORT")||folderName.equals("RIVER")||folderName.equals("SEA")||folderName.equals("WAYPOINT")||folderName.equals("BERTH")) 
								style = folderName;
							else
								style = "General";
						}
					}
					genPolygon(FileType, rs.getInt(2),DBConn,style);
				}
				rs.close();
				getIdSt.close();
			}

			if (flag.equals("SUPPORT")) {
				// Element folder = doc.createElement("Folder");
				// doc.appendChild(folder);
				AppendZipFile("  <Folder>");
				String[] tmp = SupportFlag.split(";");
				for (int i = 0; i < tmp.length; i++) {
					int gud_id = -1;
					try {
						gud_id = Integer.valueOf(tmp[i]);
						genPolygon(FileType, gud_id,DBConn,"");
					} catch (NumberFormatException e) {
						logger.warn(
								"SpatialSDI: gud_id is not valid number for support sdi: "
										+ tmp[i] + "!");
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.warn(
					"SpatialSDI: error occurs when getting IDs in genPolygonLoop: "
							+ e.getMessage());
		}
	}

	// private void genGunPolygonLoop(ArrayList<Integer> IdList){
	private void genPolygon(String FileType, Integer inputId,Connection DBConn, String style) {

		String polygon = "";
		CLOB polygonClob = null;

		String type = "";
		String placemarkName = "";
		CallableStatement getPolygonSt = null;
		try {

			if (FileType.equals("GeographicUnit"))
				getPolygonSt = DBConn.prepareCall(GetGunContent);
			else if (FileType.equals("VesselZone"))
				getPolygonSt = DBConn.prepareCall(GetVesContent);
			else if (FileType.equals("PortBerthAnchorage"))
				getPolygonSt = DBConn.prepareCall(GetVesContent);
			ResultSet rs = null;
			ResultSetMetaData metaData = null;

			getPolygonSt.setInt(1, inputId);
			getPolygonSt.registerOutParameter(2,
					oracle.jdbc.OracleTypes.VARCHAR);
			getPolygonSt
					.registerOutParameter(3, oracle.jdbc.OracleTypes.CURSOR);
			getPolygonSt.registerOutParameter(4, oracle.jdbc.OracleTypes.CLOB);
			getPolygonSt.execute();
			placemarkName = getPolygonSt.getString(2);
			rs = ((OracleCallableStatement) getPolygonSt).getCursor(3);// get
			// extended
			// data
			polygonClob = (oracle.sql.CLOB) getPolygonSt.getClob(4);// get
																	// polygon
			if (polygonClob != null) {

				polygon = ClobToString(polygonClob);

				Element placemark = doc.createElement("Placemark");
				doc.appendChild(placemark);
				Element name = doc.createElement("name");
				placemark.appendChild(name);
				name.setTextContent(placemarkName);

				Element extData = doc.createElement("ExtendedData");
				placemark.appendChild(extData);

				metaData = rs.getMetaData(); // get extended meta data

				String anchor_str = "";//for vessel zone POLYGON((0 0)), convert to <Point><coordinates>center point</coordinates></Point>
				while (rs.next()) {
					for (int i = 1; i <= metaData.getColumnCount(); i++) {

						Element data = doc.createElement("Data");
						extData.appendChild(data);
						data.setAttribute("name", metaData.getColumnLabel(i));
						Element value = doc.createElement("value");
						value.setTextContent(rs.getString(i));
						data.appendChild(value);

						if (metaData.getColumnLabel(i).equals("Type")){
							type = rs.getString(i);							
						}
						else if(metaData.getColumnLabel(i).equals("Anchor"))
							anchor_str = rs.getString(i);
					}
				}
				
				rs.close();
				getPolygonSt.close();				

				Element styleUrl = doc.createElement("styleUrl");
				placemark.appendChild(styleUrl);
				
				if (FileType.equals("GeographicUnit"))
					styleUrl.setTextContent("#" + type);
				else if (FileType.equals("VesselZone")||FileType.equals("PortBerthAnchorage"))
					styleUrl.setTextContent("#" + style);				

				if (polygon != null && !polygon.equals("")) {
					if(polygon.startsWith("POINT")){						
						Element pointEle = doc.createElement("Point");
						placemark.appendChild(pointEle);
						Element coordEle = doc.createElement("coordinates");
						pointEle.appendChild(coordEle);
						coordEle.setTextContent(anchor_str);
					}else{						
						Element ele = WKTToKML(polygon);
						placemark.appendChild(ele);
					}
				} else {// <Polygon><outerBoundaryIs><LinearRing><coordinates>
					Element polygonEle = doc.createElement("Polygon");
					placemark.appendChild(polygonEle);
					Element boundaryEle = doc.createElement("outerBoundaryIs");
					polygonEle.appendChild(boundaryEle);
					Element linearEle = doc.createElement("LinearRing");
					boundaryEle.appendChild(linearEle);
					Element coordinatesEle = doc.createElement("coordinates");
					linearEle.appendChild(coordinatesEle);
				}

				AppendZipFile("    " + getElementAsXml(placemark));
				// System.out.println(getElementAsXml(placemark));
			}// end of if polygon!=null
		}
		// catch (SQLException e) {
		// LoggerHelper.log(LogLevel.WARN,
		// "Spatial SDI: Error when generate polygon for " +placemarkName, e);
		// }
		catch (Exception e) {
			logger.warn(
					"SpatialSDI: Error when generate polygon for "
							+ placemarkName, e);
			System.out.print("Spatial SDI: Error when generate polygon: "
					+ e.getMessage());
		} finally {
			try {
				 if (getPolygonSt!=null) getPolygonSt.close();
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(), e);
			}
		}

	} 

	private Element WKTToKML(String wktIn) {
		Element ele = null;
		if (wktIn.startsWith("MULTIPOLYGON")) {
			ele = doc.createElement("MultiGeometry");
			doc.appendChild(ele);
			String tmp_str = wktIn.substring(14, wktIn.length() - 1);
//			String[] tmp_strs = tmp_str.replaceAll("\\)\\),\\(\\(",
//					"\\)\\)\\|\\(\\(").split("\\|");
			tmp_str = tmp_str.replaceAll("\\)\\), \\(\\(",
			"\\)\\),\\(\\(");
			String[] tmp_strs = tmp_str.replaceAll("\\)\\),\\(\\(",
			"\\)\\)\\|\\(\\(").split("\\|");
			for (int i = 0; i < tmp_strs.length; i++) {
				ele.appendChild(WKTPolygonToKMLPolygon(tmp_strs[i]));
			}
		} else if (wktIn.startsWith("POLYGON "))
			ele = WKTPolygonToKMLPolygon(wktIn.substring(8));
		else if (wktIn.startsWith("POLYGON"))
			ele = WKTPolygonToKMLPolygon(wktIn.substring(7));

		return ele;
	}

	/**
	 * the input wktIn will be like ((1 1, 2 2)) or ((1 1, 2 2), (0.5 0.5, 0.1
	 * 0.1), ... )
	 */
	private Element WKTPolygonToKMLPolygon(String wktIn) {

		Element pol = doc.createElement("Polygon");

//		String[] tmp_strs = wktIn.substring(1, wktIn.length() - 1).replaceAll(
//				"\\),\\(", "\\)\\|\\(").split("\\|");
		wktIn = wktIn.substring(1, wktIn.length() - 1).replaceAll(
				"\\), \\(", "\\),\\(");
		wktIn = wktIn.replaceAll(
				"\\),\\(", "\\)\\|\\(");
		String[] tmp_strs = wktIn.split("\\|");
		
		for (int i = 0; i < tmp_strs.length; i++) {
			Element bou = null;

			String[] coordinates = tmp_strs[i].substring(1,
					tmp_strs[i].length() - 1).split(",");
			StringBuffer coordinates_value = new StringBuffer("");
			if (i == 0) {
				bou = doc.createElement("outerBoundaryIs");
				pol.appendChild(bou);
				for (int j = 0; j < coordinates.length; j++) {
					coordinates[j] = coordinates[j].trim();
					if (j == 0)
						coordinates_value.append(coordinates[j].replaceAll(" ",
								","));
					else
						coordinates_value.append(" "
								+ coordinates[j].replaceAll(" ", ","));
				}
			} else {
				bou = doc.createElement("innerBoundaryIs");
				pol.appendChild(bou);
				for (int j = coordinates.length - 1; j >= 0; j--) {
					coordinates[j] = coordinates[j].trim();
					if (j == coordinates.length - 1)
						coordinates_value.append(coordinates[j].replaceAll(" ",
								","));
					else
						coordinates_value.append(" "
								+ coordinates[j].replaceAll(" ", ","));
				}
			}
			Element linearRing = doc.createElement("LinearRing");
			bou.appendChild(linearRing);
			Element cor = doc.createElement("coordinates");
			linearRing.appendChild(cor);
			cor.setTextContent(coordinates_value.toString());
		}
		return pol;
	}

	private String getElementAsXml(Element ele) {
		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer;
		try {
			transformer = tFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			transformer.setOutputProperty("indent", "no");
			StringWriter sw = new StringWriter();
			StreamResult result = new StreamResult(sw);
			DOMSource source = null;
			source = new DOMSource(ele);
			transformer.transform(source, result);
			// System.out.println(sw.toString());
			return (sw.toString());
		} catch (Exception e) {
			logger.warn(
					"Failed to transfor XML element to String : " + ele + "!");
			return "   ";
		}
	}

	private void GenZipFile(String strDoc, String fileName) {

		bw = null;

		String Tempfilename = fileName + ".temp";
		thisTempFile = new File(tempfilelocation, Tempfilename);

		// Delete temp file if it exists
		if (thisTempFile.exists() == true) {
			thisTempFile.delete();
		}
		try {
			thisTempFile.createNewFile();

			GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(
					thisTempFile));
			bw = new BufferedWriter(new OutputStreamWriter(zfile, "UTF8"));
			bw.write(strDoc);
			bw.flush();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			throw new SystemException(e);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			throw new SystemException(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SystemException(e);
		}

	}

	private void AppendZipFile(String strDoc) {
		try {
			bw.newLine();
			bw.write(strDoc);
			bw.flush();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SystemException(e);
		}
	}

	private void CloseZipfile(File tempFile, String fileName) {

		try {
			bw.newLine();
			bw.write("</Folder></Document></kml>");
			bw.flush();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SystemException(e);
		}

		File thisFile = new File(filelocation, fileName);

		// Delete temp file if it exists
		if (thisFile.exists() == true) {
			thisFile.delete();
		}

		this.MoveFile(tempFile, thisFile);

	}

	private void ReadConfiguration() {
		try {

			FileInputStream TaskFis = new FileInputStream(configfile);
			Properties Prop = new Properties();
			Prop.load(TaskFis);

			scheduletype = Prop.getProperty("scheduletype").toCharArray()[0];
			;
			scheduletime = Prop.getProperty("time");
			interval = Integer.parseInt(Prop.getProperty("interval"));
			offset = Integer.parseInt(Prop.getProperty("offset"));
			filelocation = Prop.getProperty("filelocation");
			// vesselzoneheader = Prop.getProperty("vesselzoneheader");
			// geographicunitheader = Prop.getProperty("geographicunitheader");
		} catch (Exception e) {
			throw new SystemException(
					"Error occurs while reading configuration file "+configfile );
		}
	}

	public Date CalculateFirstRunningTime() {

		Date AppDate = new Date();

		// Calculate next validate time;
		SDITimeStamp = (new Schedule(AppDate, ScheduleType
				.getInstance(scheduletype), scheduletime)).GetNextValidTime();

		// Calculate next validate APP time
		Date nextExecuteTime = new Date(SDITimeStamp.getTime() + offset);

		while (nextExecuteTime.before(AppDate)) {

			SDITimeStamp = new Date(SDITimeStamp.getTime() + interval);
			nextExecuteTime = new Date(SDITimeStamp.getTime() + offset);
		}

		return nextExecuteTime;
	}

	public void MoveFile(File f1, File f2) {
		try {
			int length = 1048576;
			FileInputStream in = new FileInputStream(f1);
			FileOutputStream out = new FileOutputStream(f2);
			byte[] buffer = new byte[length];

			while (true) {
				int ins = in.read(buffer);
				if (ins == -1) {
					in.close();
					out.flush();
					out.close();
					f1.delete();
					return;
				} else {
					out.write(buffer, 0, ins);
				}
			}

		} catch (Exception e) {

			throw new SystemException("File: " + f1.getName()
					+ " can not be moved to " + f2.getAbsolutePath(), e);
		}
	}

	public String getStringDate(Date time) {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HHmm");
		String dateString = formatter.format(time);
		return dateString;
	}

	public String ClobToString(CLOB clob) throws SQLException, IOException {

		String reString = "";
		Reader is = clob.getCharacterStream();
		BufferedReader br = new BufferedReader(is);
		String s = br.readLine();
		StringBuffer sb = new StringBuffer();
		while (s != null) {
			sb.append(s);
			s = br.readLine();
		}
		reString = sb.toString();
		return reString;
	}

	public void CreateFileProcessHistory(String filename) {
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		try {
			DatabaseMetaData dmd = DBConn.getMetaData();
			PreparedStatement objPreStatement = DBConn.prepareStatement(
					InsertFileProcessHistory, new String[] { "ID" });
			objPreStatement.setString(1, filename);
			objPreStatement.setInt(2, FileCategory.getInstance("Spatial SDI")
					.getID());
			objPreStatement.setInt(3, ProcessingStatus.PROCESSING.getID());

			objPreStatement.executeUpdate();

			// get ID
			if (dmd.supportsGetGeneratedKeys()) {
				ResultSet rs = objPreStatement.getGeneratedKeys();
				while (rs.next()) {
					this.FPH_ID = rs.getLong(1);
				}
				rs.close();
			}

			DBConn.commit();
			
			objPreStatement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("SQL exception", e);

		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				logger.error("SQL exception", e);
			}
		}
	}

	public void CompleteFileHis() {
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

		try {
			// if processint_detail table has records, then it's
			// COMPLETEDWITHWARNING
			int pdeCount = -1;
			PreparedStatement getPdetPreStatement = DBConn
					.prepareStatement(GetProcessingDetail);
			getPdetPreStatement.setLong(1, this.FPH_ID);
			ResultSet objResultSet = getPdetPreStatement.executeQuery();
			if (objResultSet.next()) {
				pdeCount = objResultSet.getInt(1);
			}

			objResultSet.close();
			getPdetPreStatement.close();

			PreparedStatement objPreStatement = null;
			objPreStatement = DBConn.prepareStatement(CompleteFileHistory);

			if (pdeCount <= 0) {
				objPreStatement.setInt(1, ProcessingStatus.COMPLETED.getID());
			} else {
				objPreStatement.setInt(1, ProcessingStatus.COMPLETEDWITHWARNING
						.getID());
			}

			objPreStatement.setLong(2, this.FPH_ID);
			objPreStatement.executeUpdate();
			DBConn.commit();
			objPreStatement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("SQL exception", e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				logger.error("SQL exception", e);
			}
		}
	}

}
