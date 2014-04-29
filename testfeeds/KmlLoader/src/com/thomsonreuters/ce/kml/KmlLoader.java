package com.thomsonreuters.ce.kml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Hashtable;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.kml.Starter;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.dbor.file.ZippedFileProcessor;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.tools.OracleHelper;

public class KmlLoader extends ZippedFileProcessor {
	
	private Logger logger  = Starter.thisLogger;
	private XMLStreamReader reader = null;

	String FileName;

	PreparedStatement objPreStatement = null;

	CallableStatement callableSm = null;

	Hashtable<String, String> rcsCodeHashtable = new Hashtable<String, String>();

	String mapKeyHeader = "";

	private static final String SetGenFullFlag = "{ call kml_util_pkg.set_generate_full_kml_proc(?)}";

	private static final String LoadPolygon = "{ call mpd2_cnr.gun_util_pkg.load_polygon_proc(?,?,?)}";

	public void Initialize(File FeedFile) {

	}

	public FileCategory getFileCatory(File FeedFile) {
		return FileCategory.getInstance("KML");
	}

	public Date strToDateLong(String strDate) {
		if (strDate != null) {
			SimpleDateFormat formatter = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ss");
			ParsePosition pos = new ParsePosition(0);
			Date strtodate = new Date(formatter.parse(strDate, pos).getTime());
			return strtodate;
		}
		return null;
	}

	public Date strToDate(String strDate) {
		if (strDate != null) {
			SimpleDateFormat formatter = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			ParsePosition pos = new ParsePosition(0);
			Date strtodate = new Date(formatter.parse(strDate, pos).getTime());
			return strtodate;
		}
		return null;
	}

	public void ProcessFile(String filename, InputStream IS) {

		FileName = filename;
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		
		try {
			//need read cfg file first for those NOT part kml file
			if(!filename.contains("Part.kml")) 
				ReadConfig("taskcontroller/cfg/KmlLoader/" + filename + ".cfg"); 
			DBConn.setAutoCommit(false);
			callableSm = DBConn.prepareCall(LoadPolygon);

			XMLInputFactory factory = XMLInputFactory.newInstance();

			reader = new StreamReaderDelegate(factory.createXMLStreamReader(IS)) {
				@Override
				public int next() throws XMLStreamException {
					while (true) {
						int event = super.next();
						if (event == XMLStreamConstants.START_ELEMENT) {
							String ELocalName = getLocalName();
							if (ELocalName.equals("SimpleData")
									|| ELocalName.equals("Data")
									|| ELocalName.equals("value")
									|| ELocalName.equals("MultiGeometry")
									|| ELocalName.equals("Polygon")
									|| ELocalName.equals("outerBoundaryIs")
									|| ELocalName.equals("innerBoundaryIs")
									|| ELocalName.equals("coordinates")) {
								return event;
							}
						} else if (event == XMLStreamConstants.END_ELEMENT) {
							String ELocalName = getLocalName();
							if (ELocalName.equals("Placemark")
									|| ELocalName.equals("Polygon")
									|| ELocalName.equals("outerBoundaryIs")
									|| ELocalName.equals("innerBoundaryIs")) {
								return event;
							}
						} else if (event == XMLStreamConstants.END_DOCUMENT) {
							return event;
						}
						continue;
					}
				}
			}; // end of reader definition

			ProcessPolygonSectios(DBConn);
			// call proc to set the generate full file flag
			CallableStatement SetGenFullFlagSt = DBConn
					.prepareCall(SetGenFullFlag);
			SetGenFullFlagSt.setString(1, "Y");
			SetGenFullFlagSt.execute();
			SetGenFullFlagSt.close();
			DBConn.commit();
		} catch (SQLException e) {
			throw new SystemException(e.getMessage(), e);
		} catch (XMLStreamException e) {
			throw new SystemException(e.getMessage(), e);
		} catch (Exception e) {
			throw new SystemException(e.getMessage(), e);
		} finally {
			try {
				if (callableSm != null)
					callableSm.close();
				if (reader != null)
					reader.close();
				if (DBConn != null)
					DBConn.close();
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(), e);
			} catch (XMLStreamException e) {
				// TODO Auto-generated catch block
				throw new SystemException(e.getMessage(), e);
			}
		}// end of try-catch
	}// end of processFile

	public void ProcessPolygonSectios(Connection DBConn)
			throws XMLStreamException {

		int event = reader.next();
		String itemName = "";
		boolean multiFlag = false;
		String tmpPolygon = "";
		String polygon = "";
		String rcsCode = "";
		String districtCodeEle = "";

		try {
			while (true) {
				switch (event) {
				case XMLStreamConstants.START_ELEMENT:
					itemName = reader.getLocalName();
					if(FileName.contains("Part.kml")){
						
						if (itemName.equals("name"))//placemark name
						{
							districtCodeEle = reader.getElementText();
							break;
						}
					 
						if (itemName.equals("Data")) 
						{
							for (int i = 0, n = reader.getAttributeCount(); i < n; ++i) {
								String value = reader.getAttributeValue(i).toUpperCase();
								if (value.equals("RCS")){ 	
									rcsCode = "Y";
									break;									
								}
							}
							break;
						}
						if(rcsCode !=null && rcsCode.equals("Y") && itemName.equals("value")){
							rcsCode = reader.getElementText();
							break;
						}
							 
					}else{			
						if (itemName.equals("SimpleData")) {
							for (int i = 0, n = reader.getAttributeCount(); i < n; ++i) {
								String value = reader.getAttributeValue(i).toUpperCase();
								if (value.equals(mapKeyHeader)) {
										districtCodeEle = reader.getElementText();
										rcsCode = rcsCodeHashtable.get(districtCodeEle);
										break;
								}
							} 
							break;
						}
					}
										
			

					if (itemName.equals("MultiGeometry")) {
						multiFlag = true;
						polygon = "MULTIPOLYGON (";
						break;
					}

					if (itemName.equalsIgnoreCase("Polygon") && rcsCode != null
							&& !rcsCode.equals("")) {
						if (tmpPolygon.equals(""))
							tmpPolygon = KMLPolygonToWKTPolygon(reader,
									districtCodeEle);
						else {
							tmpPolygon = tmpPolygon
									+ ","
									+ KMLPolygonToWKTPolygon(reader,
											districtCodeEle);
						}
						break;
					}

					break;
				case XMLStreamConstants.END_ELEMENT:
					itemName = reader.getLocalName();
					if ((itemName.equalsIgnoreCase("Placemark") && !multiFlag))// single
																				// polygon
					{
						if (rcsCode != null && !rcsCode.equals("")) {
							polygon = "POLYGON " + tmpPolygon;
						
							GetReadyParameters(DBConn, rcsCode, polygon,
									districtCodeEle);
						}

						districtCodeEle = "";
						rcsCode = "";
						tmpPolygon = "";
						polygon = "";
						break;
					} else if (multiFlag
							&& itemName.equalsIgnoreCase("Placemark")) // multi
																		// polygon
					{
						if (rcsCode != null && !rcsCode.equals("")) {
							polygon = polygon + tmpPolygon + ")";
							GetReadyParameters(DBConn, rcsCode, polygon,
									districtCodeEle);
						}

						districtCodeEle = "";
						rcsCode = "";
						tmpPolygon = "";
						polygon = "";
						multiFlag = false;
						break;
					}

				}// end of switch

				if (!reader.hasNext())
					break;

				event = reader.next();
			}// end of while
		} catch (Exception e) {
//			e.printStackTrace();
			this.LogDetails(MsgCategory.WARN,
					"KmlLoader: process polygon failed for file " + FileName + ":"
							+ e.getMessage());
			logger.warn(
					"KmlLoader: process polygon failed for file " + FileName + ":"
					+ e.getMessage());
		}
	}

	/*
	 * parse <Polygon> element
	 */
	public String KMLPolygonToWKTPolygon(XMLStreamReader reader, String district) {
		String tmp_polygon_str = "(";
		try {
			int boundary_count = 0;

			int event = reader.getEventType();
			boolean isOuter = false;
			boolean isInner = false;
			StringBuffer tmp_str_buff = new StringBuffer("");
			boolean isEnd = false;

			while (!isEnd) {
				String itemName = "";
				switch (event) {
				case XMLStreamConstants.START_ELEMENT:
					itemName = reader.getLocalName();
					if (itemName.equalsIgnoreCase("outerBoundaryIs")) {
						isOuter = true;
						tmp_str_buff = new StringBuffer("(");
						break;
					}
					if (itemName.equalsIgnoreCase("innerBoundaryIs")) {
						isInner = true;
						tmp_str_buff = new StringBuffer("(");
						break;
					}
					if (isOuter && itemName.equalsIgnoreCase("coordinates")) {
						// if it's outerboundary then process coordinates

						String[] coordinates = reader.getElementText().trim().split(
								" ");
						for (int j = 0; j < coordinates.length; j++) {
							
							String[] tmp = coordinates[j].split(",");
							//three dimensions (a,b,0), only need (a,b)
							if(tmp.length>2)  coordinates[j]=tmp[0].trim()+","+tmp[1].trim() ;
							
							if (j == 0)
								tmp_str_buff.append(coordinates[j].replace(",",
										" "));
							else
								tmp_str_buff.append(","
										+ coordinates[j].replace(",", " "));
						}
						tmp_str_buff.append(")");

						if (boundary_count == 0)
							tmp_polygon_str = tmp_polygon_str
									+ tmp_str_buff.toString();
						else {
							tmp_polygon_str = tmp_polygon_str + ","
									+ tmp_str_buff;
						}
						boundary_count++;
						break;
					}
					if (isInner && itemName.equalsIgnoreCase("coordinates")) {
						// if it's innerboundary then process coordinates in
						// reverse order
						String[] coordinates = reader.getElementText().trim().split(
								" ");
						for (int j = coordinates.length - 1; j >= 0; j--) {
							String[] tmp = coordinates[j].split(",");
							//three dimensions (a,b,0), only need (a,b)
							if(tmp.length>2)  coordinates[j]=tmp[0]+","+tmp[1];
							
							if (j == coordinates.length - 1)
								tmp_str_buff.append(coordinates[j].replace(",",
										" "));
							else
								tmp_str_buff.append(","
										+ coordinates[j].replace(",", " "));
						}

						tmp_str_buff.append(")");

						if (boundary_count == 0)
							tmp_polygon_str = tmp_polygon_str
									+ tmp_str_buff.toString();
						else {
							tmp_polygon_str = tmp_polygon_str + ","
									+ tmp_str_buff;
						}
						boundary_count++;
					}

					break;
				case XMLStreamConstants.END_ELEMENT:
					itemName = reader.getLocalName();
					if (itemName.equalsIgnoreCase("outerBoundaryIs")) {
						isOuter = false;
						break;
					}
					if (itemName.equalsIgnoreCase("innerBoundaryIs")) {
						isInner = false;
						break;
					}
					if (itemName.equalsIgnoreCase("Polygon")) {
						isEnd = true;
						break;
					}

				case XMLStreamConstants.START_DOCUMENT:
					break;
				case XMLStreamConstants.END_DOCUMENT:
					break;
				}
				if (!isEnd)
					event = reader.next();
			}// end of while

		} catch (Exception e) {
			this.LogDetails(MsgCategory.WARN,
					"KmlLoader: KML to WKT failed for district:" + district
							+ " in file " + FileName + ":"
							+ e.getMessage());
			logger.warn("KmlLoader: KML to WKT failed for district:" + district
							+ " in file " + FileName + ":"
							+ e.getMessage());
		}
		tmp_polygon_str = tmp_polygon_str + ")";

		return tmp_polygon_str;

	}

	private void GetReadyParameters(Connection DBConn, String rcsCode,
			String polygon, String district) {
		try {
			callableSm.clearParameters();

			@SuppressWarnings("deprecation")
			oracle.sql.CLOB _oClob = OracleHelper.getCLOB(DBConn, polygon);
			callableSm.setString(1, rcsCode);
			callableSm.setObject(2, _oClob);
			String replace_latest = null;

			if (FileName.indexOf(".Part.") > 0)
				replace_latest = "N";
			else
				replace_latest = "Y";

			callableSm.setString(3, replace_latest);
			try {
				callableSm.execute();
				DBConn.commit();

			} catch (SQLException e) {
				this.LogDetails(MsgCategory.WARN,
						"KmlLoader: Execute failed for district:" + district
								+ " of file " + FileName + ":"
								+ e.getMessage());
				logger.warn("KmlLoader: Execute failed for district:" + district
								+ " of file " + FileName + ":"
								+ e.getMessage());
			}
			// batchNum++;
			// rowNum++;
		} catch (Exception e) {
			this.LogDetails(MsgCategory.WARN,
					"KmlLoader: Execute failed for district:" + district
							+ " of file " + FileName + ":"
							+ e.getMessage());
			logger.warn("KmlLoader: Execute failed for district:" + district
							+ " of file " + FileName + ":"
							+ e.getMessage());
		}

	}

	private void ReadConfig(String FileName) {

		String[] arrayLine = null;
		try {
			File csv = new File(FileName);
			BufferedReader br = new BufferedReader(new FileReader(csv));

			String line = "";
			if ((line = br.readLine()) != null) {
				mapKeyHeader = (line.split(","))[0];// first column in header
			}

			while ((line = br.readLine()) != null) {

				arrayLine = line.split(",");
				rcsCodeHashtable.put(arrayLine[0], arrayLine[1]);
			}
			br.close();

		} catch (FileNotFoundException e) {

			throw new SystemException(
					"File not found while reading KML configuration file: "
							+ FileName);

		} catch (IOException e) {
			throw new SystemException(
					"Error occurs while reading KML configuration file: "
							+ FileName, e);
		}
	}

	public void Finalize() {
	}

}
