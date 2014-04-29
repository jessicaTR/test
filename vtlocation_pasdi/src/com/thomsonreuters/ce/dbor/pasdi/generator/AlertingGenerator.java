package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Locale;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import oracle.jdbc.OracleConnection;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.thomsonreuters.ce.timing.DateFun;
import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstIdentifierValue;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstName;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselEventFact;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselLocationFact;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.IDENTIFIER_VALUE_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.queue.DiskCacheWriter;
import com.thomsonreuters.ce.queue.Pipe;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;


import com.thomsonreuters.ce.dbor.messaging.TRMessageProducer;

public class AlertingGenerator {
	public static final String ALERT_MESSAGE_PREFIX = "ALERT_MESSAGE";
	private int ActiveNum = 0;
	private int ThreadNum;
	private Counter count;
	protected Date CoverageStart;
	protected Date CoverageEnd;
	private Utility util;
	private Pipe<HashMap<CursorType, SDICursorRow[]>> InQ;
	

	DiskCacheWriter<String> diskWriter = null;
	static String tempLocation = SDIConstants.tempfolder;
	static String filelocation = null;
	static String filename = null;

	private final static String Scanner_Config_File = "../cfg/pasdi/alert.conf";

	static {
		try {
			Properties ScannerProp = new Properties();
			ScannerProp.load(new FileInputStream(Scanner_Config_File));

			filelocation = ScannerProp.getProperty("fileLocation");

		} catch (Exception e) {
			SDIConstants.SDILogger.warn( "Load message config file failed!",
					e);
			throw new SystemException("Load message config file failed!");
		}
	}

	public AlertingGenerator(Date CS, Date CE, int Thread_Num,
			Pipe<HashMap<CursorType, SDICursorRow[]>> InPort, Counter ct) {
		this.ThreadNum = Thread_Num;
		this.InQ = InPort;
		this.count = ct;
		this.count.Increase();

		this.CoverageStart = CS;
		this.CoverageEnd = CE;
		util = new Utility();
		filename = "alertMessage_" + DateFun.getStringDate(this.CoverageStart)
				+ "_" + DateFun.getStringDate(this.CoverageEnd);

	}

	public final void Start() {

		try {
			diskWriter = new DiskCacheWriter<String>(tempLocation, filename
					+ ".tmp");
		} catch (Exception e) {
			SDIConstants.SDILogger.error( ALERT_MESSAGE_PREFIX
					+ ":New DiskCacheWriter failed", e);
			;
		}

		Thread[] ThreadArray = new Thread[this.ThreadNum];
		for (int i = 0; i < this.ThreadNum; i++) {
			ThreadArray[i] = new Thread(new Worker());
		}

		for (int i = 0; i < this.ThreadNum; i++) {
			ThreadArray[i].start();
		}

	}

	private class Worker implements Runnable {

		public Worker() {
			synchronized (AlertingGenerator.this) {
				AlertingGenerator.this.ActiveNum++;
			}
		}

		@Override
		public void run() {

			try {

				while (true) {

					HashMap<CursorType, SDICursorRow[]> SCR = AlertingGenerator.this.InQ
							.getObj();

					if (SCR != null) {
						
						Document doc =DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

						String Alerting_message = null;

						// build message here

						String Alerting_message_header = getHeader()
								+ getCPA(doc,SCR) + getVessel(doc,SCR) + getIdt(doc,SCR);

						String destChange = getDestinationChg(doc,SCR);

						if (destChange != null && destChange != ""
								&& destChange.indexOf("upd:VesselAsset") > 0) {
							Alerting_message = Alerting_message_header
									+ destChange + getFooter();
							// producer.Send(Alerting_message);
							diskWriter.Append(Alerting_message);
							SDIConstants.SDILogger.debug( Alerting_message);
							// System.out.println(" Destination change alert sent.");
							// System.out.println(Alerting_message);
						}
						String etaChange = getETAChg(doc,SCR);
						if (etaChange != null && etaChange != ""
								&& etaChange.indexOf("upd:VesselAsset") > 0) {
							Alerting_message = Alerting_message_header
									+ etaChange + getFooter();
							// producer.Send(Alerting_message);
							diskWriter.Append(Alerting_message);
							SDIConstants.SDILogger.debug( Alerting_message);
							// System.out.println(sysdate+" ETA change alert sent.");
							// System.out.println(Alerting_message);
						}
						String statusChange = getStatusChg(doc,SCR);
						if (statusChange != null
								&& statusChange != ""
								&& statusChange
								.indexOf("upd:CommodityPhysicalAsset") > 0) {
							Alerting_message = Alerting_message_header
									+ statusChange + getFooter();
							// producer.Send(Alerting_message);
							diskWriter.Append(Alerting_message);
							SDIConstants.SDILogger.debug( Alerting_message);
							// System.out.println(sysdate+" Status change alert sent.");
							// System.out.println(Alerting_message);
						}
						getPortChg(doc,SCR);
						//						if (portChange != null && portChange != ""
						//								&& portChange.indexOf("upd:VesselLocation") > 0) {
						//							Alerting_message = Alerting_message_header
						//									+ portChange + getFooter();
						//							// producer.Send(Alerting_message);
						//							diskWriter.Append(Alerting_message);
						//							SDIConstants.SDILogger.debug( Alerting_message);
						//							// System.out.println(sysdate+" Port change alert sent.");
						//							// System.out.println(Alerting_message);
						//						}
						// }
					} else {
						break;
					}

				}

			} catch (Exception e) {
				SDIConstants.SDILogger.error( ALERT_MESSAGE_PREFIX
						+ ":Alert generator exception", e);
			} finally {
				synchronized (AlertingGenerator.this) {
					AlertingGenerator.this.ActiveNum--;

					if (AlertingGenerator.this.ActiveNum == 0) {
						Connection DBConn = null;
						try {
							diskWriter.Finish();
							// String BaseFolder = System.getenv("FEEDSOUT");
							// SDIConstants.SDILogger.debug("Base Folder: " +
							// BaseFolder);
							File tempFile = new File(tempLocation, filename
									+ ".tmp");
							File thisFile = new File(filelocation, filename);
							MoveFile(tempFile, thisFile);
							DBConn = new EasyConnection(DBConnNames.CEF_CNR);
							// producer=new TRMessageProducer("Vessel_Update1");
							ArrayList<Object> parmVector = new ArrayList<Object>();

							String[] parmsArray = {
									"start",
									DateFun.getStringDate(AlertingGenerator.this.CoverageEnd) };
							
							parmVector.add(DBConn.createStruct("CEF_CNR.CE_VAR_NV_TYPE", parmsArray));

							parmsArray[0] = "file";
							parmsArray[1] = filename;
							
							parmVector.add(DBConn.createStruct("CEF_CNR.CE_VAR_NV_TYPE", parmsArray));

							Object obj_array[] = parmVector.toArray();
							
							OracleConnection OConn=DBConn.unwrap(OracleConnection.class);
							
							java.sql.Array array = OConn.createOracleArray("CEF_CNR.CE_VAR_NV_LST_T", obj_array);

							CallableStatement InsTaskCs = DBConn
									.prepareCall("{call cef_cnr.TASK_MAINTAIN_PKG.insert_task_queue_proc(?,?)}");

							InsTaskCs.setString(1, "ALERT_MESSAGE");
							InsTaskCs.setArray(2, array);
							InsTaskCs.execute();
							DBConn.commit();
							InsTaskCs.close();
							SDIConstants.SDILogger.info(
									ALERT_MESSAGE_PREFIX
									+ ": alert has been generated with parm="
									+ " start time="
									+DateFun.getStringDate(AlertingGenerator.this.CoverageEnd)+", file="+filename);
						} catch (Exception e) {// This means Vessel_Update1 is
							// removed from config file.
							SDIConstants.SDILogger.error(
									ALERT_MESSAGE_PREFIX
									+ ":insert task error.", e);
						} finally {
							try {
								DBConn.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								SDIConstants.SDILogger.error(
										ALERT_MESSAGE_PREFIX
										+ ":close DB connection error.",
										e);
							}
						}
						AlertingGenerator.this.count.Decrease();

					}

				}
			}

		}
	}

	private String getFooter() {
		// publisher.dispose();
		String footer = "		</env:Body>\r\n" + "</env:ContentEnvelope>";
		return footer;
	}

	private String getHeader() {
		String envolope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" xmlns=\"http://CommodityPhysicalAssets.schemas.financial.thomsonreuters.com/2010-11-17/\" xmlns:upd=\"http://CommodityPhysicalAssets.schemas.financial.thomsonreuters.com/2010-11-17/Update/\" xmlns:id=\"http://data.schemas.financial.thomsonreuters.com/metadata/2010-10-10/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://CommodityPhysicalAssets.schemas.financial.thomsonreuters.com/2010-11-17/ @XsdType@\"  pubStyle=\"@PubType@\" majVers=\"3\" minVers=\"1.0\">\r\n";

		String header = envolope
				+ "	<env:Header>\r\n"
				+ "		<env:Info>\r\n"
				+ "			<env:Id>urn:uuid:@GUID@</env:Id>\r\n"
				+ "			<env:TimeStamp>@TIMESTAMP@</env:TimeStamp>\r\n"
				+ "		</env:Info>\r\n"
				+ "</env:Header>\r\n"
				+ "<env:Body contentSet=\"@assetType@\" majVers=\"2\" minVers=\"1.0\">\r\n";

		String uuid = UUID.randomUUID().toString();
		header = header.replace("@GUID@", uuid);
		Date XMLTimestamp = new Date();// yyyy-MM-dd HH:mm:ss.SSS
		DateFormat dataFormat = new SimpleDateFormat(
				SDIConstants.DATE_FORMAT_SSS, Locale.getDefault());

		String strDate = dataFormat.format(XMLTimestamp);
		header = header.replace("@TIMESTAMP@", strDate);
		header = header.replace("@PubType@", "Message");
		header = header.replace("@XsdType@", "CommodityPhysicalAssetAlert.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

	private Element getAstBase(Document doc,HashMap<CursorType, SDICursorRow[]> SCR,
			boolean isUpd) {

		SDICursorRow[] lstCurRows = SCR.get(CursorType.AST_BASE_INFO);

		if (lstCurRows != null && lstCurRows[0] != null) {
			AstBase baseRow = (AstBase) lstCurRows[0];

			String objType = baseRow.getCne_asset_id().getObject_type();
			Long objTypeId = baseRow.getCne_asset_id().getObject_type_id();
			Long objId = baseRow.getCne_asset_id().getObject_id();
			Element elAssetId = null;
			if (isUpd)
				elAssetId = doc.createElement("upd:AssetID");
			else
				elAssetId = doc.createElement("AssetID");

			elAssetId.setAttribute("objectType", objType);
			elAssetId.setAttribute("objectTypeId", objTypeId.toString());
			elAssetId.appendChild(doc.createTextNode(objId.toString()));
			return elAssetId;

		}
		return null;
	}

	private String getCPA(Document doc,HashMap<CursorType, SDICursorRow[]> SCR) {

		String content = "";
		try {
			// Document doc =
			// Utility.getSafedocumentbuilder().get().newDocument();
			doc.setStrictErrorChecking(false);
			Element elContentItem = doc.createElement("env:ContentItem");
			doc.appendChild(elContentItem);
			Element elData = doc.createElement("env:Data");
			elData.setAttribute("type", "CommodityPhysicalAsset");
			elContentItem.appendChild(elData);

			Element elCPA = doc.createElement("CommodityPhysicalAsset");
			elData.appendChild(elCPA);

			SDICursorRow[] lstCurRows = SCR.get(CursorType.AST_BASE_INFO);

			Date effectiveFrom = null;
			Element assetType = null;
			if (lstCurRows != null && lstCurRows[0] != null) {
				AstBase baseRow = (AstBase) lstCurRows[0];
				effectiveFrom = baseRow.getEffective_from();
				Element elAstBase = getAstBase(doc,SCR, false);
				elCPA.appendChild(elAstBase);
				assetType = doc.createElement("AssetType");
				DateFormat formatter = new SimpleDateFormat(
						SDIConstants.DATE_FORMAT_SSS);
				if (effectiveFrom != null) {
					String effectFromStr = formatter.format(effectiveFrom);
					assetType.setAttribute("effectiveFrom", effectFromStr);
				}
				Element assetTypeId = doc.createElement("AssetTypeID");
				Element assetTypeRcs = doc.createElement("AssetTypeRCS");
				Element assetTypeValue = doc.createElement("AssetType");
				assetType.appendChild(assetTypeId);
				assetType.appendChild(assetTypeRcs);
				assetType.appendChild(assetTypeValue);
				Date astTypeIdEffectFrom = baseRow.getAsset_type_id()
						.getEffective_from();
				String astTypeIdEffectFromStr = formatter
						.format(astTypeIdEffectFrom);
				assetTypeId.setAttribute("effectiveFrom",
						astTypeIdEffectFromStr);
				assetTypeId.setAttribute("relatedObjectId", baseRow
						.getAsset_type_id().getRelated_object_id().toString());
				assetTypeId.setAttribute("relatedObjectTypeId", baseRow
						.getAsset_type_id().getRelated_object_type_id()
						.toString());
				assetTypeId.setAttribute("relatedObjectType", baseRow
						.getAsset_type_id().getRelated_object_type());
				assetTypeId.setAttribute("relationObjectTypeId", baseRow
						.getAsset_type_id().getRelation_object_type_id()
						.toString());
				assetTypeId.setAttribute("relationObjectType", baseRow
						.getAsset_type_id().getRelation_object_type());
				assetTypeId.setAttribute("relationshipTypeId", baseRow
						.getAsset_type_id().getRelationship_type_id()
						.toString());
				assetTypeId.setAttribute("relationshipType", baseRow
						.getAsset_type_id().getRelationship_type());
				Long objId = baseRow.getAsset_type_id().getObject_id();
				String astId = null;
				if (objId != null)
					astId = objId.toString();
				assetTypeId.appendChild(doc.createTextNode(astId));
				assetTypeRcs.appendChild(doc.createTextNode(baseRow
						.getAsset_type_rcs()));
				assetTypeValue.appendChild(doc.createTextNode(baseRow
						.getAsset_type_value()));
				assetTypeValue.setAttribute("languageId", "505062");
				elContentItem.setAttribute("action", "Overwrite");
			}

			lstCurRows = SCR.get(CursorType.AST_NAME_INFO);

			if (lstCurRows != null && lstCurRows[0] != null) {
				for (SDICursorRow astName : lstCurRows) {
					AstName astNameRow = (AstName) astName;
					// AstName nameRow = (AstName) lstCurRows[0];
					String nameType = astNameRow.getName_type();
					if (nameType != null && nameType != ""
							&& nameType.equals("FULL NAME")) {
						Element assetFullName = doc
								.createElement("AssetFullName");
						Date nameEffectiveFrom = astNameRow.getEffective_from();
						if (nameEffectiveFrom != null) {
							DateFormat formatter = new SimpleDateFormat(
									SDIConstants.DATE_FORMAT_SSS);
							String effectFromStr = formatter
									.format(nameEffectiveFrom);
							assetFullName.setAttribute("effectiveFrom",
									effectFromStr);
						}
						assetFullName.setAttribute("languageId", "505062");

						String fullName = astNameRow.getName_value();
						assetFullName.appendChild(doc.createTextNode(fullName));
						elCPA.appendChild(assetFullName);
						// util.processCurRow(AstName.class, lstCurRows, doc,
						// elCPA, "AssetName");
					}
				}
			}
			elCPA.appendChild(assetType);
			content = getElementAsXml(elContentItem);
		} catch (DOMException e) {
			SDIConstants.SDILogger.error( "DOMException", e);
		} catch (SecurityException e) {
			SDIConstants.SDILogger.error( "SecurityException", e);
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "Exception", e);
		}

		return content;
	}

	private String getVessel(Document doc,HashMap<CursorType, SDICursorRow[]> SCR) {
		try {
			Element elContentItem = doc.createElement("env:ContentItem");
			doc.appendChild(elContentItem);

			Element elData = doc.createElement("env:Data");
			elData.setAttribute("type", "VesselAsset");
			elContentItem.appendChild(elData);
			// elData.setAttribute("type", "PlantAsset");

			Element elVA = doc.createElement("VesselAsset");
			elData.appendChild(elVA);

			// ///////////////////////////////////////////

			// Get PhysicalAsset Base info: for caculating action
			SDICursorRow[] lstCurRows = SCR.get(CursorType.AST_BASE_INFO);

			if (lstCurRows != null && lstCurRows[0] != null) {
				// AstBase baseRow = (AstBase) lstCurRows[0];

				Element elAssetId = getAstBase(doc,SCR, false);
				elVA.appendChild(elAssetId);
				elContentItem.setAttribute("action", "Overwrite");
			}

			lstCurRows = SCR.get(CursorType.VESSEL_BASE_INFO);
			if (lstCurRows != null && lstCurRows.length > 0) {
				VesselBase vesBaseRow = (VesselBase) lstCurRows[0];
				Float dwt = vesBaseRow.getDwt();
				Integer dwtInt = dwt.intValue();
				Element elDwt = doc.createElement("VesselDWT");
				elDwt.appendChild(doc.createTextNode(dwtInt.toString()));
				elVA.appendChild(elDwt);
			}

			String content = getElementAsXml(elContentItem);

			return content;
		} catch (DOMException e) {
			SDIConstants.SDILogger.error( "DOMException", e);
		} catch (SecurityException e) {
			SDIConstants.SDILogger.error( "SecurityException", e);
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "Exception", e);
		}

		return "";
	}

	private String getIdt(Document doc,HashMap<CursorType, SDICursorRow[]> SCR) {

		String content = "";

		try {

			SDICursorRow[] lstCurRows = SCR
					.get(CursorType.AST_IDENTIFIER_VALUE_INFO);

			if (lstCurRows != null && lstCurRows.length > 0) {
				for (SDICursorRow iden : lstCurRows) {
					AstIdentifierValue identifierRow = (AstIdentifierValue) iden;
					Element elContentItem, elData, elIdentifier;
					elContentItem = doc.createElement("env:ContentItem");
					elContentItem.setAttribute("action", "Overwrite");

					doc.appendChild(elContentItem);
					elData = doc.createElement("env:Data");
					elData.setAttribute("type", "IdentifierItem");
					elContentItem.appendChild(elData);
					elIdentifier = doc.createElement("id:Identifier");
					elData.appendChild(elIdentifier);
					Element elIdentifierValue = doc
							.createElement("id:IdentifierValue");
					elIdentifier.appendChild(elIdentifierValue);
					Date effectiveFrom = identifierRow.getIdentifierValue()
							.getEffective_from();
					if (effectiveFrom != null) {
						DateFormat formatter = new SimpleDateFormat(
								SDIConstants.DATE_FORMAT_SSS);
						String dataValue = formatter.format(effectiveFrom);
						elIdentifierValue.setAttribute("effectiveFrom",
								dataValue);
					}

					String effectiveToStr = identifierRow.getIdentifierValue()
							.getEffective_to_na_code();
					Long entityId = identifierRow.getIdentifierValue()
							.getIdentifier_entity_id();
					Long entityTypeId = identifierRow.getIdentifierValue()
							.getIdentifier_entity_type_id();
					Long typeId = identifierRow.getIdentifierValue()
							.getIdentifier_type_id();
					String typeCode = identifierRow.getIdentifierValue()
							.getIdentifier_type_code();
					String value = identifierRow.getIdentifierValue()
							.getIdentifier_value();
					elIdentifierValue.setAttribute("effectiveToNACode",
							effectiveToStr);
					elIdentifierValue.setAttribute("identifierEntityId",
							entityId.toString());
					elIdentifierValue.setAttribute("identifierEntityTypeId",
							entityTypeId.toString());
					elIdentifierValue.setAttribute("identifierTypeId",
							typeId.toString());
					elIdentifierValue.setAttribute("identifierTypeCode",
							typeCode);
					elIdentifierValue.appendChild(doc.createTextNode(value));

					content = content + getElementAsXml(elContentItem);

				}

			}
		} catch (Exception e) {
			SDIConstants.SDILogger.error(
					"Alerting Identifier message generate failed.");
			e.printStackTrace();
		}

		return content;

	}

	private String getDestinationChg(Document doc,HashMap<CursorType, SDICursorRow[]> SCR) {
		try {
			String content = "";
			String portRic = "";
			SDICursorRow[] lstLocRows = SCR
					.get(CursorType.VESSEL_EVENT_FACT_DETAIL_INFO);
			VesselEventFact eventFact;
			Element elContentItem = null;

			if (lstLocRows != null && lstLocRows.length > 0) {
				elContentItem = doc.createElement("env:ContentItem");
				elContentItem.setAttribute("action", "Update");
				// String changeFlag = getChangeFlag(lstLocRows, "EventFact");
				Hashtable<String, String> zoneChangeFlagHt = getZoneTypeChangeFlag(
						lstLocRows, "EventFact");
				for (SDICursorRow loc : lstLocRows) {
					eventFact = (VesselEventFact) loc;
					String FactType = eventFact.getZone_type();
					String zoneRole = eventFact.getZone_role();

					String isLatest = eventFact.getIs_latst();
					Date actionTime = eventFact.getAction_time();
					// if (changeFlag != null && !changeFlag.equals("Delete")
					String changeFlag = zoneChangeFlagHt.get("DESTINATION");
					if (zoneRole.toUpperCase().equals("DESTINATION")
							&& changeFlag != null
							&& changeFlag.equals("Update")&&(isLatest!=null&&isLatest.equals("Y")&&actionTime.after(this.CoverageStart)
									&& actionTime.before(this.CoverageEnd)||isLatest!=null&&isLatest.equals("N"))) {
						portRic = portRic + getPortRic(doc,eventFact);
						doc.appendChild(elContentItem);
						Element elData = null;
						if (isLatest.equals("Y"))
							elData = doc.createElement("env:Data");
						else
							elData = doc.createElement("env:PreviousData");
						elData.setAttribute("type", "VesselAssetUpdate");
						elContentItem.appendChild(elData);

						Element elVA = doc.createElement("upd:VesselAsset");
						elData.appendChild(elVA);
						Element elAssetId = getAstBase(doc,SCR, true);
						elVA.appendChild(elAssetId);
						Element elLocation = doc
								.createElement("upd:VesselLocation");

						Element elLocRole = doc
								.createElement("upd:VesselLocationRole");
						elLocRole
						.appendChild(doc.createTextNode("Destination"));
						elLocation.appendChild(elLocRole);
						Element elVLZ = doc
								.createElement("upd:VesselLocationZone");
						elLocation.appendChild(elVLZ);
						elVA.appendChild(elLocation);

						// if(changeFlag.equals("Delete"))
						// elVLZ.setAttribute("changeFlag", changeFlag);
						// else if(isLatest.equals("Y"))
						if (isLatest.equals("Y"))
							elVLZ.setAttribute("changeFlag", changeFlag);

						Field perm_id_field = VesselEventFact.class
								.getDeclaredField("zone_perm_id");
						// Field zone_id_field =
						// VesselEventFact.class.getDeclaredField("zone_identifier");
						Field name_field = VesselEventFact.class
								.getDeclaredField("zone_name");
						Field type_field = VesselEventFact.class
								.getDeclaredField("zone_type");
						Field entry_field = VesselEventFact.class
								.getDeclaredField("entry_time");

						Field[] fields = new Field[] { perm_id_field,
								name_field, type_field, entry_field };
						util.generateSection(eventFact, fields, doc, elVLZ);

					}
				}
				content = portRic + getElementAsXml(elContentItem);
			}
			return content;
		} catch (Exception e) {
			SDIConstants.SDILogger.error(
					"Destination change alert exception", e);
			return null;
		}

	}

	private String getETAChg(Document doc,HashMap<CursorType, SDICursorRow[]> SCR) {
		try {
			String content = "";

			SDICursorRow[] lstLocRows = SCR
					.get(CursorType.VESSEL_LOCATION_FACT_DETAIL_INFO);
			VesselLocationFact LocFact;

			Element elContentItem = null;

			if (lstLocRows != null && lstLocRows.length > 0) {
				elContentItem = doc.createElement("env:ContentItem");
				elContentItem.setAttribute("action", "Update");
				String changeFlag = getChangeFlag(lstLocRows, "LocationFact","DESTINATION_ETA");
				for (SDICursorRow loc : lstLocRows) {
					LocFact = (VesselLocationFact) loc;
					String FactType = LocFact.getFact_type();
					String FactValue = LocFact.getFact_value();
					String isLatest = LocFact.getIs_latest();
					Date actionTime = LocFact.getAction_time();
					if (changeFlag != null && changeFlag.equals("Update")) {
						if (FactType.equals("DESTINATION_ETA")&&(isLatest!=null&&isLatest.equals("Y")&&actionTime.after(this.CoverageStart)
								&& actionTime.before(this.CoverageEnd)||isLatest!=null&&isLatest.equals("N"))) {
							doc.appendChild(elContentItem);
							Element elData = null;
							if (isLatest.equals("Y"))
								elData = doc.createElement("env:Data");
							else
								elData = doc.createElement("env:PreviousData");
							elData.setAttribute("type", "VesselAssetUpdate");
							elContentItem.appendChild(elData);
							// elData.setAttribute("type", "PlantAsset");

							Element elVA = doc.createElement("upd:VesselAsset");
							elData.appendChild(elVA);
							Element elAssetId = getAstBase(doc,SCR, true);
							elVA.appendChild(elAssetId);
							Element elLocation = doc
									.createElement("upd:VesselLocation");
							elVA.appendChild(elLocation);
							Element elLocRole = doc
									.createElement("upd:VesselLocationRole");
							elLocRole.appendChild(doc
									.createTextNode("Destination"));
							elLocation.appendChild(elLocRole);

							Date etaDate = strToDate(FactValue);
							if (etaDate != null) {
								DateFormat formatter = new SimpleDateFormat(
										SDIConstants.DATE_FORMAT_SSS);
								String eta = formatter.format(etaDate);

								Element elETA = doc
										.createElement("upd:VesselLocationETA");
								elETA.appendChild(doc.createTextNode(eta));

								//								if (isLatest.equals("N"))
								//									elETA.setAttribute("changeFlag", "Delete");
								//								else if (isLatest.equals("Y"))
								if (isLatest.equals("Y"))
									elETA.setAttribute("changeFlag", "Update");

								elLocation.appendChild(elETA);

							}

						}

					}
				}

				content = getElementAsXml(elContentItem);
			}

			return content;
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "ETA change alert exception", e);
			return null;
		}
	}

	private String getStatusChg(Document doc,HashMap<CursorType, SDICursorRow[]> SCR) {
		try {
			String content = "";
			SDICursorRow[] lstLocRows = SCR
					.get(CursorType.VESSEL_LOCATION_FACT_DETAIL_INFO);
			VesselLocationFact LocFact;

			Element elContentItem = null;

			if (lstLocRows != null && lstLocRows.length > 0) {
				elContentItem = doc.createElement("env:ContentItem");
				elContentItem.setAttribute("action", "Update");

				String changeFlag = getChangeFlag(lstLocRows, "LocationFact","Vessel Location Status");
				for (SDICursorRow loc : lstLocRows) {
					LocFact = (VesselLocationFact) loc;
					String FactType = LocFact.getFact_type();
					String FactValue = LocFact.getFact_value();
					String isLatest = LocFact.getIs_latest();
					Date actionTime = LocFact.getAction_time();

					if (changeFlag != null && changeFlag.equals("Update")) {
						if (FactType.equals("Vessel Location Status")&& (isLatest!=null&&isLatest.equals("Y")&&actionTime.after(this.CoverageStart)
								&& actionTime.before(this.CoverageEnd)||isLatest!=null&&isLatest.equals("N"))) {
							doc.appendChild(elContentItem);
							Element elData = null;
							if (isLatest.equals("Y"))
								elData = doc.createElement("env:Data");
							else
								elData = doc.createElement("env:PreviousData");
							elData.setAttribute("type",
									"CommodityPhysicalAssetUpdate");
							elContentItem.appendChild(elData);

							Element elCPA = doc
									.createElement("upd:CommodityPhysicalAsset");
							elData.appendChild(elCPA);
							Element elAssetId = getAstBase(doc,SCR, true);
							elCPA.appendChild(elAssetId);
							Element elAstType = doc
									.createElement("upd:AssetType");
							elCPA.appendChild(elAstType);
							Element elAstStatus = doc
									.createElement("upd:AssetStatus");

							Date effectiveFrom = null;
							SDICursorRow[] lstCurRows = SCR
									.get(CursorType.AST_BASE_INFO);
							if (lstCurRows != null && lstCurRows[0] != null) {
								AstBase baseRow = (AstBase) lstCurRows[0];
								effectiveFrom = baseRow.getEffective_from();
							}
							if (effectiveFrom != null) {
								DateFormat formatter = new SimpleDateFormat(
										SDIConstants.DATE_FORMAT_SSS);
								String dataValue = formatter
										.format(effectiveFrom);
								elAstType.setAttribute("effectiveFrom",
										dataValue);
							}

							String vesStatus = FactValue;
							if (vesStatus != null) {
								Element elAstStatusType = doc
										.createElement("upd:AssetStatusType");
								Element elAssetStatusTypeValue = doc
										.createElement("upd:AssetStatusTypeValue");
								elAstStatus.appendChild(elAstStatusType);
								elAstStatus.appendChild(elAssetStatusTypeValue);

								elAstStatusType
								.appendChild(doc
										.createTextNode("Vessel Location Status"));
								elAssetStatusTypeValue.appendChild(doc
										.createTextNode(vesStatus));
								// elAssetStatusTypeValue.setAttribute("changeFlag",
								// "Update");
								// if (isLatest.equals("N"))
								// elAssetStatusTypeValue.setAttribute(
								// "changeFlag", "Delete");
								// else if (isLatest.equals("Y"))
								if (isLatest.equals("Y"))
									elAssetStatusTypeValue.setAttribute(
											"changeFlag", "Update");

								elAstType.appendChild(elAstStatus);
							}

						}
					}
				}
				content = getElementAsXml(elContentItem);
			}

			return content;
		} catch (Exception e) {
			SDIConstants.SDILogger.error(
					"Location Status change alert exception", e);
			return null;
		}

	}

	private String getPortChg(Document doc,final HashMap<CursorType, SDICursorRow[]> SCR) {
		try {
			String content = "";
			String portRic = "";

			SDICursorRow[] lstEventRows = SCR
					.get(CursorType.VESSEL_EVENT_FACT_DETAIL_INFO);			 

			if (lstEventRows != null && lstEventRows.length > 0) {

				// String changeFlag = getChangeFlag(lstEventRows, "EventFact");
				for (SDICursorRow event : lstEventRows) {

					// Boolean hasPortChange = false;
					//					Hashtable<String, String> zoneChangeFlagHt = getZoneTypeChangeFlag(
					//							lstEventRows, "EventFact");

					VesselEventFact EventFact = (VesselEventFact) event;
					String zoneType = EventFact.getZone_type();
					String zoneRole = EventFact.getZone_role();
					String isLatest = EventFact.getIs_latst();
					Date actionTime = EventFact.getAction_time();
					//					String changeFlag = zoneChangeFlagHt.get(zoneType);
					// System.out.println("Change flag:"+changeFlag+" "+EventFact.getZone_perm_id()+" "+EventFact.getPerm_ID()+" "+EventFact.getZone_name()+" "+zoneType+" "+zoneRole+" "+isLatest+" "+actionTime+" ");

					// if ( changeFlag != null && !changeFlag.equals("Delete")
					if (zoneRole.toUpperCase().equals("CURRENT") && actionTime.after(this.CoverageStart)
							&& actionTime.before(this.CoverageEnd)) {
						portRic = getPortRic(doc,EventFact);

						Element elContentItem = doc.createElement("env:ContentItem");
						elContentItem.setAttribute("action", "Update");
						doc.appendChild(elContentItem);
						Element elData = doc.createElement("env:Data");
						elContentItem.appendChild(elData);
						Element elVA = doc.createElement("upd:VesselAsset");
						elData.appendChild(elVA);
						elData.setAttribute("type", "VesselAssetUpdate");
						Element elAssetId = getAstBase(doc,SCR, true);
						elVA.appendChild(elAssetId);						

						Element elLocation = doc
								.createElement("upd:VesselLocation");
						Element elLocRole = doc
								.createElement("upd:VesselLocationRole");
						elLocRole.appendChild(doc.createTextNode(zoneRole));
						elLocation.appendChild(elLocRole);
						Element elVLZ = doc
								.createElement("upd:VesselLocationZone");
						elLocation.appendChild(elVLZ);
						elVA.appendChild(elLocation);
						// elVLZ.setAttribute("changeFlag", changeFlag);
						if (isLatest != null && isLatest != ""
								&& isLatest.equals("Y"))
							elVLZ.setAttribute("changeFlag", "Insert");
						else if (isLatest != null && isLatest != ""
								&& isLatest.equals("N"))
							elVLZ.setAttribute("changeFlag", "Delete");
						Field perm_id_field = VesselEventFact.class
								.getDeclaredField("zone_perm_id");
						// Field zone_id_field =
						// VesselEventFact.class.getDeclaredField("zone_identifier");
						Field name_field = VesselEventFact.class
								.getDeclaredField("zone_name");
						Field type_field = VesselEventFact.class
								.getDeclaredField("zone_type");
						Field entry_field = VesselEventFact.class
								.getDeclaredField("entry_time");
						Field out_field = VesselEventFact.class
								.getDeclaredField("out_time");

						if (isLatest != null && isLatest != ""
								&& isLatest.equals("Y")) {
							Field[] fields = new Field[] { perm_id_field,
									name_field, type_field, entry_field };
							util.generateSection(event, fields, doc, elVLZ);
						} else if (isLatest != null && isLatest != ""
								&& isLatest.equals("N")) {
							Field[] fields = new Field[] { perm_id_field,
									name_field, type_field, entry_field,
									out_field };
							util.generateSection(event, fields, doc, elVLZ);
						}
						String header = getHeader()
								+ getCPA(doc, SCR) + getVessel(doc,SCR) + getIdt(doc,SCR);
						String message = header+portRic+getElementAsXml(elContentItem)+getFooter();
						diskWriter.Append(message);
						SDIConstants.SDILogger.debug( message);

					}

				}
				//				content = portRic + getElementAsXml(elContentItem);

				return "Send port change successfully.";

			}
		} catch (DOMException e) {
			SDIConstants.SDILogger.error( "DOMException", e);
		} catch (SecurityException e) {
			SDIConstants.SDILogger.error( "SecurityException", e);
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "Exception", e);
		}

		return null;
	}

	private String getPortRic(Document doc,VesselEventFact event) {
		try {
			String content = "";
			Element elContentItem, elData, elIdentifier;
			elContentItem = doc.createElement("env:ContentItem");
			elContentItem.setAttribute("action", "Overwrite");
			// elContentItem.setAttribute("action", action);
			doc.appendChild(elContentItem);
			elData = doc.createElement("env:Data");
			elData.setAttribute("type", "IdentifierItem");
			elContentItem.appendChild(elData);
			elIdentifier = doc.createElement("id:Identifier");
			elData.appendChild(elIdentifier);
			Element elIdentifierValue = doc.createElement("id:IdentifierValue");
			elIdentifier.appendChild(elIdentifierValue);
			// Field port_ric_field =
			// VesselEventFact.class.getDeclaredField("zone_identifier");
			IDENTIFIER_VALUE_TYPE zone_idt = null;
			try {
				zone_idt = event.getZone_identifier();
			} catch (Exception e) {
				return "";
			}
			if (zone_idt != null) {
				Date effectiveFrom = zone_idt.getEffective_from();
				Date effectiveTo = zone_idt.getEffective_to();
				String effectiveFromStr = null;
				String effectiveToStr = null;
				if (effectiveFrom != null) {
					DateFormat formatter = new SimpleDateFormat(
							SDIConstants.DATE_FORMAT_SSS);
					effectiveFromStr = formatter.format(effectiveFrom);
				}
				if (effectiveTo != null) {
					DateFormat formatter = new SimpleDateFormat(
							SDIConstants.DATE_FORMAT_SSS);
					effectiveToStr = formatter.format(effectiveTo);
				}

				String effectiveToNAStr = zone_idt.getEffective_to_na_code();
				Long entityId = zone_idt.getIdentifier_entity_id();
				Long entityTypeId = zone_idt.getIdentifier_entity_type_id();
				Long typeId = zone_idt.getIdentifier_type_id();
				String typeCode = zone_idt.getIdentifier_type_code();
				String value = zone_idt.getIdentifier_value();

				elIdentifierValue.setAttribute("effectiveFrom",
						effectiveFromStr);
				elIdentifierValue.setAttribute("effectiveTo", effectiveToStr);
				elIdentifierValue.setAttribute("effectiveToNACode",
						effectiveToNAStr);
				elIdentifierValue.setAttribute("identifierEntityId",
						entityId.toString());
				elIdentifierValue.setAttribute("identifierEntityTypeId",
						entityTypeId.toString());
				elIdentifierValue.setAttribute("identifierTypeCode", typeCode);
				elIdentifierValue.setAttribute("identifierTypeId",
						typeId.toString());
				elIdentifierValue.appendChild(doc.createTextNode(value));

				// Field[] port_ric_fields = new Field[] {port_ric_field};
				// util.generateSection(event, port_ric_fields, doc,
				// elIdentifier);
				content = getElementAsXml(elContentItem);
				if (value == null || value == "")
					return "";
			}
			return content;
		} catch (DOMException e) {
			SDIConstants.SDILogger.error( "DOMException", e);
		} catch (SecurityException e) {
			SDIConstants.SDILogger.error( "SecurityException", e);
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "Exception", e);
		}
		return "";
	}

	private String getElementAsXml(Element ele) {
		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer;
		String resultStr = "";
		try {
			transformer = tFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			// transformer.setOutputProperty(OutputKeys.ENCODING,"ISO-8859-1");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			// we want to pretty format the XML output
			// note : this is broken in jdk1.5 beta!
			transformer.setOutputProperty(
					"{http://xml.apache.org/xslt}indent-amount", "4");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");

			StringWriter sw = new StringWriter();
			StreamResult result = new StreamResult(sw);
			DOMSource source = null;
			source = new DOMSource(ele);
			transformer.transform(source, result);
			resultStr = sw.toString();
			resultStr = resultStr.replaceAll("env:Data type",
					"env:Data xsi:type");
			resultStr = resultStr.replaceAll("env:PreviousData type",
					"env:PreviousData xsi:type");
			resultStr = resultStr.replaceAll("zone_identifier",
					"id:IdentifierValue");
			resultStr = resultStr.replaceAll("effective_from", "effectiveFrom");
			resultStr = resultStr.replaceAll("related_object_type_id",
					"relatedObjectTypeId");
			resultStr = resultStr.replaceAll("relation_object_type_id",
					"relationObjectTypeId");
			resultStr = resultStr.replaceAll("relationship_type_id",
					"relationshipTypeId");
			resultStr = resultStr.replaceAll("related_object_id",
					"relatedObjectId");
			resultStr = resultStr.replaceAll("related_object_type",
					"relatedObjectType");
			resultStr = resultStr.replaceAll("relation_object_type",
					"relationObjectType");
			resultStr = resultStr.replaceAll("relationship_type",
					"relationshipType");

			resultStr = resultStr.replaceAll("serialVersionUID=\"1\"", "");
			resultStr = resultStr.replaceAll("zone_perm_id", "upd:VLZAssetID");
			resultStr = resultStr.replaceAll("zone_name", "upd:VLZName");
			resultStr = resultStr.replaceAll("zone_type", "upd:VLZType");
			resultStr = resultStr.replaceAll("entry_time",
					"upd:VLZEntryTimestamp");
			resultStr = resultStr.replaceAll("out_time", "upd:VLZOutTimestamp");

			// System.out.println(sw.toString());
			return resultStr;
		} catch (Exception e) {
			SDIConstants.SDILogger.warn(
					"Failed to transfor XML element to String : " + ele + "!");
			return "   ";
		}
	}

	private Date strToDate(String strDate) {
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddhhmmss");
			ParsePosition pos = new ParsePosition(0);
			Date d = new Date(formatter.parse(strDate, pos).getTime());
			return d;
		} catch (Exception e) {
			// this.LogDetails(MsgCategory.ERROR, "strToDate failed:" + strDate
			// + " is not in format: MM/dd/yyyy"+e.getMessage());
			SDIConstants.SDILogger.error( "strToDate failed:" + strDate
					+ " is not in format: MM/dd/yyyy", e);
			return null;
		}
	}
 

	private String getChangeFlag(SDICursorRow[] lstLocRows, String cursor,String facttype) {

		Boolean hasY = false;
		Boolean hasN = false;
		VesselLocationFact LocFact;
		VesselEventFact eventFact;
		String isLatest = "";
		Date actionTime = null;
		for (SDICursorRow row : lstLocRows) {
			if (cursor.equals("LocationFact")) {
				LocFact = (VesselLocationFact) row;
				String FactType = LocFact.getFact_type();
				if(FactType.equals(facttype)){
					isLatest = LocFact.getIs_latest();
					actionTime = LocFact.getAction_time();
				}
			} else if (cursor.equals("EventFact")) {
				eventFact = (VesselEventFact) row;				 
				isLatest = eventFact.getIs_latst();
				actionTime = eventFact.getAction_time();
			}

			if (isLatest != null && isLatest != "" && isLatest.equals("Y")
					&& actionTime.after(this.CoverageStart)
					&& actionTime.before(this.CoverageEnd))
				hasY = true;
			else if (isLatest != null && isLatest != "" && isLatest.equals("N"))
				hasN = true;
		}

		// if(hasY) System.out.println("hasY");
		// if(hasN) System.out.println("hasN");

		if (hasY && hasN)
			return "Update";
		if (hasY && hasN == false)
			return "Insert";
		if (hasY == false && hasN)
			return "Delete";

		return null;

	}

	private Hashtable<String, String> getZoneTypeChangeFlag(
			SDICursorRow[] lstLocRows, String cursor) {

		ArrayList<SDICursorRow> portCursors = new ArrayList<SDICursorRow>();
		ArrayList<SDICursorRow> berthCursors = new ArrayList<SDICursorRow>();
		ArrayList<SDICursorRow> anchoCursors = new ArrayList<SDICursorRow>();
		ArrayList<SDICursorRow> destCursors = new ArrayList<SDICursorRow>();
		Hashtable<String, String> zoneTypeChangeFlagHt = new Hashtable<String, String>();

		for (SDICursorRow event : lstLocRows) {
			VesselEventFact EventFact = (VesselEventFact) event;
			String zoneType = EventFact.getZone_type();
			String roleType = EventFact.getZone_role();
			if(roleType.toUpperCase().equals("CURRENT")){
				if (zoneType.toUpperCase().equals("PORT"))
					portCursors.add(event);
				else if (zoneType.toUpperCase().equals("BERTH"))
					berthCursors.add(event);
				else if (zoneType.toUpperCase().equals("ANCHORAGE"))
					anchoCursors.add(event);
			}else if(roleType.toUpperCase().equals("DESTINATION")){
				destCursors.add(event);
			}
		}

		String portFlag = getChangeFlag(
				portCursors.toArray(new SDICursorRow[portCursors.size()]),
				cursor,null);
		String berthFlag = getChangeFlag(
				berthCursors.toArray(new SDICursorRow[berthCursors.size()]),
				cursor,null);
		String anchoFlag = getChangeFlag(
				anchoCursors.toArray(new SDICursorRow[anchoCursors.size()]),
				cursor,null);
		String destFlag = getChangeFlag(
				destCursors.toArray(new SDICursorRow[destCursors.size()]),
				cursor,null);
		//		System.out.println("port flag:" + portFlag + " berth Flag:" + berthFlag
		//				+ "  ancho flag:" + anchoFlag);
		if (portFlag != null)
			zoneTypeChangeFlagHt.put("PORT", portFlag);
		if (berthFlag != null)
			zoneTypeChangeFlagHt.put("BERTH", berthFlag);
		if (anchoFlag != null)
			zoneTypeChangeFlagHt.put("ANCHORAGE", anchoFlag);
		if (destFlag != null)
			zoneTypeChangeFlagHt.put("DESTINATION", destFlag);
		return zoneTypeChangeFlagHt;
	}

	public java.sql.Timestamp getDBDate(Connection DBConn) {

		String SQL_1 = "select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual";
		String strDate = null;
		try {
			PreparedStatement ps = DBConn.prepareStatement(SQL_1);
			ResultSet objResult = ps.executeQuery();
			if (objResult.next()) {
				strDate = objResult.getString(1);
			}
			objResult.close();
			ps.close();
		} catch (SQLException e) {
			throw new LogicException("Get sysdate from database error.");
		}
		java.sql.Timestamp curdate = java.sql.Timestamp.valueOf(strDate);
		return curdate;
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

}
