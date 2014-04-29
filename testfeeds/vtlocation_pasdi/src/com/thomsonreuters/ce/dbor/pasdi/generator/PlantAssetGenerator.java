package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.io.File;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;
import org.w3c.dom.Element;




import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.SDIPreLoadCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.Geographic_Unit;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantCommonBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantCommonNote;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantCommonStatus;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantPgeAnalytics;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantPgeOperator;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantPgeOutage;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantPgeStatistic;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantPlaCoalMine;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantPlaOutage;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.PlantPlaStatistic;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.RELATION_OBJECT_ID_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.queue.Pipe;

public class PlantAssetGenerator extends GZIPSDIFileGenerator {

	private static Templates cachedXSLT;
	private static final ThreadLocal<Transformer> safeTransformer = new ThreadLocal<Transformer>() {
		public Transformer initialValue() {
			Transformer transformer = null;
			try {
				transformer = getCachedXSLT().newTransformer();
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
			} catch (TransformerConfigurationException e) {
				SDIConstants.SDILogger.error(
								"Plant SDI Generator: Failed to build XSLT Transformer");
				e.printStackTrace();
			}
			return transformer;
		}
	};

	private static Templates getCachedXSLT() {
		if (cachedXSLT == null) {
			TransformerFactory transFact = TransformerFactory.newInstance();
			Source xsltSource = new StreamSource(new File(
					SDIConstants.PLANT_XSLT));
			try {
				cachedXSLT = transFact.newTemplates(xsltSource);
			} catch (TransformerConfigurationException e) {
				SDIConstants.SDILogger.error(
						"Plant SDI Generator: Failed to build XSLT template");
				e.printStackTrace();
			}
		}
		return cachedXSLT;
	}

	private String errPreMsg;

	private SDIPreLoadCache sdiPLC;

	private Utility util;

	public PlantAssetGenerator(final String sdiFileLocation,
			final String fileName, final Date sdiStartTime,
			final Date sdiEndTime, final int Thread_Num,
			final Pipe<HashMap<CursorType, SDICursorRow[]>> InPort,
			final Counter cursorCounter, final SDIPreLoadCache SDIPLC) {
		super(sdiFileLocation, fileName, sdiStartTime, sdiEndTime, Thread_Num,
				InPort, cursorCounter);
		sdiPLC = SDIPLC;
		util = new Utility();

	}

	@Override
	protected String convertContentItem(
			Map<CursorType, SDICursorRow[]> mapCurRows) {
		String content = "";
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			doc.setStrictErrorChecking(false);

			Element elContentItem = doc.createElement("ContentItem");
			doc.appendChild(elContentItem);

			Element elData = doc.createElement("Data");
			elContentItem.appendChild(elData);
			// elData.setAttribute("type", "PlantAsset");

			Element elPA = doc.createElement("PlantAsset");
			elData.appendChild(elPA);

			Element elPASubType = doc.createElement("PlantAssetSubType");

			Element elPowerPlant = doc.createElement("PowerPlant");

			// ///////////////////////////////////////////

			// Get PhysicalAsset Base info: for caculating action
			SDICursorRow[] lstCurRows = mapCurRows
					.get(CursorType.AST_BASE_INFO);

			if (lstCurRows != null && lstCurRows[0] != null) {
				AstBase baseRow = (AstBase) lstCurRows[0];
				long permId = baseRow.getPerm_ID();
				this.errPreMsg = "File Name:" + this.FileName + "->permId:"
						+ permId;

				Field[] fields = AstBase.class.getDeclaredFields();

				// Generate Base items
				try {
					util.generateSection(baseRow, fields, doc, elPA);
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Asset Base Items->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Asset Base Items->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Asset Base Items->UnknownException");
				}

				Date createdDate = baseRow.getEntity_created_date();
				if (createdDate != null) {
					// Set action value
					String action = this.getAction(createdDate);
					elContentItem.setAttribute("action", action);
				}
			}

			// String commGroupName = "";
			lstCurRows = mapCurRows.get(CursorType.PLANT_COMMON_BASE_INFO);
			if (lstCurRows != null && lstCurRows.length > 0) {
				PlantCommonBase plantCommonBase = (PlantCommonBase) lstCurRows[0];
				// commGroupName = plantCommonBase.getCommodity_group_name();
				try {
					generatePlantBaseEls(plantCommonBase, doc, elPA);
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->Plant Common Base Items->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->Plant Common Base Items->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Plant Common Base Items->UnknownException");
					e.printStackTrace();
				}
			}

			// PlantStatus
			lstCurRows = mapCurRows.get(CursorType.PLANT_COMMON_STATUS_INFO);
			if (lstCurRows != null) {
				try {
					util.processCurRow(PlantCommonStatus.class, lstCurRows,
							doc, elPASubType, "PlantStatus");
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Plant Status->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Plant Status->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Plant Status->UnknownException");
					e.printStackTrace();
				}
			}

			// PlantOutage
			lstCurRows = mapCurRows.get(CursorType.PLANT_PLA_OUTAGE_INFO);
			if (lstCurRows != null) {
				try {
					ProcessPlantOutage(lstCurRows, doc, elPASubType);
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PLA_OUTAGE_INFO->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PLA_OUTAGE_INFO->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->PLANT_PLA_OUTAGE_INFO->UnknownException");
					e.printStackTrace();
				}
			}

			// PlantNote
			lstCurRows = mapCurRows.get(CursorType.PLANT_COMMON_NOTE_INFO);
			if (lstCurRows != null) {
				try {
					util.processCurRow(PlantCommonNote.class, lstCurRows, doc,
							elPASubType, "PlantNote");
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_COMMON_NOTE_INFO->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_COMMON_NOTE_INFO->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->PLANT_COMMON_NOTE_INFO->UnknownException");
					e.printStackTrace();
				}
			}
			// PlantStatistic
			lstCurRows = mapCurRows.get(CursorType.PLANT_PLA_STATISTIC_INFO);
			if (lstCurRows != null)
				try {
					util.processCurRow(PlantPlaStatistic.class, lstCurRows,
							doc, elPASubType, "PlantStatistic");
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PLA_STATISTIC_INFO->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PLA_STATISTIC_INFO->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->PLANT_PLA_STATISTIC_INFO->UnknownException");
					e.printStackTrace();
				}
			// ProcessStatisticRow(HM, elPASubType, commGroupName);

			// String str = xmlToString(elPASubType.getLastChild());

			// PlantCoalMine
			try {
				ProcessCoalMineRow(mapCurRows, elPASubType);
			} catch (IllegalArgumentException e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->PlantCoalMine->IllegalArgumentException");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->PlantCoalMine->IllegalAccessException");
				e.printStackTrace();
			} catch (Exception e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->PlantCoalMine->UnknownException");
				e.printStackTrace();
			}

			// PowerPlant Operator NERC ISO
			lstCurRows = mapCurRows.get(CursorType.PLANT_PGE_OPERATOR_INFO);
			if (lstCurRows != null) {
				try {
					ProcessOperatorCurRow(lstCurRows, doc, elPowerPlant);
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PGE_OPERATOR_INFO->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PGE_OPERATOR_INFO->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->PLANT_PGE_OPERATOR_INFO->UnknownException");
					e.printStackTrace();
				}
			}

			lstCurRows = mapCurRows.get(CursorType.PLANT_PGE_STATISTIC_INFO);
			if (lstCurRows != null) {
				try {
					util.processCurRow(PlantPgeStatistic.class, lstCurRows,
							doc, elPASubType, "PlantStatistic");
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PGE_STATISTIC_INFO->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->PLANT_PGE_STATISTIC_INFO->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->PLANT_PGE_STATISTIC_INFO->UnknownException");
					e.printStackTrace();
				}
			}
			// str = xmlToString(elPASubType.getLastChild());
			//
			try {
				ProcessAnalyticsCurRow(mapCurRows, doc, elPowerPlant);
			} catch (IllegalArgumentException e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->PlantAnalytics->IllegalArgumentException");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->PlantAnalytics->IllegalAccessException");
				e.printStackTrace();
			} catch (Exception e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->PlantAnalytics->UnknownException");
				e.printStackTrace();
			}

			if (elPowerPlant.hasChildNodes()) {
				elPASubType.appendChild(elPowerPlant);
			}
			if (elPASubType.hasChildNodes()) {
				elPA.appendChild(elPASubType);
			}

			if (isProd) {
				try {
					content = transformXml(doc);
				} catch (TransformerException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->TransformerException: Failed to transform document obj to string");
					e.printStackTrace();
				}
			} else {
				content = util.getDocumentAsXml(doc);
			}
		} catch (Exception e) {
			String msg = this.errPreMsg + "->Unknown Exception";
			if (e.getMessage() != null) {
				msg = this.errPreMsg + "->" + e.getMessage();
			}
			SDIConstants.SDILogger.error( msg);
			e.printStackTrace();
		}

		return content;
	}

	private void generatePlantBaseEls(PlantCommonBase plantCommonBase,
			Document doc, Element elPA) throws IllegalArgumentException,
			IllegalAccessException {

		Field[] fields = PlantCommonBase.class.getDeclaredFields();
		Element item;
		String itemName, value;
		Object vo;

		for (Field f : fields) {
			f.setAccessible(true);
			itemName = f.getName();
			vo = f.get(plantCommonBase);
			if (vo != null) {
				item = doc.createElement(itemName);
				if (itemName.equalsIgnoreCase("plant_city_id")) {
					item = doc.createElement("PlantCityID");
					value = vo.toString();
					Geographic_Unit gun = sdiPLC.universe_gun_detail_info
							.get(Long.valueOf(value));
					if (gun == null) // //error
						continue;

					util.setGunAttr(gun, doc, item);

					Element elGun;
					Object gunObj = gun.getRcs_code();
					if (gunObj != null && gunObj.toString().length() > 0) {
						elGun = doc.createElement("PlantCityRCS");
						elGun.appendChild(doc.createTextNode(gunObj.toString()));
						elPA.appendChild(elGun);
					}
					gunObj = gun.getGun_desc();
					if (gunObj != null && gunObj.toString().length() > 0) {
						elGun = doc.createElement("PlantCity");
						elGun.appendChild(doc.createTextNode(gunObj.toString()));
						elPA.appendChild(elGun);
					}
				} else if (vo instanceof java.util.Date) {
					DateFormat formatter = new SimpleDateFormat(
							SDIConstants.DATE_FORMAT_SSS);
					value = formatter.format((Date) vo);
				} else {
					value = vo.toString();
				}
				item.appendChild(doc.createTextNode(value));
				elPA.appendChild(item);
			}
		}

	}

	@Override
	public String getDefaultElement() {
		return "<env:ContentItem  action=\"Overwrite\">\r\n"
				+ " <env:Data xsi:type=\"PlantAsset\" />\r\n"
				+ "</env:ContentItem>";
	}

	@Override
	protected String getFooter() {
		String footer = "		</env:Body>\r\n" + "</env:ContentEnvelope>";
		return footer;
	}

	@Override
	protected String getHeader() {
		String envolope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" xmlns=\"http://CommodityPhysicalAssets.schemas.financial.thomsonreuters.com/2010-11-17/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://CommodityPhysicalAssets.schemas.financial.thomsonreuters.com/2010-11-17/ @XsdType@\"  pubStyle=\"@PubType@\" majVers=\"3\" minVers=\"1.0\">\r\n";

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
		SimpleDateFormat dataFormat = new SimpleDateFormat(
				SDIConstants.DATE_FORMAT_SSS, Locale.getDefault());

		// DateFormat df = DateFormat.getDateTimeInstance(DateFormat.MEDIUM,
		// DateFormat.MEDIUM);
		String strDate = dataFormat.format(XMLTimestamp);
		header = header.replace("@TIMESTAMP@", strDate);
		if (this.IsFullLoad()) {
			header = header.replace("@PubType@", "FullRebuild");
		} else {
			header = header.replace("@PubType@", "Incremental");
		}
		header = header.replace("@XsdType@", "PlantAsset.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

	/**
	 * Process Power Plant Analytics CurRow
	 * 
	 * @param lstCurRows
	 * @param doc
	 * @param elPA
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	private void ProcessAnalyticsCurRow(Map<CursorType, SDICursorRow[]> HM,
			Document doc, Element elPowerPlant)
			throws IllegalArgumentException, IllegalAccessException {

		SDICursorRow[] lstCurRows = HM.get(CursorType.PLANT_PGE_ANALYTICS_INFO);

		if (lstCurRows != null) {

			Element elPowerOutput = doc.createElement("PlantPowerOutput");
			Element elPowerOutage = doc.createElement("PlantPowerOutage");
			for (SDICursorRow row : lstCurRows) {
				Object objName = ((PlantPgeAnalytics) row).getAnalytic_name();
				Object objValue = ((PlantPgeAnalytics) row).getAnalytic_value();
				if (objName != null && objValue != null) {
					String name = objName.toString();
					String value = objValue.toString();
					if (name.equalsIgnoreCase("PlantPowerOutageYTDDays")
							|| name.equalsIgnoreCase("PlantPowerOutageLastYearDays")) {
						value = value.substring(0, value.indexOf("."));
					}

					Element item = doc.createElement(name);
					item.appendChild(doc.createTextNode(value));
					if (name.indexOf("Output") > 0) {
						elPowerOutput.appendChild(item);
					} else if (name.indexOf("Outage") > 0) {
						elPowerOutage.appendChild(item);
					}
				}
			}

			lstCurRows = HM.get(CursorType.PLANT_PGE_OUTAGE_INFO);
			if (lstCurRows != null) {
				util.processCurRow(PlantPgeOutage.class, lstCurRows, doc,
						elPowerOutage, "PlantPowerOutagePlan");
			}

			if (elPowerOutput.hasChildNodes()) {
				elPowerPlant.appendChild(elPowerOutput);
			}
			if (elPowerOutage.hasChildNodes()) {
				elPowerPlant.appendChild(elPowerOutage);
			}

		}
	}

	private void ProcessCoalMineRow(Map<CursorType, SDICursorRow[]> HM,
			Element elPASubType) throws IllegalArgumentException,
			IllegalAccessException {
		SDICursorRow[] lstCurRows = HM.get(CursorType.PLANT_PLA_COAL_MINE_INFO);

		if (lstCurRows != null) {

			Document doc = elPASubType.getOwnerDocument();
			PlantPlaCoalMine plaCoalMineRow;
			int initCoalMineId = -1;
			Element elCoalMine = null;
			Field[] fields = PlantPlaCoalMine.class.getDeclaredFields();
			List<String> lstCMType = new ArrayList<String>();
			List<String> lstCMRan = new ArrayList<String>();
			List<String> lstCMVol = new ArrayList<String>();
			List<String> lstCMUse = new ArrayList<String>();

			for (int j = 0; j < lstCurRows.length; j++) {
				plaCoalMineRow = (PlantPlaCoalMine) lstCurRows[j];
				if (plaCoalMineRow.getCoal_mine_id() != initCoalMineId) {
					initCoalMineId = plaCoalMineRow.getCoal_mine_id();
					lstCMType.clear();
					lstCMRan.clear();
					lstCMVol.clear();
					lstCMUse.clear();
					elCoalMine = doc.createElement("PlantCoalMine");
					Element item;
					for (int i = 0; i < fields.length; i++) {
						fields[i].setAccessible(true);
						Object obj = fields[i].get(plaCoalMineRow);
						if (obj != null && obj.toString().length() > 0) {
							String itemName = fields[i].getName();
							item = doc.createElement(itemName);
							item.appendChild(doc.createTextNode(obj.toString()));
							elCoalMine.appendChild(item);
							if (itemName.equalsIgnoreCase("coal_mine_type")) {
								lstCMType.add(obj.toString());
							} else if (itemName
									.equalsIgnoreCase("coal_mine_coal_rank")) {
								lstCMRan.add(obj.toString());
							} else if (itemName
									.equalsIgnoreCase("coal_mine_coal_volatility")) {
								lstCMVol.add(obj.toString());
							} else if (itemName
									.equalsIgnoreCase("coal_mine_coal_use")) {
								lstCMUse.add(obj.toString());
							}
						}
					}
				} else {
					String value = plaCoalMineRow.getCoal_mine_type();
					if (value != null && value.length() > 0
							&& !lstCMType.contains(value)) {
						Element item = doc.createElement("coal_mine_type");
						item.appendChild(doc.createTextNode(value));
						elCoalMine.appendChild(item);
						lstCMType.add(value);
					}
					value = plaCoalMineRow.getCoal_mine_coal_rank();
					if (value != null && value.length() > 0
							&& !lstCMRan.contains(value)) {
						Element item = doc.createElement("coal_mine_coal_rank");
						item.appendChild(doc.createTextNode(value));
						elCoalMine.appendChild(item);
						lstCMRan.add(value);
					}
					value = plaCoalMineRow.getCoal_mine_coal_volatility();
					if (value != null && value.length() > 0
							&& !lstCMVol.contains(value)) {
						Element item = doc
								.createElement("coal_mine_coal_volatility");
						item.appendChild(doc.createTextNode(value));
						elCoalMine.appendChild(item);
						lstCMVol.add(value);
					}
					value = plaCoalMineRow.getCoal_mine_coal_use();
					if (value != null && value.length() > 0
							&& !lstCMUse.contains(value)) {
						Element item = doc.createElement("coal_mine_coal_use");
						item.appendChild(doc.createTextNode(value));
						elCoalMine.appendChild(item);
						lstCMUse.add(value);
					}
				}
				if (elCoalMine.hasChildNodes()) {
					elPASubType.appendChild(elCoalMine);
				}
			}
		}
	}

	/**
	 * Process Plant Operator CurRow
	 * 
	 * @param lstCurRows
	 * @param doc
	 * @param elPA
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	private void ProcessOperatorCurRow(SDICursorRow[] lstCurRows, Document doc,
			Element elPowerPlant) throws IllegalArgumentException,
			IllegalAccessException {
		if (lstCurRows != null) {
			Element item;
			String itemName, value;

			for (int i = 0; i < lstCurRows.length; i++) {
				RELATION_OBJECT_ID_TYPE systemOperatorId = ((PlantPgeOperator) lstCurRows[i])
						.getSystem_operator_id();
				String operatorType = ((PlantPgeOperator) lstCurRows[i])
						.getSystem_operator_type();
				String operatorName = ((PlantPgeOperator) lstCurRows[i])
						.getSystem_operator_name();
				Field[] flds = RELATION_OBJECT_ID_TYPE.class
						.getDeclaredFields();
				if (operatorType != null) {
					Element elOperatorName = null;
					if (operatorType.equalsIgnoreCase("NERC")) {
						if (systemOperatorId != null) {
							item = doc
									.createElement("PlantPowerSystemOperatorNERCID");
							for (Field f : flds) {
								itemName = f.getName();
								f.setAccessible(true);
								Object vo = f.get(systemOperatorId);
								if (vo != null) {
									if (vo instanceof java.util.Date) {
										DateFormat formatter = new SimpleDateFormat(
												SDIConstants.DATE_FORMAT_SSS);
										value = formatter.format((Date) vo);
									} else {
										value = vo.toString();
									}

									if (itemName.equals("object_id")) {
										item.appendChild(doc
												.createTextNode(value));
										elPowerPlant.appendChild(item);
									} else {
										item.setAttribute(itemName, value);
									}
								}
							}
						}
						if (operatorName != null) {
							elOperatorName = doc
									.createElement("PlantPowerSystemOperatorNERC");
							elOperatorName.appendChild(doc
									.createTextNode(operatorName));
							elPowerPlant.appendChild(elOperatorName);
						}
					} else if (operatorType.equalsIgnoreCase("ISO")) {
						if (systemOperatorId != null) {
							item = doc
									.createElement("PlantPowerSystemOperatorISOID");
							for (Field f : flds) {
								itemName = f.getName();
								f.setAccessible(true);
								Object vo = f.get(systemOperatorId);
								if (vo != null) {
									if (vo instanceof java.util.Date) {
										DateFormat formatter = new SimpleDateFormat(
												SDIConstants.DATE_FORMAT_SSS);
										value = formatter.format((Date) vo);
									} else {
										value = vo.toString();
									}

									if (itemName.equals("object_id")) {
										item.appendChild(doc
												.createTextNode(value));
										elPowerPlant.appendChild(item);
									} else {
										item.setAttribute(itemName, value);
									}
								}
							}
						}
						if (operatorName != null) {
							elOperatorName = doc
									.createElement("PlantPowerSystemOperatorISO");
							elOperatorName.appendChild(doc
									.createTextNode(operatorName));
							elPowerPlant.appendChild(elOperatorName);
						}
					}

				}
			}
		}
	}

	private void ProcessPlantOutage(SDICursorRow[] lstCurRows, Document doc,
			Element elPASubType) throws IllegalArgumentException,
			IllegalAccessException {
		// PlantPlaOutage.class,

		Field[] flds = PlantPlaOutage.class.getDeclaredFields();
		Element plantOutageEl;
		Element item;
		String itemName, value;

		for (SDICursorRow row : lstCurRows) {
			plantOutageEl = doc.createElement("PlantOutage");
			Object vo;
			for (Field f : flds) {
				f.setAccessible(true);
				itemName = f.getName();
				vo = f.get(row);
				if (vo != null) {
					item = doc.createElement(itemName);
					if (itemName.equalsIgnoreCase("outage_country_id")) {
						item = doc.createElement("PlantOutageCountryID");
						value = vo.toString();
						Geographic_Unit gun = sdiPLC.universe_gun_detail_info
								.get(Long.valueOf(value));
						if (gun == null) // //error
							continue;

						// xslt copy element
						util.setGunAttr(gun, doc, item);
					} else if (vo instanceof java.util.Date) {
						DateFormat formatter = new SimpleDateFormat(
								SDIConstants.DATE_FORMAT_SSS);
						value = formatter.format((Date) vo);
					} else {
						value = vo.toString();
					}
					item.appendChild(doc.createTextNode(value));
					plantOutageEl.appendChild(item);
				}
			}
			if (plantOutageEl.hasChildNodes())
				elPASubType.appendChild(plantOutageEl);
		}
	}

	private String transformXml(Document doc) throws TransformerException {
		String result = "";
		Source xmlSource = new javax.xml.transform.dom.DOMSource(doc);

		// DOMResult result = new DOMResult();
		StringWriter writer = new StringWriter();

		// do the transformation
		safeTransformer.get().transform(xmlSource, new StreamResult(writer));
		result = writer.toString();

		result = result
				.replaceAll(
						"xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\"",
						"");
		result = result.replaceAll("type=\"PlantAsset\">",
				"xsi:type=\"PlantAsset\">");

		return result;
	}

}
