package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.io.File;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
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
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AssetZoom;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstCoordinate;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstLocation;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstName;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstOrgAss;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstStatus;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstTypeCommodity;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.Geographic_Unit;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselEventOriginDestination;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.queue.Pipe;

public class CommodityPhysicalAssetGenerator extends GZIPSDIFileGenerator {

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
								"CommodityPhysical SDI Generator: Failed to build XSLT Transformer");
				e.printStackTrace();
			}
			return transformer;
		}
	};

	private static Templates getCachedXSLT() {
		if (cachedXSLT == null) {
			TransformerFactory transFact = TransformerFactory.newInstance();
			Source xsltSource = new StreamSource(new File(
					SDIConstants.PHYSICAL_XSLT));
			try {
				cachedXSLT = transFact.newTemplates(xsltSource);
			} catch (TransformerConfigurationException e) {
				SDIConstants.SDILogger.error(
								"CommodityPhysical SDI Generator: Failed to build XSLT template");
				e.printStackTrace();
			}
		}
		return cachedXSLT;
	}

	private String errPreMsg;

	private SDIPreLoadCache sdiPLC;

	private Utility util;

	public CommodityPhysicalAssetGenerator(final String sdiFileLocation,
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
			final Map<CursorType, SDICursorRow[]> mapCurRows) {
		String content = "";
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			doc.setStrictErrorChecking(false);
			Element elContentItem = doc.createElement("ContentItem");
			doc.appendChild(elContentItem);
			Element elData = doc.createElement("Data");
			elContentItem.appendChild(elData);

			Element elCPA = doc.createElement("CommodityPhysicalAsset");
			elData.appendChild(elCPA);

			SDICursorRow[] lstCurRows = mapCurRows
					.get(CursorType.AST_BASE_INFO);

			String pasType = null;
			if (lstCurRows != null && lstCurRows[0] != null) {
				AstBase baseRow = (AstBase) lstCurRows[0];
				long permId = baseRow.getPerm_ID();
				this.errPreMsg = "File Name:" + this.FileName + "->permId:"
						+ permId;
				pasType = baseRow.getPas_type();
				Field[] fields = AstBase.class.getDeclaredFields();

				// Generate Base items
				try {
					util.generateSection(baseRow, fields, doc, elCPA);
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->Generate Asset Base items->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->Generate Asset Base items->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Generate Asset Base items->UnknownException");
				}

				Date createdDate = baseRow.getEntity_created_date();
				if (createdDate != null) {
					String action = this.getAction(createdDate);
					elContentItem.setAttribute("action", action);
				}
			}

			generateSectionsContent(mapCurRows, elCPA, pasType);

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

	private void generateSectionsContent(
			final Map<CursorType, SDICursorRow[]> hmCTypeRows,
			final Element elCPA, final String pasType) {
		boolean isVesselDead = false;

		Document doc = elCPA.getOwnerDocument();
		// PhysicalAsset Name
		SDICursorRow[] lstCurRows = hmCTypeRows.get(CursorType.AST_NAME_INFO);
		try {
			util.processCurRow(AstName.class, lstCurRows, doc, elCPA,
					"AssetName");
		} catch (IllegalArgumentException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetName->IllegalArgumentException");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetName->IllegalAccessException");
			e.printStackTrace();
		} catch (Exception e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetName->UnknownException");
		}

		lstCurRows = hmCTypeRows.get(CursorType.AST_ORG_ASS_INFO);
		try {
			util.processCurRow(AstOrgAss.class, lstCurRows, doc, elCPA,
					"AssetOrganisation");
		} catch (IllegalArgumentException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetOrganisation->IllegalArgumentException");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetOrganisation->IllegalAccessException");
			e.printStackTrace();
		} catch (Exception e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetOrganisation->UnknownException");
		}

		try {
			if (processStatusRow(hmCTypeRows, doc, elCPA)) {
				isVesselDead = true;
			}
		} catch (Exception e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->Asset Status->UnknownException");
		}

		lstCurRows = hmCTypeRows.get(CursorType.AST_TYPE_COMMODITY_INFO);
		try {
			util.processCurRow(AstTypeCommodity.class, lstCurRows, doc, elCPA,
					"AssetCommodityType");
		} catch (IllegalArgumentException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetCommodityType->IllegalArgumentException");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetCommodityType->IllegalAccessException");
			e.printStackTrace();
		} catch (Exception e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetCommodityType->UnknownException");
		}

		// TODO:
		try {
			processGeographicUnit(pasType, hmCTypeRows, doc, elCPA);
		} catch (IllegalArgumentException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->GeographicUnit->IllegalArgumentException");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->GeographicUnit->IllegalAccessException");
			e.printStackTrace();
		} catch (Exception e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->GeographicUnit->UnknownException");
		}

		// TODO:
		try {
			processAssetZoom(hmCTypeRows, isVesselDead, doc, elCPA);
		} catch (Exception e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->AssetZoom->UnknownException");
		}
	}

	@Override
	public String getDefaultElement() {
		return "<env:ContentItem  action=\"Overwrite\">\r\n"
				+ " <env:Data xsi:type=\"CommodityPhysicalAsset\" />\r\n"
				+ "</env:ContentItem>";
	}

	@Override
	protected String getFooter() {
		return "		</env:Body>\r\n" + "</env:ContentEnvelope>";
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
		DateFormat dataFormat = new SimpleDateFormat(
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

		header = header.replace("@XsdType@", "CommodityPhysicalAsset.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

	private void processAssetZoom(
			final Map<CursorType, SDICursorRow[]> hmCTypeRows,
			boolean isVesselDead, Document doc, Element elCPA) {
		AssetZoom assetZoom = new AssetZoom(hmCTypeRows, sdiPLC);
		// AssetZoom
		if (!isVesselDead) {
			if (assetZoom.Exists()) {
				Element elZA = doc.createElement("AssetZoom");

				String AssetZoomRegions = assetZoom.getAssetZoomRegions();
				String zoomRegionsRCS = assetZoom.getAssetZoomRegionsRCS();
				String zoomRegionsBLat = assetZoom
						.getAssetZoomRegionsBoundsLat();
				String zoomRegionsBLon = assetZoom
						.getAssetZoomRegionsBoundsLon();
				if (AssetZoomRegions != null && AssetZoomRegions.length() > 0) {
					Element elzoomRegions = doc
							.createElement("AssetZoomRegions");
					elzoomRegions.appendChild(doc
							.createTextNode(AssetZoomRegions));
					elZA.appendChild(elzoomRegions);
				}
				if (zoomRegionsRCS != null && zoomRegionsRCS.length() > 0) {
					Element elzoomRegionsRCS = doc
							.createElement("AssetZoomRegionsRCS");
					elzoomRegionsRCS.appendChild(doc
							.createTextNode(zoomRegionsRCS));
					elZA.appendChild(elzoomRegionsRCS);
				}
				if (zoomRegionsBLat != null && zoomRegionsBLat.length() > 0) {
					Element elAZRB = doc
							.createElement("AssetZoomRegionsBoundsLat");
					elAZRB.appendChild(doc.createTextNode(zoomRegionsBLat));
					elZA.appendChild(elAZRB);
				}
				if (zoomRegionsBLon != null && zoomRegionsBLon.length() > 0) {
					Element item = doc
							.createElement("AssetZoomRegionsBoundsLon");
					item.appendChild(doc.createTextNode(zoomRegionsBLon));
					elZA.appendChild(item);
				}
				if (elZA.hasChildNodes()) {
					elCPA.appendChild(elZA);
				}
			}

			SDICursorRow[] lstCurRows = hmCTypeRows
					.get(CursorType.AST_COORDINATE_INFO);
			try {
				util.processCurRow(AstCoordinate.class, lstCurRows, doc, elCPA,
						"AssetCoordinate");
			} catch (IllegalArgumentException e) {
				SDIConstants.SDILogger.error( errPreMsg
						+ "->AssetCoordinate->IllegalArgumentException");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				SDIConstants.SDILogger.error( errPreMsg
						+ "->AssetCoordinate->IllegalAccessException");
				e.printStackTrace();
			} catch (Exception e) {
				SDIConstants.SDILogger.error( errPreMsg
						+ "->AssetCoordinate->UnknownException");
			}

		}
	}

	private void processGeographicUnit(final String pasType,
			final Map<CursorType, SDICursorRow[]> hmCTypeRows, Document doc,
			Element elCPA) throws IllegalArgumentException,
			IllegalAccessException {
		SDICursorRow[] lstCurRows = null;
		if (pasType.equalsIgnoreCase("VESSEL")) {
			lstCurRows = hmCTypeRows.get(CursorType.VESSEL_OPEN_EVENT_INFO);
			if (lstCurRows != null) {
				for (int i = 0; i < lstCurRows.length; i++) {
					VesselEventOriginDestination vEOD = (VesselEventOriginDestination) lstCurRows[i];
					// TODO:
					util.generateVesselGun(new String[] {
							"VesselGeographicUnit", "AssetGeographicUnit" },
							vEOD, elCPA, sdiPLC);
				}
			}

		} else if (hmCTypeRows.get(CursorType.AST_LOCATION_INFO) != null) {
			lstCurRows = hmCTypeRows.get(CursorType.AST_LOCATION_INFO);
			AstLocation loc = null;
			for (int i = 0; i < lstCurRows.length; i++) {
				loc = (AstLocation) lstCurRows[i];
				Long gunId = loc.getGun_id();
				Geographic_Unit gu = sdiPLC.universe_gun_detail_info.get(gunId);
				if (gu != null) { // error
					// TODO:
					Element elPlantGU = doc
							.createElement("AssetGeographicUnit");
					Field[] fields = Geographic_Unit.class.getDeclaredFields();
					util.generateSection(gu, fields, doc, elPlantGU);
					if (elPlantGU.hasChildNodes())
						elCPA.appendChild(elPlantGU);
					// processPlantGunRow(gu, doc, elCPA);
				}
			}
		}
	}

	@SuppressWarnings("unused")
	private void processPlantGunRow(final Geographic_Unit gun,
			final Document doc, Element elCPA) {

		Element elItem = null;
		Element elParent = doc.createElement("PlantGeographicUnit");
		if (gun.getGun_perm_id() != null) {
			elItem = doc.createElement("AssetGeographicUnitID");
			elItem.appendChild(doc.createTextNode(String.valueOf(gun
					.getGun_perm_id())));
			util.setGunAttr(gun, doc, elItem);
			elParent.appendChild(elItem);
		}

		if ((gun.getRcs_code() != null)
				&& (gun.getRcs_code().toString().length() > 0)) {
			elItem = doc.createElement("AssetGeographicUnitRCS");
			elItem.appendChild(doc.createTextNode(gun.getRcs_code().toString()));
			elParent.appendChild(elItem);
		}
		if (gun.getGun_desc() != null
				&& gun.getGun_desc().toString().length() > 0) {
			elItem = doc.createElement("AssetGeographicUnit");
			elItem.appendChild(doc.createTextNode(gun.getGun_desc()));
			elParent.appendChild(elItem);
		}
		if (gun.getGuc_desc() != null
				&& gun.getGuc_desc().toString().length() > 0) {
			elItem = doc.createElement("AssetGeographicUnitType");
			elItem.appendChild(doc.createTextNode(gun.getGuc_desc()));
			elParent.appendChild(elItem);
		}

		if (elParent.hasChildNodes()) {
			elItem = doc.createElement("AssetGeographicUnitRank");
			// TODO:notyet
			elItem.appendChild(doc.createTextNode(String.valueOf(1)));
			elParent.appendChild(elItem);

			elCPA.appendChild(elParent);
		}

	}

	/**
	 * Process Asset Status Row
	 * 
	 * @param lstCurRows
	 * @param doc
	 * @param elCPA
	 */
	private boolean processStatusRow(
			Map<CursorType, SDICursorRow[]> hmCTypeRows, Document doc,
			Element elCPA) {
		boolean isVesselDead = false;
		SDICursorRow[] lstCurRows = hmCTypeRows.get(CursorType.AST_STATUS_INFO);
		if (lstCurRows != null) {
			for (int i = 0; i < lstCurRows.length; i++) {
				boolean isVessel = false;
				boolean isDead = false;

				String statusType = ((AstStatus) lstCurRows[i])
						.getStatus_type();
				String statusValue = ((AstStatus) lstCurRows[i])
						.getStatus_value();
				Element elAssetStatus = doc.createElement("AssetStatus");
				if (statusType != null && statusType.length() > 0) {
					if (statusType.equalsIgnoreCase("Vessel Status")) {
						isVessel = true;
					}
					Element item = doc.createElement("AssetStatusType");
					item.appendChild(doc.createTextNode(statusType));
					elAssetStatus.appendChild(item);
				}
				if (statusValue != null && statusValue.length() > 0) {
					if (statusValue.equalsIgnoreCase("Dead")
							|| statusValue.equalsIgnoreCase("Cancelled")) {
						isDead = true;
					}
					Element item = doc.createElement("AssetStatusTypeValue");
					item.appendChild(doc.createTextNode(statusValue));
					elAssetStatus.appendChild(item);
				}
				if (elAssetStatus.hasChildNodes()) {
					elCPA.appendChild(elAssetStatus);
				}
				if (isVessel && isDead) {
					isVesselDead = true;
				}
			}
		}
		return isVesselDead;
	}

	/**
	 * Transform raw xml document with xslt
	 * 
	 * @param doc
	 *            Raw xmldocument
	 * @return Formatted xml as string
	 * @throws TransformerException
	 */
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
		result = result.replaceAll("type=\"CommodityPhysicalAsset\">",
				"xsi:type=\"CommodityPhysicalAsset\">");

		return result;
	}
}
