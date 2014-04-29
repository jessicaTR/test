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
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselEventOriginDestination;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselLatestLoc;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.queue.Pipe;

public class VesselAssetGenerator extends GZIPSDIFileGenerator {

	private static Templates cachedXSLT;
	private static final ThreadLocal<Transformer> safeTransformer = new ThreadLocal<Transformer>() {
		public Transformer initialValue() {
			Transformer transformer = null;

			try {
				transformer = VesselAssetGenerator.getCachedXSLT()
						.newTransformer();
			} catch (TransformerConfigurationException e) {
				SDIConstants.SDILogger.error(
								"Vessel SDI Generator: Failed to build XSLT Transformer");
				e.printStackTrace();
			}
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

			return transformer;
		}
	};

	private static Templates getCachedXSLT() {
		if (cachedXSLT == null) {
			TransformerFactory transFact = TransformerFactory.newInstance();
			Source xsltSource = new StreamSource(new File(
					SDIConstants.VESSEL_XSLT));
			try {
				cachedXSLT = transFact.newTemplates(xsltSource);
			} catch (TransformerConfigurationException e) {
				SDIConstants.SDILogger.error(
						"Vessel SDI Generator: Failed to build XSLT template");
				e.printStackTrace();
			}
		}
		return cachedXSLT;
	}

	private String errPreMsg;

	private SDIPreLoadCache sdiPLC;

	private Utility util;

	public VesselAssetGenerator(final String sdiFileLocation,
			final String fileName, final Date sdiStartTime,
			final Date sdiEndTime, final int Thread_Num,
			final Pipe<HashMap<CursorType, SDICursorRow[]>> InPort,
			final Counter cursorCounter, final SDIPreLoadCache sdiPLC) {
		super(sdiFileLocation, fileName, sdiStartTime, sdiEndTime, Thread_Num,
				InPort, cursorCounter);
		util = new Utility();
		this.sdiPLC = sdiPLC;
	}

	@Override
	protected String convertContentItem(
			Map<CursorType, SDICursorRow[]> mapCurRows) {
		String content = "";
		long permId;
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			doc.setStrictErrorChecking(false);

			Element elContentItem = doc.createElement("ContentItem");
			doc.appendChild(elContentItem);

			Element elData = doc.createElement("Data");
			elContentItem.appendChild(elData);
			// elData.setAttribute("type", "PlantAsset");

			Element elVA = doc.createElement("VesselAsset");
			elData.appendChild(elVA);

			// ///////////////////////////////////////////

			// Get PhysicalAsset Base info: for caculating action
			SDICursorRow[] lstCurRows = mapCurRows
					.get(CursorType.AST_BASE_INFO);

			if (lstCurRows != null && lstCurRows[0] != null) {
				AstBase baseRow = (AstBase) lstCurRows[0];
				permId = baseRow.getPerm_ID();
				Field[] fields = AstBase.class.getDeclaredFields();

				this.errPreMsg = "File Name:" + this.FileName + "->permId:"
						+ permId;
				// Generate Base items
				try {
					util.generateSection(baseRow, fields, doc, elVA);
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
							+ "->Generate Asset Base items->UnkonwnException");
					e.printStackTrace();
				}

				Date createdDate = baseRow.getEntity_created_date();
				if (createdDate != null) {
					// Set action value
					String action = this.getAction(createdDate);
					elContentItem.setAttribute("action", action);
				}
			}

			lstCurRows = mapCurRows.get(CursorType.VESSEL_BASE_INFO);
			if (lstCurRows != null && lstCurRows.length > 0) {
				VesselBase vesBaseRow = (VesselBase) lstCurRows[0];
				Field[] fields = VesselBase.class.getDeclaredFields();

				try {
					util.generateSection(vesBaseRow, fields, doc, elVA);
				} catch (IllegalArgumentException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->Generate Vessel Base items->IllegalArgumentException");
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					SDIConstants.SDILogger.error(
									this.errPreMsg
											+ "->Generate Vessel Base items->IllegalAccessException");
					e.printStackTrace();
				} catch (Exception e) {
					SDIConstants.SDILogger.error( this.errPreMsg
							+ "->Generate Vessel Base items->UnkonwnException");
					e.printStackTrace();
				}
			}

			try {
				ProcessOpenEvent(mapCurRows, doc, elVA);
			} catch (Exception e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->Vessel OpenEvent->UnkonwnException");
				e.printStackTrace();
			}
			try {
				ProcessDestination(mapCurRows, doc, elVA);
			} catch (Exception e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->Vessel Destination->UnkonwnException");
				e.printStackTrace();
			}
			try {
				ProcessOrigin(mapCurRows, doc, elVA);
			} catch (Exception e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->Vessel Origin->UnkonwnException");
				e.printStackTrace();
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

	private void GenerateLatestLocationItems(VesselLatestLoc loc, Document doc,
			Element elLocation) throws IllegalArgumentException,
			IllegalAccessException {
		Field[] fields = VesselLatestLoc.class.getDeclaredFields();

		for (int i = 0; i < fields.length - 3; i++) {
			fields[i].setAccessible(true);
			Object obj = fields[i].get(loc);
			String itemName = fields[i].getName();
			if (obj != null && !itemName.equalsIgnoreCase("loc_eta")) {
				String itemValue = null;
				if (obj instanceof java.util.Date) {
					DateFormat formatter = new SimpleDateFormat(
							SDIConstants.DATE_FORMAT_SSS);
					itemValue = formatter.format((Date) obj);
				} else {
					itemValue = obj.toString();
				}
				Element item = doc.createElement(itemName);
				item.appendChild(doc.createTextNode(itemValue));
				elLocation.appendChild(item);
			}
		}
		// Generate VesselLocationSource section;
		Element elLS = doc.createElement("VesselLocationSource");
		for (int k = (fields.length - 3); k < fields.length; k++) {
			fields[k].setAccessible(true);
			Object obj = fields[k].get(loc);
			if (obj != null) {
				String itemName = fields[k].getName();
				String itemValue = null;
				if (obj instanceof java.util.Date) {
					DateFormat formatter = new SimpleDateFormat(
							SDIConstants.DATE_FORMAT_SSS);
					itemValue = formatter.format((Date) obj);
				} else {
					itemValue = obj.toString();
				}
				Element item = doc.createElement(itemName);
				item.appendChild(doc.createTextNode(itemValue));
				elLS.appendChild(item);
			}
		}
		if (elLS.hasChildNodes()) {
			elLocation.appendChild(elLS);
		}

	}

	private void GenerateOpenEventItems(SDICursorRow[] openEvents,
			Document doc, Element elLocation) throws IllegalArgumentException,
			IllegalAccessException {

		for (int i = 0; i < openEvents.length; i++) {
			// TODO:
			VesselEventOriginDestination vEOD = (VesselEventOriginDestination) openEvents[i];
			util.generateVesselGun(new String[] { "VesselLocationZone",
					"VesselLocationGun" }, vEOD, elLocation, sdiPLC);
		}
	}

	@Override
	public String getDefaultElement() {
		return "<env:ContentItem  action=\"Overwrite\">\r\n"
				+ " <env:Data xsi:type=\"VesselAsset\" />\r\n"
				+ "</env:ContentItem>";
	}

	@Override
	protected String getFooter() {
		// publisher.dispose();
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

		header = header.replace("@XsdType@", "VesselAsset.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

	private void ProcessDestination(Map<CursorType, SDICursorRow[]> HM,
			Document doc, Element elVA) {
		SDICursorRow[] lstLocRows = HM.get(CursorType.VESSEL_LATEST_LOC_INFO);
		VesselLatestLoc latestLoc;

		Element elLocation = doc.createElement("VesselLocation");
		Element elLocRole = doc.createElement("VesselLocationRole");
		elLocRole.appendChild(doc.createTextNode("Destination"));
		elLocation.appendChild(elLocRole);

		if (lstLocRows != null) {
			latestLoc = (VesselLatestLoc) lstLocRows[0];
			Date obj = latestLoc.getLoc_eta();
			if (obj != null) {
				DateFormat formatter = new SimpleDateFormat(
						SDIConstants.DATE_FORMAT_SSS);
				String eta = formatter.format((Date) obj);

				Element elETA = doc.createElement("loc_eta");
				elETA.appendChild(doc.createTextNode(eta));
				elLocation.appendChild(elETA);
			}
		}

		SDICursorRow[] lstDestRows = HM.get(CursorType.VESSEL_DESTINATION_INFO);
		try {
			if (lstDestRows != null && lstDestRows.length > 0) {
				// TODO:
				for (SDICursorRow row : lstDestRows) {
					VesselEventOriginDestination vEOD = (VesselEventOriginDestination) row;
					util.generateVesselGun(new String[] { "VesselLocationZone",
							"VesselLocationGun" }, vEOD, elLocation, sdiPLC);
				}
			}
			// util.processCurRow(VesselDestination.class, lstDestRows, doc,
			// elLocation, "VesselLocationZone");
		} catch (IllegalArgumentException e) {
			SDIConstants.SDILogger.error(
							this.errPreMsg
									+ "->function:ProcessDestination->IllegalArgumentException");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			SDIConstants.SDILogger.error( this.errPreMsg
					+ "->function:ProcessDestination->IllegalAccessException");
			e.printStackTrace();
		}

		if (elLocation.getChildNodes().getLength() > 1)
			elVA.appendChild(elLocation);
	}

	/**
	 * Process Vessel Location Current Row
	 * 
	 * @param lstCurRows
	 * @param doc
	 * @param elVA
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	private void ProcessOpenEvent(Map<CursorType, SDICursorRow[]> HM,
			Document doc, Element elVA) {
		SDICursorRow[] lstLocRows = HM.get(CursorType.VESSEL_LATEST_LOC_INFO);
		SDICursorRow[] lstEventRows = HM.get(CursorType.VESSEL_OPEN_EVENT_INFO);
		Element elLocation = doc.createElement("VesselLocation");
		Element elLocRole = doc.createElement("VesselLocationRole");
		elLocRole.appendChild(doc.createTextNode("Current"));
		elLocation.appendChild(elLocRole);

		if (lstLocRows != null) {
			try {
				GenerateLatestLocationItems((VesselLatestLoc) lstLocRows[0],
						doc, elLocation);
			} catch (IllegalArgumentException e) {
				SDIConstants.SDILogger.error(
								this.errPreMsg
										+ "->function:GenerateLatestLocationItems->IllegalArgumentException");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				SDIConstants.SDILogger.error(
								this.errPreMsg
										+ "->function:GenerateLatestLocationItems->IllegalAccessException");
				e.printStackTrace();
			}
		}
		if (lstEventRows != null) {
			try {
				GenerateOpenEventItems(lstEventRows, doc, elLocation);
			} catch (IllegalArgumentException e) {
				SDIConstants.SDILogger.error(
								this.errPreMsg
										+ "->function:GenerateOpenEventItems->IllegalArgumentException");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				SDIConstants.SDILogger.error(
								this.errPreMsg
										+ "->function:GenerateOpenEventItems->IllegalAccessException");
				e.printStackTrace();
			}
		}

		if (elLocation.getChildNodes().getLength() > 1)
			elVA.appendChild(elLocation);
	}

	private void ProcessOrigin(Map<CursorType, SDICursorRow[]> HM,
			Document doc, Element elVA) {

		SDICursorRow[] lstOriginRows = HM.get(CursorType.VESSEL_ORIGIN_INFO);
		if (lstOriginRows != null) {
			Element elLocation = doc.createElement("VesselLocation");
			Element elLocRole = doc.createElement("VesselLocationRole");
			elLocRole.appendChild(doc.createTextNode("Origin"));
			elLocation.appendChild(elLocRole);
			// TODO:
			try {
				if (lstOriginRows != null && lstOriginRows.length > 0) {
					for (SDICursorRow row : lstOriginRows) {
						VesselEventOriginDestination vEOD = (VesselEventOriginDestination) row;
						util.generateVesselGun(new String[] {
								"VesselLocationZone", "VesselLocationGun" },
								vEOD, elLocation, sdiPLC);
					}
				}
			} catch (IllegalArgumentException e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->function:ProcessOrigin->IllegalArgumentException");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				SDIConstants.SDILogger.error( this.errPreMsg
						+ "->function:ProcessOrigin->IllegalAccessException");
				e.printStackTrace();
			}

			if (elLocation.getChildNodes().getLength() > 1)
				elVA.appendChild(elLocation);
		}
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
		result = result.replaceAll("type=\"VesselAsset\">",
				"xsi:type=\"VesselAsset\">");

		return result;
	}
}
