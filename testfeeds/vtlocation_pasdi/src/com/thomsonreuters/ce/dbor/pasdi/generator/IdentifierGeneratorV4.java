package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;






import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstIdentifierValue;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.IDENTIFIER_VALUE_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.queue.Pipe;

public class IdentifierGeneratorV4 extends GZIPSDIFileGenerator {
	private Utility util;

	public IdentifierGeneratorV4(String fileLocation, String fileName,
			Date startTime, Date endTime, int threadNum,
			Pipe<HashMap<CursorType, SDICursorRow[]>> pipeIn, Counter ct) {
		super(fileLocation, fileName, startTime, endTime, threadNum, pipeIn, ct);
		util = new Utility();
	}

	@Override
	protected String convertContentItem(
			Map<CursorType, SDICursorRow[]> mapCurRows) {
		String result = "";
		try {
			AstBase baseRow = (AstBase) mapCurRows
					.get(CursorType.AST_BASE_INFO)[0];
			String adminStatus = baseRow.getAdmin_status();
			// if adminStatus is "Obsolete", won't populate it.
			if (adminStatus != null
					&& !adminStatus.equalsIgnoreCase("Obsolete")) {
				result = generateAssetContent(mapCurRows);
			}
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "Exception", e);
		}

		return result;
	}

	private String generateAssetContent(
			Map<CursorType, SDICursorRow[]> mapCurRows) {
		String content = "";
		Document doc;
		try {
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		} catch (ParserConfigurationException e1) {
			// TODO Auto-generated catch block
			throw new SystemException(e1);
		}
		doc.setStrictErrorChecking(false);

		Element elContentItem, elData, elIdentifier;
		SDICursorRow[] arrIden = mapCurRows
				.get(CursorType.AST_IDENTIFIER_VALUE_INFO);

		if (arrIden != null) {
			for (SDICursorRow obj : arrIden) {
				AstIdentifierValue identifierVal = (AstIdentifierValue) obj;
				IDENTIFIER_VALUE_TYPE identifierObj = identifierVal
						.getIdentifierValue();
				elContentItem = doc.createElement("env:ContentItem");
				// elContentItem.setAttribute("action", action);
				doc.appendChild(elContentItem);

				elData = doc.createElement("env:Data");
				elData.setAttribute("type", "IdentifierItem");
				elContentItem.appendChild(elData);
				elIdentifier = doc.createElement("Identifier");
				elData.appendChild(elIdentifier);

				String idenValue = identifierObj.getIdentifier_value();
				if (idenValue != null) {
					Element elIdenValue = doc.createElement("IdentifierValue");
					elIdenValue.appendChild(doc.createTextNode(idenValue));

					Date date = identifierObj.getEffective_from();
					if (date != null) {
						String action = this.getAction(date);
						elContentItem.setAttribute("action", action);

						DateFormat formatter = new SimpleDateFormat(
								SDIConstants.DATE_FORMAT_SSS);
						elIdenValue.setAttribute("effectiveFrom",
								formatter.format(date));
					}

					date = identifierObj.getEffective_to();
					if (date != null) {
						DateFormat formatter = new SimpleDateFormat(
								SDIConstants.DATE_FORMAT_SSS);
						elIdenValue.setAttribute("effectiveTo",
								formatter.format(date));
					}

					String attrStr = identifierObj.getEffective_to_na_code();
					if (attrStr != null && attrStr.length() > 0) {
						elIdenValue.setAttribute("effectiveToNACode", attrStr);
					}

					Long attrVal = identifierObj.getIdentifier_entity_id();
					if (attrVal != null) {
						elIdenValue.setAttribute("identifierEntityId",
								attrVal.toString());
					}

					attrVal = identifierObj.getIdentifier_entity_type_id();
					if (attrVal != null) {
						elIdenValue.setAttribute("identifierEntityTypeId",
								attrVal.toString());
					}

					attrVal = identifierObj.getIdentifier_type_id();
					if (attrVal != null) {
						elIdenValue.setAttribute("identifierTypeId",
								attrVal.toString());
					}

					attrStr = identifierObj.getIdentifier_type_code();
					if (attrStr != null && attrStr.length() > 0) {
						elIdenValue.setAttribute("identifierTypeCode", attrStr);
					}
					elIdentifier.appendChild(elIdenValue);
				}

				try {
					content = util.getDocumentAsXml(doc);
					content = content.replaceAll("<env:Data type=",
							"<env:Data xsi:type=");
				} catch (TransformerException e) {
					SDIConstants.SDILogger.error(
									"Identifier_v4 SDI: "
											+ FileName
											+ ": Failed to transform Identifier node to String: "
											+ elContentItem.getTextContent());
					e.printStackTrace();
				}
			}
		}
		return content;
	}

	@Override
	protected String getHeader() {
		String envolope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" "
				+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
				+ " xmlns=\"http://data.schemas.financial.thomsonreuters.com/metadata/2010-10-10/\" "
				+ " xsi:schemaLocation=\"http://data.schemas.financial.thomsonreuters.com/metadata/2010-10-10/ @XsdType@\"  pubStyle=\"@PubType@\" majVers=\"3\" minVers=\"1.0\">\r\n";

		String header = envolope
				+ "	<env:Header>\r\n"
				+ "		<env:Info>\r\n"
				+ "			<env:Id>urn:uuid:@GUID@</env:Id>\r\n"
				+ "			<env:TimeStamp>@TIMESTAMP@</env:TimeStamp>\r\n"
				+ "		</env:Info>\r\n"
				+ "</env:Header>\r\n"
				+ "<env:Body contentSet=\"@assetType@\" majVers=\"4\" minVers=\"0.0\">\r\n";

		String uuid = UUID.randomUUID().toString();
		header = header.replace("@GUID@", uuid);
		Date XMLTimestamp = new Date();// yyyy-MM-dd HH:mm:ss.SSS
		DateFormat dataFormat = new SimpleDateFormat(
				SDIConstants.DATE_FORMAT_SSS, Locale.getDefault());

		String strDate = dataFormat.format(XMLTimestamp);
		header = header.replace("@TIMESTAMP@", strDate);
		if (this.IsFullLoad()) {
			header = header.replace("@PubType@", "FullRebuild");
		} else {
			header = header.replace("@PubType@", "Incremental");
		}

		header = header.replace("@XsdType@", "IdentifierDataItem.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

	@Override
	protected String getFooter() {
		return "		</env:Body>\r\n" + "</env:ContentEnvelope>";
	}

	@Override
	public String getDefaultElement() {
		String content = "<env:ContentItem  action=\"Overwrite\">\r\n"
				+ " <env:Data xsi:type=\"IdentifierItem\" />\r\n"
				+ "</env:ContentItem>";
		return content;
	}

}
