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
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstIdentifier;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.queue.Pipe;

public class IdentifierGenerator extends GZIPSDIFileGenerator {
	private Utility util;

	public IdentifierGenerator(String fileLocation, String fileName, Date startTime, Date endTime,
			int threadNum, Pipe<HashMap<CursorType, SDICursorRow[]>> pipeIn,
			Counter ct) {
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
			Map<CursorType, SDICursorRow[]> hmCTypeRows) {

		String content = "";
		AstBase baseRow = (AstBase) hmCTypeRows.get(CursorType.AST_BASE_INFO)[0];

		Document doc;
		try {
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		} catch (ParserConfigurationException e1) {
			// TODO Auto-generated catch block
			throw new SystemException(e1);
		}
		doc.setStrictErrorChecking(false);

		Date entityCreatedDate = baseRow.getEntity_created_date();
		String action = this.getAction(entityCreatedDate);

		Date effectiveFrom = baseRow.getEffective_from();

		SDICursorRow[] arrIden = hmCTypeRows
				.get(CursorType.AST_IDENTIFIER_INFO);
		if (arrIden != null) {
			for (SDICursorRow iden : arrIden) {
				AstIdentifier identifierRow = (AstIdentifier) iden;
				String permId = String.valueOf(identifierRow.getPerm_ID());
				String typeCode = identifierRow.getIdentifier_name();
				String typeValue = identifierRow.getIdentifier_id();

				Element elContentItem = doc.createElement("env:ContentItem");
				doc.appendChild(elContentItem);
				elContentItem.setAttribute("action", action);

				Element elData = doc.createElement("env:Data");
				elContentItem.appendChild(elData);
				elData.setAttribute("type", "IdentifierItem");

				Element elIdentifier = doc.createElement("Identifier");
				if (effectiveFrom != null) {
					DateFormat formatter = new SimpleDateFormat(
							SDIConstants.DATE_FORMAT_SSS);
					String dataValue = formatter.format(effectiveFrom);
					elIdentifier.setAttribute("effectiveFrom", dataValue);
				}
				elData.appendChild(elIdentifier);

				Element entityId = doc.createElement("IdentifierEntityId");
				entityId.setAttribute("identifierEntityType", "PhysicalAsset");
				entityId.appendChild(doc.createTextNode(permId));
				elIdentifier.appendChild(entityId);

				if (typeCode != null && typeCode.length() > 0) {
					Element elTypeCode = doc
							.createElement("IdentifierTypeCode");
					elTypeCode.appendChild(doc.createTextNode(typeCode));
					elIdentifier.appendChild(elTypeCode);
				}

				if (typeValue != null && typeValue.length() > 0) {
					Element value = doc.createElement("IdentifierValue");
					value.appendChild(doc.createTextNode(typeValue));
					elIdentifier.appendChild(value);
				}
			}

			try {
				content = util.getDocumentAsXml(doc);
			} catch (TransformerException e) {
				SDIConstants.SDILogger.error( "Identifier_V2: Failed to transform document to string, the permId is:  " + doc.getElementsByTagName("IdentifierEntityId").item(0).getTextContent());
				e.printStackTrace();
			}
			content = content.replaceAll("<env:Data type=",
					"<env:Data xsi:type=");
		}
		return content;
	}

	@Override
	public String getDefaultElement() {
		 return "<env:ContentItem  action=\"Overwrite\">\r\n"
		 + " <env:Data xsi:type=\"IdentifierItem\" />\r\n"
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
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" "
				+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
				+ "xmlns=\"http://data.schemas.financial.thomsonreuters.com/metadata/2009-09-01/\" "
				+ "pubStyle=\"@PubType@\" majVers=\"3\" minVers=\"1.0\" "
				+ "xsi:schemaLocation=\"http://data.schemas.financial.thomsonreuters.com/metadata/2009-09-01/ IdentifierDataItem.xsd\"> ";

		String header = envolope
				+ "	<env:Header>\r\n"
				+ "		<env:Info>\r\n"
				+ "			<env:Id>urn:uuid:@GUID@</env:Id>\r\n"
				+ "			<env:TimeStamp>@TIMESTAMP@</env:TimeStamp>\r\n"
				+ "		</env:Info>\r\n"
				+ "</env:Header>\r\n"
				+ "<env:Body contentSet=\"@assetType@\" majVers=\"3\" minVers=\"1.0\">\r\n";

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
		header = header.replace("@XsdType@", "IdentifierDataItem.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

}
