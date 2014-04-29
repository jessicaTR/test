/**
 * 
 */
package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;




import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.SDIPreLoadCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.GenericCursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericCfgOutput;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericMetaDataEleDetail;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericMetaDataGroupDetail;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericSDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.PERMANENT_ID_IDENTIFIER_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.queue.Pipe;

/**
 * @author lei.yang
 * 
 */
public class GenericAssetGenerator extends GZIPSDIFileGenerator {

	private SDIPreLoadCache sdiPLC;
	private Utility util;
	
	public GenericAssetGenerator(final String sdiFileLocation,
			final String physicalAssetName, final Date sdiStartTime,
			final Date sdiEndTime, final int Thread_Num,
			final Pipe<HashMap<CursorType, SDICursorRow[]>> InPort,
			final Counter cursorCounter, final SDIPreLoadCache SDIPLC) {
		super(sdiFileLocation, physicalAssetName, sdiStartTime, sdiEndTime,
				Thread_Num, InPort, cursorCounter);
		sdiPLC = SDIPLC;
		util = new Utility();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thomsonreuters.ce.taskcontroller.task.file.searchsdiv2.generator.
	 * GZIPSDIFileGenerator#convertContentItem(java.util.Map)
	 */
	@Override
	protected String convertContentItem(
			final Map<CursorType, SDICursorRow[]> hmCTypeRows) {
		// GenericCursorType(key):
		// GenericCursorType.GetGenericCursorType("generic_data_ags_facility")
		// loop arr[],
		// 1 asset-->
		String content = "";
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			doc.setStrictErrorChecking(false);

			SDICursorRow[] lstBaseRows = hmCTypeRows
					.get(CursorType.AST_BASE_INFO);
			AstBase baseRow = (AstBase) lstBaseRows[0];
			
			String assetPasType = baseRow.getPas_type();

			Date createdDate = baseRow.getEntity_created_date();

			Element elContentItem = doc.createElement("env:ContentItem");
			String action = this.getAction(createdDate);
			elContentItem.setAttribute("action", action);
			doc.appendChild(elContentItem);

			Element elData = doc.createElement("env:Data");
			elData.setAttribute("type", "GenericAsset");
			elContentItem.appendChild(elData);

			Element elGA = doc.createElement("GenericAsset");
			elData.appendChild(elGA);

			Element elAssetId = doc.createElement("AssetID");
			PERMANENT_ID_IDENTIFIER_TYPE objAssetId = baseRow.getCne_asset_id();
			if (objAssetId.getObject_type_id() != null && objAssetId.getObject_type_id().toString().length() > 0){
				elAssetId.setAttribute("objectTypeId", objAssetId.getObject_type_id().toString());
			}
			if (objAssetId.getObject_type() != null && objAssetId.getObject_type().toString().length() > 0){
				elAssetId.setAttribute("objectType", objAssetId.getObject_type().toString());
			}
			if (objAssetId.getObject_id() != null){
				elAssetId.appendChild(doc.createTextNode(objAssetId.getObject_id().toString()));				
			}
			elGA.appendChild(elAssetId);
			
			Iterator<Entry<String, GenericCfgOutput[]>> iterator = sdiPLC.generic_cfg_output_list
					.entrySet().iterator();

			while (iterator.hasNext()) {
				Entry<String, GenericCfgOutput[]> entry = (Entry<String, GenericCfgOutput[]>) iterator
						.next();
				String mataPasType = entry.getKey();
				if (!assetPasType.equalsIgnoreCase(mataPasType)) {
					continue;
				}
				GenericCfgOutput[] cfgs = entry.getValue();
				for (int i = 0; i < cfgs.length; i++) {
					String isGroup = cfgs[i].getIs_group();
					Long itemId = cfgs[i].getItem_id();

					if (isGroup.equalsIgnoreCase("N")) {
						GenericMetaDataEleDetail metadataDetail = sdiPLC.generic_metadata_ele_detail_info
								.get(itemId);
						String tablename = metadataDetail.getTable_name();
						String colname = metadataDetail.getColumn_name();
						CursorType curType = GenericCursorType
								.GetGenericCursorType(tablename);
						SDICursorRow[] sdiRows = hmCTypeRows.get(curType);
						
						if (sdiRows != null) {
							for (int j = 0; j < sdiRows.length; j++) {
								GenericSDICursorRow gRow = (GenericSDICursorRow) sdiRows[j];

								String value = "";
								Object obj = gRow.getObject(colname);
								if (obj != null && obj.toString().length() > 0) {
									String objType = metadataDetail
											.getGAMetadataDataType();
									String metadataId = metadataDetail
											.getGAMetadataID();
									if (objType.equalsIgnoreCase("xs:datetime")) {
										DateFormat formatter1 = new SimpleDateFormat(
												SDIConstants.DATE_FORMAT_SSS);
										value = formatter1.format((Date) obj);
									} else {
										value = String.valueOf(obj);
									}

									Element gaData = doc
											.createElement("GAData");
									Element item = doc
											.createElement("GAMetadataID");
									item.setAttribute("languageId", "505062");
									item.appendChild(doc
											.createTextNode(metadataId));
									gaData.appendChild(item);

									item = doc.createElement("GAValue");
									item.appendChild(doc.createTextNode(value));									
									gaData.appendChild(item);

									elGA.appendChild(gaData);
								}
							}
						}

					} else {
						Map<String, String> mapFilter = new HashMap<String, String>();
						GenericMetaDataGroupDetail[] groupDetails = sdiPLC.generic_metadata_group_detail_info
								.get(itemId);
						for (GenericMetaDataGroupDetail detail : groupDetails) {
							if (detail.getGAMetadataID() != null
									&& detail.getGAMetadataEnumerationValue() != null) {
								mapFilter.put(detail.getGAMetadataID(),
										detail.getGAMetadataEnumerationValue());
							}
						}
						String tablename = groupDetails[0].getTable_name();
						String groupId = groupDetails[0].getGAGroupID();

						CursorType curType = GenericCursorType
								.GetGenericCursorType(tablename);
						SDICursorRow[] sdiRows = hmCTypeRows.get(curType);

						if (sdiRows != null) {

							boolean valid = true;
							for (int j = 0; j < sdiRows.length; j++) {
								Element elGAGroup = doc
										.createElement("GAGroup");
								Element elGroupId = doc
										.createElement("GAGroupID");
								elGroupId.appendChild(doc
										.createTextNode(groupId));
								elGAGroup.appendChild(elGroupId);

								GenericSDICursorRow gRow = (GenericSDICursorRow) sdiRows[j];

								valid = true;
								Iterator<Entry<String, String>> ite = mapFilter
										.entrySet().iterator();
								while (ite.hasNext()) {
									Entry<String, String> entry1 = ite.next();
									String key = entry1.getKey();
									String value = entry1.getValue();
									if (gRow.getObject(key) == null
											|| (gRow.getObject(key) != null && !(gRow
													.getObject(key).toString()
													.equalsIgnoreCase(value)))) {
										valid = false;
									}
								}

								if (valid) {
									String value = "";
									for (GenericMetaDataGroupDetail detail : groupDetails) {
										String metadataId = detail
												.getGAMetadataID();
										if (metadataId != null) {
											GenericMetaDataEleDetail mDetail = detail.GMED;
											Object obj = gRow
													.getObject(metadataId);
											String objType = mDetail
													.getGAMetadataDataType();

											if (obj == null) {
												continue;
											}

											if (objType
													.equalsIgnoreCase("xs:datetime")) {
												DateFormat formatter2 = new SimpleDateFormat(
														SDIConstants.DATE_FORMAT_SSS);
												value = formatter2
														.format((Date) obj);
											} else {
												value = String.valueOf(obj);
											}

											if (value != null
													&& value.length() > 0) {
												Element elGAData = doc
														.createElement("GAData");
												Element item = doc
														.createElement("GAMetadataID");
												item.appendChild(doc
														.createTextNode(metadataId));
												elGAData.appendChild(item);

												item = doc
														.createElement("GAValue");
												item.appendChild(doc
														.createTextNode(value));
												elGAData.appendChild(item);

												elGAGroup.appendChild(elGAData);
											}
										}
									}
								}
								if (elGAGroup.getChildNodes().getLength() > 1) {
									elGA.appendChild(elGAGroup);
								}
							}
						}
					}
				}
			}
			
			if (elAssetId.getNextSibling() == null){
				return "";
			}

			// String content1 = getDocumentAsXml(doc);
			// Document resultDoc = transformXml(doc, cachedXSLT);
			content = util.getDocumentAsXml(doc);
			content = content.replaceAll("<env:Data type=", "<env:Data xsi:type=");

		} catch (DOMException e) {
			SDIConstants.SDILogger.error( "DOMException", e);
		} catch (SecurityException e) {
			SDIConstants.SDILogger.error( "SecurityException", e);
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "Exception", e);
		}
		return content;
	}

	@Override
	public String getDefaultElement() {
		return "<env:ContentItem  action=\"Overwrite\">\r\n"
				+ " <env:Data xsi:type=\"GenericAsset\" />\r\n"
				+ "</env:ContentItem>";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thomsonreuters.ce.taskcontroller.task.file.searchsdiv2.generator.
	 * GZIPSDIFileGenerator#getFooter()
	 */
	@Override
	protected String getFooter() {
		return "		</env:Body>\r\n" + "</env:ContentEnvelope>";

	}	

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thomsonreuters.ce.taskcontroller.task.file.searchsdiv2.generator.
	 * GZIPSDIFileGenerator#getHeader()
	 */
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

		header = header.replace("@XsdType@", "GenericAsset.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}
}
