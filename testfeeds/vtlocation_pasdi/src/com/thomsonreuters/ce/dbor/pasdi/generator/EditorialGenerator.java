package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;




import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.SDIPreLoadCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstBase;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstLocation;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.AstTypeCommodity;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.EditorialRCS;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GeographicUnit_EditorialRCS;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.Geographic_Unit;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.queue.Pipe;

public class EditorialGenerator extends GZIPSDIFileGenerator {

//	private String assetId;
	private SDIPreLoadCache SDIPLC = null;
	private Utility util;

	public EditorialGenerator(final String sdiFileLocation,
			final String fileName, final Date sdiStartTime,
			final Date sdiEndTime, final int Thread_Num,
			final Pipe<HashMap<CursorType, SDICursorRow[]>> InPort,
			final Counter cursorCounter, final SDIPreLoadCache SDIPLC) {
		super(sdiFileLocation, fileName, sdiStartTime, sdiEndTime, Thread_Num,
				InPort, cursorCounter);
		this.SDIPLC = SDIPLC;
		util = new Utility();
	}

	@Override
	protected String convertContentItem(
			Map<CursorType, SDICursorRow[]> mapCurRows) {
		String result = "";
		String assetId = "";
		try {
			SDICursorRow[] lstRow = mapCurRows.get(CursorType.AST_BASE_INFO);
			AstBase baseRow = (AstBase) lstRow[0];
			assetId = String.valueOf(baseRow.getPerm_ID());
			String adminStatus = baseRow.getAdmin_status();
			// if adminStatus is "Obsolete", won't populate it.
			// if (adminStatus != null &&
			// !adminStatus.equalsIgnoreCase("Obsolete")){
			if (adminStatus != null) {
				result = generateAssetContent(mapCurRows);
			}
		} catch (Exception e) {
			String msg = "File Name:"+this.FileName+"->permId:"+ assetId + "->Unknown Exception";
			if (e.getMessage() != null){
				msg = "File Name:"+this.FileName+"->permId:"+ assetId + "->" + e.getMessage();
			}
			SDIConstants.SDILogger.error( msg);
			e.printStackTrace();
		}
		return result;
	}

	private String generateAssetContent(
			Map<CursorType, SDICursorRow[]> hmCTypeRows) {
		Document doc;
		try {
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		} catch (ParserConfigurationException e1) {
			// TODO Auto-generated catch block
			throw new SystemException(e1);
		}
		doc.setStrictErrorChecking(false);

		String assetCategory = "";
		String assetTypeRCS = "";
		// String commdityTypeRCS = "";

		// AssetBaseInfo
		AstBase baseRow = (AstBase) hmCTypeRows.get(CursorType.AST_BASE_INFO)[0];	
		String assetId = String.valueOf(baseRow.getPerm_ID());
		Date entityCreatedDate = baseRow.getEntity_created_date();
		String assetType = baseRow.getPas_type();

		assetCategory = baseRow.getAsset_category();
		assetTypeRCS = baseRow.getAsset_type_rcs();

		// AstTypeCommodity
		SDICursorRow[] lstATCRow = hmCTypeRows
				.get(CursorType.AST_TYPE_COMMODITY_INFO);

		List<String> lstComTypeRCS = new ArrayList<String>();
		if (lstATCRow != null) {
			AstTypeCommodity atcRow = null;
			for (int i = 0; i < lstATCRow.length; i++) {
				atcRow = (AstTypeCommodity) lstATCRow[i];
				lstComTypeRCS.add(atcRow.getCommodity_typ_rcs());
			}
		}

		Element elContentItem = doc.createElement("env:ContentItem");
		doc.appendChild(elContentItem);
		// Set action value
		String action = this.getAction(entityCreatedDate);
		elContentItem.setAttribute("action", action);

		Element elData = doc.createElement("env:Data");
		elContentItem.appendChild(elData);
		elData.setAttribute("type", "Editorial");

		Element elEdt = doc.createElement("Editorial");
		elData.appendChild(elEdt);

		Element eAssetId = doc.createElement("AssetID");
		eAssetId.appendChild(doc.createTextNode(assetId));
		elEdt.appendChild(eAssetId);

		// Generate EditorialAssetType
		if (assetCategory != null && assetTypeRCS != null)
			generateEditorialAssetTypeSec(assetId, elEdt, assetCategory,
					assetTypeRCS);
		if (lstComTypeRCS.size() > 0)
			generateEditorialCommodityType(assetId, elEdt, assetCategory,
					lstComTypeRCS);

		// GeographicUnit
		if (assetType != null && !assetType.equalsIgnoreCase("VESSEL")) {
			generateGeographicUnit(assetId, elEdt, hmCTypeRows, assetCategory);
		}
		
		String content = "";
		try {
			content = util.getDocumentAsXml(doc);
		} catch (TransformerException e) {
			SDIConstants.SDILogger.error( "File Name:"+this.FileName+"->permId:"
					+ assetId + "->TransformerException: Failed to transform document to string");
			e.printStackTrace();
		}		
		content = content.replaceAll("<env:Data type=", "<env:Data xsi:type=");

		return content;
	}

	// Generate EditorialAssetType
	private void generateEditorialAssetTypeSec(String assetId, Element elEdt,
			String assetCategory, String assetTypeRCS) {
		Document doc = elEdt.getOwnerDocument();

		EditorialRCS[] arrEtItems = null;
		if (assetCategory.equalsIgnoreCase("Vessels")) {
			arrEtItems = SDIPLC.getEditorialRCS(assetCategory, assetTypeRCS,
					"EditorialCommodityType");

			if (arrEtItems != null && arrEtItems.length > 0) {
				for (int i = 0; i < arrEtItems.length; i++) {
					Element elAssetType = doc
							.createElement("EditorialCommodityType");
					Element item = null;
					if (arrEtItems[i].getTarget_id() != null) {
						item = doc.createElement("EditorialCommodityTypeID");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_id().toString()));
						elAssetType.appendChild(item);
					}
					if (arrEtItems[i].getTarget_rcs_code() != null
							&& arrEtItems[i].getTarget_rcs_code().length() > 0) {
						item = doc.createElement("EditorialCommodityTypeRCS");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_rcs_code()));
						elAssetType.appendChild(item);
					}
					if (elAssetType.hasChildNodes()) {
						elEdt.appendChild(elAssetType);
					}
				}
			}

			// Special gun
			arrEtItems = SDIPLC.getSpecialEditorialRCS(Long.valueOf(assetId),
					"EditorialCommodityType");
			if (arrEtItems != null && arrEtItems.length > 0) {
				for (int i = 0; i < arrEtItems.length; i++) {
					Element elAssetType = doc
							.createElement("EditorialCommodityType");
					Element item = null;
					if (arrEtItems[i].getTarget_id() != null) {
						item = doc.createElement("EditorialCommodityTypeID");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_id().toString()));
						elAssetType.appendChild(item);
					}
					if (arrEtItems[i].getTarget_rcs_code() != null
							&& arrEtItems[i].getTarget_rcs_code().length() > 0) {
						item = doc.createElement("EditorialCommodityTypeRCS");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_rcs_code()));
						elAssetType.appendChild(item);
					}
					if (elAssetType.hasChildNodes()) {
						elEdt.appendChild(elAssetType);
					}
				}
			}

		} else {
			arrEtItems = SDIPLC.getEditorialRCS(assetCategory, assetTypeRCS,
					"EditorialAssetType");

			if (arrEtItems != null && arrEtItems.length > 0) {
				for (int i = 0; i < arrEtItems.length; i++) {
					Element elAssetType = doc
							.createElement("EditorialAssetType");
					Element item = null;
					if (arrEtItems[i].getTarget_id() != null) {
						item = doc.createElement("EditorialAssetTypeID");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_id().toString()));
						elAssetType.appendChild(item);
					}
					if (arrEtItems[i].getTarget_rcs_code() != null
							&& arrEtItems[i].getTarget_rcs_code().length() > 0) {
						item = doc.createElement("EditorialAssetTypeRCS");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_rcs_code()));
						elAssetType.appendChild(item);
					}
					if (elAssetType.hasChildNodes()) {
						elEdt.appendChild(elAssetType);
					}
				}
			}

			// Special gun
			arrEtItems = SDIPLC.getSpecialEditorialRCS(Long.valueOf(assetId),
					"EditorialAssetType");
			if (arrEtItems != null && arrEtItems.length > 0) {
				for (int i = 0; i < arrEtItems.length; i++) {
					Element elAssetType = doc
							.createElement("EditorialAssetType");
					Element item = null;
					if (arrEtItems[i].getTarget_id() != null) {
						item = doc.createElement("EditorialAssetTypeID");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_id().toString()));
						elAssetType.appendChild(item);
					}
					if (arrEtItems[i].getTarget_rcs_code() != null
							&& arrEtItems[i].getTarget_rcs_code().length() > 0) {
						item = doc.createElement("EditorialAssetTypeRCS");
						item.appendChild(doc.createTextNode(arrEtItems[i]
								.getTarget_rcs_code()));
						elAssetType.appendChild(item);
					}
					if (elAssetType.hasChildNodes()) {
						elEdt.appendChild(elAssetType);
					}
				}
			}
		}
	}

	// Generate EditorialCommodityType
	private void generateEditorialCommodityType(String assetId, Element elEdt,
			String assetCategory, List<String> lstComTypeRCS) {
		Document doc = elEdt.getOwnerDocument();
		EditorialRCS[] arrEtItems = SDIPLC.getEditorialRCS(assetCategory,
				lstComTypeRCS.get(0), "EditorialCommodityType");

		if (arrEtItems != null && arrEtItems.length > 0) {
			for (int i = 0; i < arrEtItems.length; i++) {
				Element elAssetType = doc
						.createElement("EditorialCommodityType");
				Element item = null;
				if (arrEtItems[i].getTarget_id() != null) {
					item = doc.createElement("EditorialCommodityTypeID");
					item.appendChild(doc.createTextNode(arrEtItems[i]
							.getTarget_id().toString()));
					elAssetType.appendChild(item);
				}
				if (arrEtItems[i].getTarget_rcs_code() != null
						&& arrEtItems[i].getTarget_rcs_code().length() > 0) {
					item = doc.createElement("EditorialCommodityTypeRCS");
					item.appendChild(doc.createTextNode(arrEtItems[i]
							.getTarget_rcs_code()));
					elAssetType.appendChild(item);
				}
				if (elAssetType.hasChildNodes()) {
					elEdt.appendChild(elAssetType);
				}
			}
		}

		// Special gun
		arrEtItems = SDIPLC.getSpecialEditorialRCS(Long.valueOf(assetId),
				"EditorialCommodityType");
		if (arrEtItems != null && arrEtItems.length > 0) {
			for (int i = 0; i < arrEtItems.length; i++) {
				Element elAssetType = doc
						.createElement("EditorialCommodityType");
				Element item = null;
				if (arrEtItems[i].getTarget_id() != null) {
					item = doc.createElement("EditorialCommodityTypeID");
					item.appendChild(doc.createTextNode(arrEtItems[i]
							.getTarget_id().toString()));
					elAssetType.appendChild(item);
				}
				if (arrEtItems[i].getTarget_rcs_code() != null
						&& arrEtItems[i].getTarget_rcs_code().length() > 0) {
					item = doc.createElement("EditorialCommodityTypeRCS");
					item.appendChild(doc.createTextNode(arrEtItems[i]
							.getTarget_rcs_code()));
					elAssetType.appendChild(item);
				}
				if (elAssetType.hasChildNodes()) {
					elEdt.appendChild(elAssetType);
				}
			}
		}
	}

	// Generate GeographicUnit
	private void generateGeographicUnit(String assetId, Element elEdt,
			Map<CursorType, SDICursorRow[]> hmCTypeRows, String assetCategory) {
		Document doc = elEdt.getOwnerDocument();

		SDICursorRow[] arrCursorRow = hmCTypeRows
				.get(CursorType.AST_LOCATION_INFO);
		if (arrCursorRow != null) {
			for (int i = 0; i < arrCursorRow.length; i++) {
				AstLocation loc = (AstLocation) arrCursorRow[i];
				if (loc.getGun_id() != null
						&& loc.getHierarchy_classification_id() != null) {
					Geographic_Unit gu = SDIPLC.universe_gun_detail_info
							.get(loc.getGun_id());

					// get editorial mapping of sub geographic unit
					EditorialRCS[] arrEtItems = SDIPLC.getEditorialRCS(
							assetCategory, gu.getRcs_code(),
							"EditorialGeographicUnit");
					if (arrEtItems != null && arrEtItems.length > 0) {
						Element elEditorialGeoUnit = doc
								.createElement("EditorialGeographicUnit");
						for (int k = 0; k < arrEtItems.length; k++) {
							if (arrEtItems[k].getTarget_id() != null) {
								Element item = doc
										.createElement("EditorialGeographicUnit");
								item.appendChild(doc
										.createTextNode(arrEtItems[k]
												.getTarget_id().toString()));
								elEditorialGeoUnit.appendChild(item);
							}
							if (arrEtItems[k].getTarget_rcs_code() != null
									&& arrEtItems[k].getTarget_rcs_code()
											.length() > 0) {
								Element item = doc
										.createElement("EditorialGeographicUnitRCS");
								item.appendChild(doc
										.createTextNode(arrEtItems[k]
												.getTarget_rcs_code()));
								elEditorialGeoUnit.appendChild(item);
							}

							if (elEditorialGeoUnit.hasChildNodes()) {
								elEdt.appendChild(elEditorialGeoUnit);
							}
						}
					}

					// get editorial mapping of parent geographic unit
					GeographicUnit_EditorialRCS[] arrGE = gu.getGunEditorialRCS(loc.getHierarchy_classification_id());
					if (arrGE != null && arrGE.length > 0) {
						for (int j = 0; j < arrGE.length; j++) {
							
							Element elGeoUnit = doc
									.createElement("EditorialGeographicUnit");
							Object vo = arrGE[j].getTarget_id();
							if (vo != null && vo.toString().length() > 0) {
								Element item = doc
										.createElement("EditorialGeographicUnitID");
								item.appendChild(doc
										.createTextNode(vo.toString()));
								elGeoUnit.appendChild(item);
							}
							
							vo = arrGE[j].getTarget_rcs_code();
							if (vo != null && vo.toString().length() > 0) {
								Element item = doc
										.createElement("EditorialGeographicUnitRCS");
								item.appendChild(doc
										.createTextNode(vo.toString()));
								elGeoUnit.appendChild(item);
							}
							if (elGeoUnit.hasChildNodes()) {
								elEdt.appendChild(elGeoUnit);
							}
							
							if(!gu.getRcs_code().equalsIgnoreCase(arrGE[j].getTarget_rcs_code())){
								arrEtItems = SDIPLC.getEditorialRCS(assetCategory, vo.toString(),"EditorialGeographicUnit");
								if (arrEtItems != null && arrEtItems.length > 0) {
									Element elEditorialGeoUnit = doc
											.createElement("EditorialGeographicUnit");
									for (int k = 0; k < arrEtItems.length; k++) {
										if (arrEtItems[k].getTarget_id() != null) {
											Element item = doc
													.createElement("EditorialGeographicUnit");
											item.appendChild(doc
													.createTextNode(arrEtItems[k]
															.getTarget_id()
															.toString()));
											elEditorialGeoUnit.appendChild(item);
										}
										if (arrEtItems[k].getTarget_rcs_code() != null
												&& arrEtItems[k]
														.getTarget_rcs_code()
														.length() > 0) {
											Element item = doc
													.createElement("EditorialGeographicUnitRCS");
											item.appendChild(doc
													.createTextNode(arrEtItems[k]
															.getTarget_rcs_code()));
											elEditorialGeoUnit.appendChild(item);
										}

										if (elEditorialGeoUnit.hasChildNodes()) {
											elEdt.appendChild(elEditorialGeoUnit);
										}
									}
								}
							}
						}
					}

				}
			}
		}

		// Special gun
		EditorialRCS[] arrSpecialEtItems = SDIPLC.getSpecialEditorialRCS(
				Long.valueOf(assetId), "EditorialGeographicUnit");
		if (arrSpecialEtItems != null && arrSpecialEtItems.length > 0) {
			Element elEditorialGeoUnit = doc
					.createElement("EditorialGeographicUnit");
			for (int k = 0; k < arrSpecialEtItems.length; k++) {
				if (arrSpecialEtItems[k].getTarget_id() != null) {
					Element item = doc.createElement("EditorialGeographicUnit");
					item.appendChild(doc.createTextNode(arrSpecialEtItems[k]
							.getTarget_id().toString()));
					elEditorialGeoUnit.appendChild(item);
				}
				if (arrSpecialEtItems[k].getTarget_rcs_code() != null
						&& arrSpecialEtItems[k].getTarget_rcs_code().length() > 0) {
					Element item = doc
							.createElement("EditorialGeographicUnitRCS");
					item.appendChild(doc.createTextNode(arrSpecialEtItems[k]
							.getTarget_rcs_code()));
					elEditorialGeoUnit.appendChild(item);
				}

				if (elEditorialGeoUnit.hasChildNodes()) {
					elEdt.appendChild(elEditorialGeoUnit);
				}
			}
		}

	}

	@Override
	public String getDefaultElement() {
		return "<env:ContentItem  action=\"Overwrite\">\r\n"
				+ " <env:Data xsi:type=\"Editorial\" />\r\n"
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
		header = header.replace("@XsdType@", "Editorial.xsd");
		header = header.replace("@assetType@", "EditorialType");

		return header;
	}

}
