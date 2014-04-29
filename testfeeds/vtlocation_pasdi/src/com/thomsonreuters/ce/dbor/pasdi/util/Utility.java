package com.thomsonreuters.ce.dbor.pasdi.util;

import java.io.StringWriter;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;



import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.SDIPreLoadCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.Geographic_Unit;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.UniverseVesZoneDetail;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.VesselEventOriginDestination;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.IDENTIFIER_VALUE_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.PERMANENT_ID_IDENTIFIER_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.RELATION_OBJECT_ID_TYPE;

public class Utility {
	private static boolean isDebug = false;

	public static void print(String msg) {
		if (isDebug) {
			System.out.println(msg);
		}
	}

	public static void print(Exception e, String msg) {
		if (isDebug) {
			System.out.println(msg);
			e.printStackTrace();
		}
	}

	private static final ThreadLocal<Transformer> safeTrans = new ThreadLocal<Transformer>() {
		@Override
		public Transformer initialValue() {
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = null;
			try {
				transformer = tf.newTransformer();
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
								"SDI Generator: Failed to create Transformer in thread local");
				e.printStackTrace();
			}
			return transformer;
		}
	};

	/**
	 * Transform XmlDocument to string
	 * 
	 * @param doc
	 * @return
	 * @throws TransformerException 
	 */
	public String getDocumentAsXml(Document doc) throws TransformerException {

		DOMSource domSource = new DOMSource(doc);
		StringWriter writer = new StringWriter();
		StreamResult sr = new StreamResult(writer);
		safeTrans.get().transform(domSource, sr);
		return writer.toString();
	}

	
	public String getNodeAsString(Node node) throws TransformerException {
		DOMSource source = new DOMSource(node);
		StringWriter writer = new StringWriter();
		StreamResult sr = new StreamResult(writer);
		safeTrans.get().transform(source, sr);
		return writer.toString();
	}

	/**
	 * Process CurRow with common logic
	 * 
	 * @param curClass
	 * @param lstCurRows
	 * @param doc
	 * @param elParent
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	public void processCurRow(final Class<?> curClass,
			final SDICursorRow[] lstCurRows, final Document doc,
			Element elSubRoot, final String parentElementName)
			throws IllegalArgumentException, IllegalAccessException {
		if (lstCurRows != null) {
			Field[] fields = curClass.getDeclaredFields();
			for (int i = 0; i < lstCurRows.length; i++) {
				Element elParent = doc.createElement(parentElementName);
				generateSection(lstCurRows[i], fields, doc, elParent);

				if (elParent.hasChildNodes()) {
					elSubRoot.appendChild(elParent);
				}
			}
		}
	}

	public void generateSection(final Object row, Field[] fields,
			Document doc, Element curBase) throws IllegalArgumentException,
			IllegalAccessException {
		String itemName, value;
		Object vo;
		Element item = null;
		Field[] flds;

		for (Field field : fields) {
			field.setAccessible(true);
			itemName = field.getName();
			vo = field.get(row);

			if (vo != null) {
				item = doc.createElement(itemName);
				if (vo instanceof PERMANENT_ID_IDENTIFIER_TYPE) {
					Long id = ((PERMANENT_ID_IDENTIFIER_TYPE) vo)
							.getObject_id();
					if (id != null && id.toString().length() > 0) {
						flds = PERMANENT_ID_IDENTIFIER_TYPE.class
								.getDeclaredFields();
						setAttr(flds, doc, item, vo);
						curBase.appendChild(item);
					}
				} else if (vo instanceof RELATION_OBJECT_ID_TYPE) {
					Long id = ((RELATION_OBJECT_ID_TYPE) vo).getObject_id();
					if (id != null && id.toString().length() > 0) {
						flds = RELATION_OBJECT_ID_TYPE.class
								.getDeclaredFields();
						setAttr(flds, doc, item, vo);
						curBase.appendChild(item);
					}
				} else if (vo instanceof IDENTIFIER_VALUE_TYPE) {
					Long id = ((IDENTIFIER_VALUE_TYPE) vo).getIdentifier_entity_id();
					if (id != null && id.toString().length() > 0) {
						flds = IDENTIFIER_VALUE_TYPE.class
								.getDeclaredFields();
						setAttr(flds, doc, item, vo);
						curBase.appendChild(item);
					}
				} else if(vo instanceof Collection || vo instanceof Map){
					//TODO: if vo is collection, skip it
				} else {
					if (vo instanceof java.util.Date) {
						DateFormat formatter = new SimpleDateFormat(
								SDIConstants.DATE_FORMAT_SSS);
						value = formatter.format((Date) vo);
					} else if (itemName.equalsIgnoreCase("longitude")
							|| itemName.equalsIgnoreCase("latitude")
							|| itemName.equalsIgnoreCase("statistic_value")) {
						// TODO: For decimal values which exceeded the
						// region
						// and change them from scientific to float
						DecimalFormat decFormat = new DecimalFormat(
								"#################.############");
						Float val = (Float) vo;
						value = decFormat.format(val);
					} else {
						value = vo.toString();
					}
					item.appendChild(doc.createTextNode(value));
					curBase.appendChild(item);
				}

			}
		}
	}

	private void setAttr(Field[] flds, Document doc, Element item, Object vo)
			throws IllegalArgumentException, IllegalAccessException {
		String subItemName, subValue;
		Object subVO;

		for (Field f : flds) {
			f.setAccessible(true);
			subItemName = f.getName();
			subVO = f.get(vo);
			if (subVO != null) {
				if (subItemName.equalsIgnoreCase("object_id")) {
					item.appendChild(doc.createTextNode(subVO.toString()));
				} else {
					if (subVO instanceof java.util.Date) {
						DateFormat formatter = new SimpleDateFormat(
								SDIConstants.DATE_FORMAT_SSS);
						subValue = formatter.format((Date) subVO);
					} else {
						subValue = subVO.toString();
					}
					item.setAttribute(subItemName, subValue);
				}
			}
		}
	}

	public void setGunAttr(Geographic_Unit gun, Document doc, Element item) {
		// set relation attributes
		Object vo = gun.getRelation_object_type();
		if (vo != null)
			item.setAttribute("relationObjectType", vo.toString());

		vo = gun.getRelation_object_type_id();
		if (vo != null)
			item.setAttribute("relationObjectTypeId", vo.toString());

		vo = gun.getRelationship_type();
		if (vo != null)
			item.setAttribute("relationshipType", vo.toString());

		vo = gun.getRelationship_type_id();
		if (vo != null)
			item.setAttribute("relationshipTypeId", vo.toString());

		Element elID = (Element) doc.getElementsByTagName("cne_asset_id").item(
				0);
		if (elID != null) {
			item.setAttribute("relatedObjectId", elID.getTextContent());

			if (elID.getAttribute("object_type") != null) {
				item.setAttribute("relatedObjectType",
						elID.getAttribute("object_type"));
			}
			if (elID.getAttribute("object_type_id") != null) {
				item.setAttribute("relatedObjectTypeId",
						elID.getAttribute("object_type_id"));
			}
		}

		NodeList elLst = doc.getElementsByTagName("effective_from");
		if (elLst != null && elLst.getLength() > 0) {
			Element elEF = (Element) elLst.item(0);
			if (elEF != null)
				item.setAttribute("effectiveFrom", elEF.getTextContent());
		}
	}
	
	public void generateVesselGun(String[] arrElName, VesselEventOriginDestination vEOD, 
			Element elParent, SDIPreLoadCache sdiPLC) throws IllegalArgumentException, IllegalAccessException{
		Document doc = elParent.getOwnerDocument();
		Field[] fields = VesselEventOriginDestination.class.getDeclaredFields();
//		Element elOpenEvent = doc.createElement("VesselGeographicUnit");
		Element element = doc.createElement(arrElName[0]);
		if (vEOD.getZone_id() == null
				|| vEOD.getZone_type() == null) {			
			generateSection(vEOD, fields, doc, element);			
		} else {
			String curName = vEOD.getCur_name();
			Long zoneId = vEOD.getZone_id();
			String zoneType = vEOD.getZone_type();
			
			if (curName
					.equalsIgnoreCase("universe_ves_zone_detail_info")) {
				UniverseVesZoneDetail zoneDetail = sdiPLC
						.getVesselZoneDetail(zoneId, zoneType);
				if (zoneDetail != null) {
					// TODO:					
					generateSection(vEOD, fields, doc, element);
					fields = UniverseVesZoneDetail.class.getDeclaredFields();
					generateSection(zoneDetail, fields, doc, element);					
				}
			} else if (curName
					.equalsIgnoreCase("universe_gun_detail_info")) {
				element = doc.createElement(arrElName[1]);
				generateSection(vEOD, fields, doc, element);
				Geographic_Unit gu = sdiPLC.universe_gun_detail_info
						.get(zoneId);
				if (gu != null){
					fields = Geographic_Unit.class.getDeclaredFields();
					generateSection(gu, fields, doc, element);
				}
			}
		}
		if (element.hasChildNodes()) {
			elParent.appendChild(element);
		}
	}


	// public static ThreadLocal<Transformer> getSafetransformer() {
	// return safeTransformer;
	// }
}
