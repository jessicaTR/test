package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilderFactory;
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




import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.SDIPreLoadCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.RELATION_OBJECT_ID_TYPE;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.util.Utility;
import com.thomsonreuters.ce.queue.DiskCache;

public class RelationshipGenerator implements Runnable {

	private SDIPreLoadCache sdiPLC = null;
	private Counter count;
	private String fileLocation;
	private String fileName;

	protected Date CoverageStart;
	protected Date CoverageEnd;
	BufferedWriter writer = null;
	
	private static final ThreadLocal<Transformer> safeTrans = new ThreadLocal<Transformer>() {
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
								"Identifier_v4 SDI: Failed to create Transformer in thread local");
				e.printStackTrace();
			}
			return transformer;
		}
	};
	
	public RelationshipGenerator(String fileLocation, String fileName, Date startTime, Date endTime,
			SDIPreLoadCache sdiplc, Counter counter) {
		this.fileLocation = fileLocation;
		this.fileName = fileName;
		this.CoverageStart = startTime;
		this.CoverageEnd = endTime;	
		sdiPLC = sdiplc;
		
		this.count = counter;
		this.count.Increase();
	}

	protected void writeContent() {
		String content = "";
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();			
			doc.setStrictErrorChecking(false);
			
			Element elContentItem, elData, elRelation;
			DiskCache<RELATION_OBJECT_ID_TYPE> relationCache = this.sdiPLC.AssetRelations;
			RELATION_OBJECT_ID_TYPE relation;
			while (relationCache.HasNext()) {
				elContentItem = doc.createElement("env:ContentItem");				
//				elContentItem.setAttribute("action", action);
//				doc.appendChild(elContentItem);
				elData = doc.createElement("env:Data");
				elData.setAttribute("type", "RelationshipItem");
				elContentItem.appendChild(elData);				
				elRelation = doc.createElement("Relationship");
				elData.appendChild(elRelation);
				relation = relationCache.GetNext();				
				Long objectId = relation.getObject_id();
				if (objectId != null){					
					Element elObjectId = doc.createElement("RelationObjectId");
					elObjectId.appendChild(doc.createTextNode(objectId.toString()));
					
					Date date = relation.getEffective_from(); 
					if (date != null){
						String action = this.getAction(date);
						elContentItem.setAttribute("action", action);
						
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSS);
						elObjectId.setAttribute("effectiveFrom", formatter.format(date));
					}
					
					date = relation.getEffective_to(); 
					if (date != null){
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSS);
						elObjectId.setAttribute("effectiveTo", formatter.format(date));
					}
					
					Long val;
					String str;
					val = relation.getRelated_object_id();
					if (val != null){
						elObjectId.setAttribute("relatedObjectId", val.toString());
					}
					
					val = relation.getRelated_object_order();
					if (val != null){
						elObjectId.setAttribute("relatedObjectId", val.toString());
					}
					
					str = relation.getRelated_object_type();
					if (str != null){
						elObjectId.setAttribute("relatedObjectType", str.toString());
					}
					
					val = relation.getRelated_object_type_id();
					if (val != null){
						elObjectId.setAttribute("relatedObjectTypeId", val.toString());
					}
					
					str = relation.getRelation_object_na_code();
					if (str != null){
						elObjectId.setAttribute("relationObjectNACode", str.toString());
					}
					
					val = relation.getRelation_object_order();
					if (val != null){
						elObjectId.setAttribute("relationObjectOrder", val.toString());
					}

					str = relation.getRelation_object_type();
					if (str != null){
						elObjectId.setAttribute("relationObjectType", str.toString());
					}
										
					val = relation.getRelation_object_type_id();
					if (val != null){
						elObjectId.setAttribute("relationObjectTypeId", val.toString());
					}
					
					val = relation.getRelation_role();
					if (val != null){
						elObjectId.setAttribute("relationRole", val.toString());
					}
					
					Float confidence = relation.getRelationship_confidence();
					if (confidence != null){
						elObjectId.setAttribute("relationshipConfidence", confidence.toString());
					}
					
					val = relation.getRelationship_id();
					if (val != null){
						elObjectId.setAttribute("relationshipId", val.toString());
					}
					
					str = relation.getRelationship_type();
					if (str != null){
						elObjectId.setAttribute("relationshipType", str.toString());
					}
										
					val = relation.getRelationship_type_id();
					if (val != null){
						elObjectId.setAttribute("relationshipTypeId", val.toString());
					}
					
					elRelation.appendChild(elObjectId);
				}
				
				try {
					content = getNodeAsString(elContentItem);
					content = content.replaceAll("<env:Data type=",
							"<env:Data xsi:type=");
					writer.write(content);
				} catch (TransformerException e) {
					SDIConstants.SDILogger.error(
									"Relationship SDI: "
											+ fileName
											+ ": Failed to transform Identifier node to String: "
											+ elContentItem.getTextContent());
					e.printStackTrace();
				} catch (IOException e) {
					SDIConstants.SDILogger.error( "Relationship SDI: "
							+ fileName + ": Failed to write content: "
							+ content);
					e.printStackTrace();
				}	
			}
			
			if (content.length() == 0) {
				content = "<env:ContentItem  action=\"Overwrite\">\r\n"
						 + " <env:Data xsi:type=\"RelationshipItem\" />\r\n"
						 + "</env:ContentItem>";
				try {
					writer.write(content);
				} catch (IOException e) {
					SDIConstants.SDILogger.error( "Identifier_v4 SDI: "
							+ fileName + ": Failed to write content: " + content);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {

		File thisTempFile = new File(this.fileLocation, this.fileName + ".temp");

		// Delete temp file if it exists
		if (thisTempFile.exists() == true) {
			thisTempFile.delete();
		}

		try {
			thisTempFile.createNewFile();

			GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(
					thisTempFile));
			writer = new BufferedWriter(new OutputStreamWriter(
					zfile, "UTF8"));

			writer.write(getHeader());
			writer.flush();

			writeContent();

			writer.write(getFooter());
			writer.flush();
			writer.close();

			File thisFile = new File(this.fileLocation, this.fileName);

			// Delete temp file if it exists
			if (thisFile.exists() == true) {
				thisFile.delete();
			}

			this.moveFile(thisTempFile, thisFile);

			SDIConstants.SDILogger.info( "SDI File: " + this.fileName
					+ " has been generated");

		} catch (FileNotFoundException e) {
			SDIConstants.SDILogger.error( "FileNotFoundException", e);
		} catch (UnsupportedEncodingException e) {
			SDIConstants.SDILogger.error( "UnsupportedEncodingException", e);
		} catch (IOException e) {
			SDIConstants.SDILogger.error( "IOException: Faild to write content to local file", e);
		} finally {
			this.count.Decrease();
		}
	}
	
	protected String getHeader() {
		String envolope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" " 
				+ " xmlns=\"http://data.schemas.financial.thomsonreuters.com/metadata/2010-10-10/\" " 
				+ " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
				+ " xsi:schemaLocation=\"http://data.schemas.financial.thomsonreuters.com/metadata/2010-10-10/ @XsdType@\"  pubStyle=\"@PubType@\" majVers=\"3\" minVers=\"1.0\">\r\n";

		String header = envolope
				+ "	<env:Header>\r\n"
				+ "		<env:Info>\r\n"
				+ "			<env:Id>urn:uuid:@GUID@</env:Id>\r\n"
				+ "			<env:TimeStamp>@TIMESTAMP@</env:TimeStamp>\r\n"
				+ "		</env:Info>\r\n"
				+ "</env:Header>\r\n"
				+ "<env:Body contentSet=\"@assetType@\" majVers=\"4\" minVers=\"2.0\">\r\n";

		String uuid = UUID.randomUUID().toString();
		header = header.replace("@GUID@", uuid);
		Date XMLTimestamp = new Date();// yyyy-MM-dd HH:mm:ss.SSS
		DateFormat dataFormat = new SimpleDateFormat(
				SDIConstants.DATE_FORMAT_SSS, Locale.getDefault());

		String strDate = dataFormat.format(XMLTimestamp);
		header = header.replace("@TIMESTAMP@", strDate);
		if (this.CoverageStart == null) {
			header = header.replace("@PubType@", "FullRebuild");
		} else {
			header = header.replace("@PubType@", "Incremental");
		}

		header = header.replace("@XsdType@", "RelationshipDataItem.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

	private String getFooter() {
		return "		</env:Body>\r\n" + "</env:ContentEnvelope>";
	}

	public void moveFile(File f1, File f2) {

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
		} catch (FileNotFoundException e) {
			SDIConstants.SDILogger.error("FileNotFoundException",e);
		} catch (IOException e) {
			SDIConstants.SDILogger.error("IOException",e);
		}
	}	
	
		
	public String getAction(Date EntityCreateDate)
	{
		if (CoverageStart==null)
		{
			return "Insert";
		}
		else if (EntityCreateDate.after(CoverageStart))
		{
			return "Insert";
		}
		else
		{
			return "Overwrite";
		}
	}
	
	private String getNodeAsString(Node node) throws TransformerException {
		DOMSource source = new DOMSource(node);
		StringWriter writer = new StringWriter();
		StreamResult sr = new StreamResult(writer);
		safeTrans.get().transform(source, sr);
		return writer.toString();
	}
}
