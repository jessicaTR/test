package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;



import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.SDIPreLoadCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericMetaDataEleDetail;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;

public class GenericAssetMetadataGenerator implements Runnable {

	private SDIPreLoadCache SDIPLC = null;
	private Counter count;
	protected String Location;
	protected String FileName;
	
	protected Date CoverageStart;
	protected Date CoverageEnd;
	
	public GenericAssetMetadataGenerator(String fileLocation, String fileName,
			Date startTime, Date endTime, SDIPreLoadCache sdiplc, Counter counter) {	
		this.Location=fileLocation;
		this.FileName=fileName;
		this.CoverageStart=startTime;
		this.CoverageEnd=endTime;		
		this.SDIPLC=sdiplc;	
		
		this.count = counter;
		this.count.Increase();
	}
	
	public boolean IsFullLoad()
	{
		if (CoverageStart==null)
		{
			return true;
		}
		else
		{
			return false;
		}		
	}	
	
	public Date getCoverageEnd() {
		return CoverageEnd;
	}

	public Date getCoverageStart() {
		return CoverageStart;
	}
	
	public void run() {		
		File thisTempFile = new File(this.Location, this.FileName+".temp");
		
		//Delete temp file if it exists
		if (thisTempFile.exists() == true) {
			thisTempFile.delete();
		}					
		
		try {
			thisTempFile.createNewFile();

			GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(thisTempFile));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zfile, "UTF8"));

			bw.write(getHeader());
			bw.flush();

			bw.write(GenerateContent());

			
			bw.write(getFooter());
			bw.flush();
			bw.close();
			
			File thisFile = new File(this.Location, this.FileName);

			// Delete temp file if it exists
			if (thisFile.exists() == true) {
				thisFile.delete();
			}						

			this.MoveFile(thisTempFile, thisFile);	
			
			SDIConstants.SDILogger.info("SDI File: "+this.FileName+" has been generated");

			
		} catch (FileNotFoundException e) {
			SDIConstants.SDILogger.error("FileNotFoundException",e);
		} catch (UnsupportedEncodingException e) {
			SDIConstants.SDILogger.error("UnsupportedEncodingException",e);
		} catch (IOException e) {
			SDIConstants.SDILogger.error("IOException",e);
		} finally {
			this.count.Decrease();
		}
	}
	
	private String GenerateContent(){
		String content = "";
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.newDocument();
			doc.setStrictErrorChecking(false);

			GenericMetadataSection(doc);			

			if (doc.hasChildNodes()){
				content = getDocumentAsXml(doc);
				content = content.replaceAll("<env:Data type=", "<env:Data xsi:type=");
			} 
			else {
				content = "<env:ContentItem  action=\"Overwrite\">\r\n"
						+ " <env:Data xsi:type=\"GenericAssetMetadata\" />\r\n"
						+ "</env:ContentItem>";
			}
			
		} catch (DOMException e) {
			SDIConstants.SDILogger.error( "DOMException", e);
		} catch (SecurityException e) {
			SDIConstants.SDILogger.error( "SecurityException", e);
		} catch (ParserConfigurationException e) {
			SDIConstants.SDILogger.error( "ParserConfigurationException", e);
		} catch (Exception e) {
			SDIConstants.SDILogger.error( "Exception", e);
		}
		return content;
	}
	
	private void GenericMetadataSection(Document doc) {
		Iterator<Entry<Long, GenericMetaDataEleDetail>> iterator = this.SDIPLC.generic_metadata_ele_detail_info.entrySet()
				.iterator();
		GenericMetaDataEleDetail detail = null;
		Field[] fields = GenericMetaDataEleDetail.class.getDeclaredFields();
		while (iterator.hasNext()) {
			Entry<Long, GenericMetaDataEleDetail> entry = (Entry<Long, GenericMetaDataEleDetail>) iterator.next();
			detail = (GenericMetaDataEleDetail) entry.getValue();

			
			// filter 
			Date modifiedDate = detail.getEntity_modified_date();
			if (!this.IsFullLoad() && ( modifiedDate.before(CoverageStart) || modifiedDate.after(CoverageEnd)) ){
				continue;
			}			

			String isActive = detail.getIs_active();
			Date  createdDate = detail.getEntity_created_date();
			String action = "";
			if (isActive.equalsIgnoreCase("N")){
				action = "Delete";
			} else {
				action = this.getAction(createdDate);
			}
			
			Element elContentItem = doc.createElement("env:ContentItem");
			elContentItem.setAttribute("action", action);
			doc.appendChild(elContentItem);
			Element elData = doc.createElement("env:Data");
			elData.setAttribute("type", "GenericAssetMetadata");
			elContentItem.appendChild(elData);

			Element elGAM = doc.createElement("GenericAssetMetadata");
			elData.appendChild(elGAM);

//			elGAM.setAttribute("languageId", "505062");
			
			Date efFrom = detail.getGAMetadata_effective_from();
			if (efFrom != null) {
				DateFormat formatter = new SimpleDateFormat(
						SDIConstants.DATE_FORMAT_SSS);
				String value = formatter.format(efFrom);
				elGAM.setAttribute("effectiveFrom", value);
			}		
					
			for (int i = 0; i < fields.length; i++) {
				fields[i].setAccessible(true);
				Object obj;
				try {
					obj = fields[i].get(detail);
					if (obj != null) {
						String itemName = fields[i].getName();
						if (itemName.equalsIgnoreCase("GAMetadataAssetGroups")){
							Element parentElement = doc.createElement("GAMetadataAssetGroups");
							String[] groups = obj.toString().split("\\|");
							for (String group : groups){
								if (group != null && group.length() > 0){
									Element gItem = doc.createElement("GAMetadataAssetGroup");
									gItem.appendChild(doc.createTextNode(group));
									parentElement.appendChild(gItem);
								}
							}
							if (parentElement.hasChildNodes()){
								elGAM.appendChild(parentElement);
							}
						} else if (itemName.equalsIgnoreCase("GAMetadataEnumerationValues")){
							Element parentElement = doc.createElement("GAMetadataEnumerationValues");
							String[] enumValues = obj.toString().split("\\|");
							for (String value : enumValues){
								if (value != null && value.length() > 0){
									Element gItem = doc.createElement("GAMetadataEnumerationValue");
									gItem.appendChild(doc.createTextNode(value));
									parentElement.appendChild(gItem);
								}
							}
							if (parentElement.hasChildNodes()){
								elGAM.appendChild(parentElement);
							}
						} else {
							if (itemName.indexOf("GA") != 0 || itemName.equalsIgnoreCase("GAMetadata_effective_from"))
								continue;
							
							Element item = doc.createElement(itemName);							
							String itemValue = null;
							if (obj instanceof java.util.Date) {
								DateFormat formatter = new SimpleDateFormat(
										SDIConstants.DATE_FORMAT_SSS);
								itemValue = formatter.format((Date) obj);
							} else {
								itemValue = obj.toString();
							}
							if (itemName.equalsIgnoreCase("GAMetadataDescription")
									|| itemName.equalsIgnoreCase("GAMetadataEnglishLongName") 
									|| itemName.equalsIgnoreCase("GAMetadataID")){
								item.setAttribute("languageId", "505062");
							}
							item.appendChild(doc.createTextNode(itemValue));
							elGAM.appendChild(item);
						}						
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {				
					e.printStackTrace();
				}				
			}			
		}
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
		} catch (FileNotFoundException e) {
			SDIConstants.SDILogger.error("FileNotFoundException",e);
		} catch (IOException e) {
			SDIConstants.SDILogger.error("IOException",e);
		}
	}		
	
	protected String getFooter() {
		return "		</env:Body>\r\n" + "</env:ContentEnvelope>";
	}

	private String getHeader() {
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

		header = header.replace("@XsdType@", "GenericAssetMetadata.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
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
	
	/**
	 * Transform XmlDocument to string
	 * 
	 * @param doc
	 * @return
	 */
	private String getDocumentAsXml(final Document doc) {

		String result = "";
		try {
			DOMSource domSource = new DOMSource(doc);

			TransformerFactory transFactory = TransformerFactory.newInstance();
			Transformer transformer = transFactory.newTransformer();
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
			//
			java.io.StringWriter writer = new java.io.StringWriter();

			StreamResult streamResult = new StreamResult(writer);
			transformer.transform(domSource, streamResult);
			result = writer.toString();
		} catch (TransformerConfigurationException e) {
			SDIConstants.SDILogger.error(
					"TransformerConfigurationException", e);
		} catch (IllegalArgumentException e) {
			SDIConstants.SDILogger.error( "IllegalArgumentException", e);
		} catch (TransformerFactoryConfigurationError e) {
			SDIConstants.SDILogger.error(
					"TransformerFactoryConfigurationError", e);
		} catch (TransformerException e) {
			SDIConstants.SDILogger.error( "TransformerException", e);
		}

		return result;

	}
}
