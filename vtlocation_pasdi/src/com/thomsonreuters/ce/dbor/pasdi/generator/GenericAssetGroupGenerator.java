package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
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
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.GenericMetaDataGroupDetail;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;

public class GenericAssetGroupGenerator implements Runnable {

	private Counter count;
	protected Date CoverageEnd;
	protected Date CoverageStart;
	private String fileName;

	private String location;
	private SDIPreLoadCache SDIPLC = null;

	public GenericAssetGroupGenerator(String fileLocation, String fileName,
			Date startTime, Date endTime, SDIPreLoadCache sdiplc, Counter counter) {
		this.location = fileLocation;
		this.fileName = fileName;
		this.CoverageStart = startTime;
		this.CoverageEnd = endTime;
		this.SDIPLC = sdiplc;	
		
		this.count = counter;
		this.count.Increase();
	}

	private String GenerateContent() {
		String content = "";
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.newDocument();
			doc.setStrictErrorChecking(false);

			GenericMetadataGroupSection(doc);
			if (doc.hasChildNodes()){
				content = getDocumentAsXml(doc);
				content = content.replaceAll("<env:Data type=", "<env:Data xsi:type=");
			} 
			else {
				content = "<env:ContentItem  action=\"Overwrite\">\r\n"
						+ " <env:Data xsi:type=\"GAMetadataGroup\" />\r\n"
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

	private void GenericMetadataGroupSection(Document doc) {
				
		Iterator<Entry<Long, GenericMetaDataGroupDetail[]>> iterator = this.SDIPLC.generic_metadata_group_detail_info
				.entrySet().iterator();
		GenericMetaDataGroupDetail[] details = null;
		while (iterator.hasNext()) {
			Entry<Long, GenericMetaDataGroupDetail[]> entry = (Entry<Long, GenericMetaDataGroupDetail[]>) iterator
					.next();
			details = (GenericMetaDataGroupDetail[]) entry.getValue();

			GenericMetaDataGroupDetail detail = details[0];
			
			//filter
			Date modifiedDate = detail.getEntity_modified_date();
			if (!this.IsFullLoad() && ( modifiedDate.before(CoverageStart) || modifiedDate.after(CoverageEnd)) ){
				continue;
			}			

			String groupId = detail.getGAGroupID();
			String groupType = detail.getGAGroupType();
			String searchable = detail.getGAMetadataSearchable();
			String isActive = detail.getIs_active();
			Date createdDate = detail.getEntity_created_date();
			
			if (isActive.equalsIgnoreCase("N")) {
				continue;
			}

			Element elContentItem = doc.createElement("env:ContentItem");
			String action = this.getAction(createdDate);
			elContentItem.setAttribute("action", action);
			doc.appendChild(elContentItem);
			Element elData = doc.createElement("env:Data");
			elData.setAttribute("type", "GAMetadataGroup");
			elContentItem.appendChild(elData);

			Element elGAG = doc.createElement("GAGroup");
			elData.appendChild(elGAG);

			if (groupId != null && groupId.replace(" ", "").length() > 0) {
				Element item = doc.createElement("GAGroupID");
				item.appendChild(doc.createTextNode(groupId));
				elGAG.appendChild(item);
			}
			if (groupType != null && groupType.replace(" ", "").length() > 0) {
				Element item = doc.createElement("GAGroupType");
				item.appendChild(doc.createTextNode(groupType));
				elGAG.appendChild(item);
			}
			if (searchable != null && searchable.replace(" ", "").length() > 0) {
				Element item = doc.createElement("GAGroupSearchable");
				item.appendChild(doc.createTextNode(searchable));
				elGAG.appendChild(item);
			}

			for (int i = 0; i < details.length; i++) {
				String metadataId = details[i].getGAMetadataID();
				String metadataValue = details[i]
						.getGAMetadataEnumerationValue();

				Element parentEl = doc.createElement("GAGroupMetadata");

				Element item = doc.createElement("GAMetadataID");
				item.setAttribute("languageId", "505062");
				item.appendChild(doc.createTextNode(metadataId));
				parentEl.appendChild(item);
				
				if (metadataValue != null) {
					item = doc.createElement("GAGroupMetadataValue");
					item.appendChild(doc.createTextNode(metadataValue));
					parentEl.appendChild(item);
				}
				
				if (parentEl.hasChildNodes()) {
					elGAG.appendChild(parentEl);
				}
			}
		}
	}

	public String getAction(Date EntityCreateDate) {
		if (CoverageStart == null) {
			return "Insert";
		} else if (EntityCreateDate.after(CoverageStart)) {
			return "Insert";
		} else {
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

		header = header.replace("@XsdType@", "GenericAssetGroup.xsd");
		header = header.replace("@assetType@", "CommodityPhysicalAssets");

		return header;
	}

	public boolean IsFullLoad() {
		if (CoverageStart == null) {
			return true;
		} else {
			return false;
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
			SDIConstants.SDILogger.error( "FileNotFoundException", e);
		} catch (IOException e) {
			SDIConstants.SDILogger.error( "IOException", e);
		}
	}

	public void run() {

		File thisTempFile = new File(this.location, this.fileName + ".temp");

		// Delete temp file if it exists
		if (thisTempFile.exists() == true) {
			thisTempFile.delete();
		}

		try {
			thisTempFile.createNewFile();

			GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(
					thisTempFile));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					zfile, "UTF8"));

			bw.write(getHeader());
			bw.flush();

			bw.write(GenerateContent());

			bw.write(getFooter());
			bw.flush();
			bw.close();

			File thisFile = new File(this.location, this.fileName);

			// Delete temp file if it exists
			if (thisFile.exists() == true) {
				thisFile.delete();
			}

			this.MoveFile(thisTempFile, thisFile);

			SDIConstants.SDILogger.info( "SDI File: " + this.fileName
					+ " has been generated");

		} catch (FileNotFoundException e) {
			SDIConstants.SDILogger.error( "FileNotFoundException", e);
		} catch (UnsupportedEncodingException e) {
			SDIConstants.SDILogger.error( "UnsupportedEncodingException", e);
		} catch (IOException e) {
			SDIConstants.SDILogger.error( "IOException", e);
		} finally {
			this.count.Decrease();
		}
	}
}
