package com.thomsonreuters.ce.oasdi;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import javax.xml.stream.util.StreamReaderDelegate;

import com.thomsonreuters.ce.dbor.cache.SDIPublishStyle;
import com.thomsonreuters.ce.dbor.file.ZippedFileProcessor;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;


public class OASDILoader extends ZippedFileProcessor {

	private static final String MergeSQL = "MERGE INTO organisation_oa a using (select ? as perm_id, ? as organisation_name, ? as URL, ? as country_code,? as entity_create_date, ? as entity_modified_date, ? as effective_from_date, ? as effective_to_date from dual) b on (a.perm_id=b.perm_id) when matched then update set a.organisation_name=b.organisation_name, a.URL=b.URL, a.country_code=b.country_code, a.entity_create_date=b.entity_create_date, a.entity_modified_date=b.entity_modified_date, a.effective_from_date=b.effective_from_date, a.effective_to_date=b.effective_to_date when not matched then insert (perm_id, organisation_name, URL, country_code, entity_create_date, entity_modified_date, effective_from_date, effective_to_date) values (b.perm_id, b.organisation_name, b.URL, b.country_code, b.entity_create_date, b.entity_modified_date, b.effective_from_date, b.effective_to_date)";

	private static final String NAMESPACE_ENV = "http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/";

	private static final String NAMESPACE_OA = "http://oa.schemas.tfn.thomson.com/Organization/2010-05-01/";

	private static final String PrevCheckSQL = "select 1 from file_process_history a,sdi_file_detail b where a.id=b.fph_id and a.dit_file_category_id=? and b.incremental_date=to_date(?,'yyyymmddhh24miss') and a.end_time is not null";

	private static final String InsertSDIDetail = "insert into sdi_file_detail (fph_id, dit_publish_style_id, uuid,sdi_timestamp,incremental_date, previous_date) values (?,?,?,?,to_date(?,'yyyymmddhh24miss'),to_date(?,'yyyymmddhh24miss'))";

	private String SDI_Filename;
	
	private String Time_Stamp_text;
	
	private int File_Category_ID;
	
	public void Initialize(File FeedFile) {
		
		SDI_Filename = FeedFile.getName();	
		
		FileCategory FC=getFileCatory(FeedFile);
		
		if (FC.equals(FileCategory.getInstance("OA SDI")))
		{
			Time_Stamp_text=SDI_Filename.substring(43);
		}
		
		
		File_Category_ID = FC.getID();
	}

	public Date strToDateLong(String strDate) {
		if (strDate != null) {
			SimpleDateFormat formatter = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ss");
			ParsePosition pos = new ParsePosition(0);
			Date strtodate = new Date(formatter.parse(strDate, pos).getTime());
			return strtodate;
		}
		return null;
	}

	public void ProcessFile(String filename, InputStream IS) {
		
		try {

			XMLInputFactory factory = XMLInputFactory.newInstance();
			XMLStreamReader reader;

			reader = new StreamReaderDelegate(factory.createXMLStreamReader(IS)) {
				
				@Override
				public int next() throws XMLStreamException {

					while (true) {
						int event = super.next();

						if (event == XMLStreamConstants.START_ELEMENT) {
							String ELocalName = getLocalName();
							String NameSpace = getNamespaceURI();

							if (NameSpace.equals(NAMESPACE_OA)) {
								if (ELocalName.equals("Organization")
										|| ELocalName.equals("OrganizationId")
										|| ELocalName
												.equals("OrganizationName")
										|| ELocalName
												.equals("OrganizationWebsite")
										|| ELocalName
												.equals("OrganizationAddressCountryCode")) {
									return event;
								}
							} else if (NameSpace.equals(NAMESPACE_ENV)) {
								if (ELocalName.equals("ContentEnvelope")
										|| ELocalName.equals("ContentItem")
										|| ELocalName.equals("Id")
										|| ELocalName.equals("TimeStamp")) {
									return event;
								}
							}

						} else if (event == XMLStreamConstants.END_ELEMENT) {
							String ELocalName = getLocalName();
							String NameSpace = getNamespaceURI();

							if (NameSpace.equals(NAMESPACE_ENV)) {
								if (ELocalName.equals("Info")) {
									return event;
								}
							}
							else if (NameSpace.equals(NAMESPACE_OA))
							{
								if (ELocalName.equals("Organization")) {
									return event;
								}
							}
						}else if (event == XMLStreamConstants.END_DOCUMENT) {
							return event;
						}

						continue;

					}
				}
			};

			String action = null;
			String entityCreatedDate = null;
			String entityModifiedDate = null;
			long OrganizationId = 0L;
			
			String OrganizationName = null;
			String effectiveFrom = null;
			String effectiveTo = null;
			String URL = null;
			String CountryCode=null;
			String MsgID = null;
			Date MsgTimeStamp = null;
			String publishingStyle = null;
			

			
			while (reader.hasNext()) {
				reader.next();

				if (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {

					String ELocalName = reader.getLocalName();
					String NameSpace = reader.getNamespaceURI();

					if (NameSpace.equals(NAMESPACE_ENV)) {
						if (ELocalName.equals("ContentEnvelope")) {
							publishingStyle = reader.getAttributeValue(null,
									"pubStyle");
						} else if (ELocalName.equals("ContentItem")) {
							action = reader.getAttributeValue(null, "action");
						} else if (ELocalName.equals("Id")) {
							MsgID = reader.getElementText();
						} else if (ELocalName.equals("TimeStamp")) {
							MsgTimeStamp = strToDateLong(reader.getElementText());
						}

					} else if (NameSpace.equals(NAMESPACE_OA)) {
						if (ELocalName.equals("Organization")) {
							entityCreatedDate = reader.getAttributeValue(null,
									"entityCreatedDate");
							entityModifiedDate = reader.getAttributeValue(null,
									"entityModifiedDate");
						} else if (ELocalName.equals("OrganizationId")) {
							OrganizationId = Long.parseLong(reader
									.getElementText());
						} else if (ELocalName.equals("OrganizationName")) {
							String temp_effectiveFrom = reader.getAttributeValue(null,
									"effectiveFrom");
							String temp_effectiveTo = reader.getAttributeValue(null,
									"effectiveTo");
							String organizationNameTypeCode = reader
									.getAttributeValue(null,
											"organizationNameTypeCode");
							int languageId = Integer.parseInt(reader.getAttributeValue(
									null, "languageId"));		
							
							if (organizationNameTypeCode.equals("LNG"))
							{
								if (languageId<=505075 && languageId>=505062)
								{
									OrganizationName=reader.getAttributeValue(null,"organizationNameLocalNormalized");
								}
								else
								{
									OrganizationName=reader.getAttributeValue(null,"organizationNameEnglishNormalized");
								}
								
								effectiveFrom=temp_effectiveFrom;
								effectiveTo=temp_effectiveTo;
							}
							
						} else if (ELocalName.equals("OrganizationWebsite")) {
							URL = reader.getElementText();
						} else if (ELocalName.equals("OrganizationAddressCountryCode")) {
							CountryCode = reader.getElementText();
						}

					}

				} else if (reader.getEventType() == XMLStreamConstants.END_ELEMENT) {
					String ELocalName = reader.getLocalName();
					String NameSpace = reader.getNamespaceURI();

					if (NameSpace.equals(NAMESPACE_ENV)) {
						if (ELocalName.equals("Info")) {
							Date DateTimeofPrev = null;
							Date DatetimeofInc = null;

							SimpleDateFormat formatter = new SimpleDateFormat(
									"yyyy-MM-dd-HHmm");

							SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
							
							if (publishingStyle.equals("Incremental")) {
								ParsePosition pos = new ParsePosition(0);
								DateTimeofPrev = new Date(formatter.parse(
										Time_Stamp_text, pos).getTime());
								
								pos = new ParsePosition(16);
								DatetimeofInc = new Date(formatter.parse(
										Time_Stamp_text, pos).getTime());
								
								
								Connection DBConn=null;
								
								
								try {
									//if previous file has been processed successfully
									
									DBConn = new EasyConnection(DBConnNames.CEF_CNR);
									PreparedStatement objPreStatement = null;
									ResultSet objResult = null;
									objPreStatement = DBConn
											.prepareStatement(PrevCheckSQL);
									
									objPreStatement.setInt(1, File_Category_ID);									
									objPreStatement.setString(2, f.format(DateTimeofPrev));
									objResult = objPreStatement.executeQuery();

									
									if (!objResult.next()) {
										throw new LogicException(
												"Can not process incremental SDI file: "
														+ SDI_Filename
														+ " because previous SDI file has not been processed successfully");
									}

									//create new record in table EM_SDI_HISTORY
									objPreStatement = DBConn
											.prepareStatement(InsertSDIDetail);
									objPreStatement.setLong(1, this.FPH_ID);
									objPreStatement.setInt(2, SDIPublishStyle.INCREMENTAL.getID());
									objPreStatement.setString(3, MsgID);
									objPreStatement.setDate(4, MsgTimeStamp);
									
									objPreStatement.setString(5, f.format(DatetimeofInc));
									objPreStatement.setString(6, f.format(DateTimeofPrev));
									
									objPreStatement.executeUpdate();
																	
									DBConn.commit();

									objPreStatement.close();

								} catch (SQLException e) {
									// TODO Auto-generated catch block
									throw new SystemException(e.getMessage(),e);

								}finally
								{
									try {
										DBConn.close();
									} catch (SQLException e) {
										throw new SystemException(e.getMessage(),e);
									}									
								}

							} else if (publishingStyle.equals("FullRebuild")) {
								ParsePosition pos = new ParsePosition(0);
								DatetimeofInc = new Date(formatter.parse(
										Time_Stamp_text, pos).getTime());
								
								Connection DBConn=null;
								try {
									DBConn = new EasyConnection(DBConnNames.CEF_CNR);
									PreparedStatement objPreStatement = null;

									// create new record in table EM_SDI_HISTORY

									objPreStatement = DBConn.prepareStatement(InsertSDIDetail);
									objPreStatement.setLong(1, this.FPH_ID);
									objPreStatement.setInt(2,SDIPublishStyle.FULL.getID());
									objPreStatement.setString(3, MsgID);
									objPreStatement.setDate(4, MsgTimeStamp);
									objPreStatement.setString(5, f.format(DatetimeofInc));
									objPreStatement.setString(6, null);
									objPreStatement.executeUpdate();
									DBConn.commit();

									objPreStatement.close();
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									throw new SystemException(e.getMessage(),e);
								}finally
								{
									try {
										DBConn.close();
									} catch (SQLException e) {
										throw new SystemException(e.getMessage(),e);
									}									
								}								
							}

						}
					}
					else if (NameSpace.equals(NAMESPACE_OA))
					{
						if (ELocalName.equals("Organization")) {
							if (OrganizationName == null) {								
								this.LogDetails(MsgCategory.WARN, "OASDI: Can not locate a proper English name for organization: "+OrganizationId);
							} else {

								///////////////////////////
								Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
								PreparedStatement objPreStatement = null;

								try {

									objPreStatement = DBConn.prepareStatement(MergeSQL);
									objPreStatement.setLong(1, OrganizationId);
									objPreStatement.setString(2, OrganizationName);
									objPreStatement.setString(3, URL);
									objPreStatement.setString(4, CountryCode);
									objPreStatement.setDate(5, strToDateLong(entityCreatedDate));
									objPreStatement.setDate(6, strToDateLong(entityModifiedDate));
									objPreStatement.setDate(7, strToDateLong(effectiveFrom));
									objPreStatement.setDate(8, strToDateLong(effectiveTo));

									objPreStatement.executeUpdate();
									DBConn.commit();			
									
									objPreStatement.close();

								} catch (SQLException e) {
									// TODO Auto-generated catch block
									throw new SystemException(e.getMessage(),e);

								}finally
								{
									try {
										DBConn.close();
									} catch (SQLException e) {
										throw new SystemException(e.getMessage(),e);
									}									
								}							
								///////////////////////////
							}

							action = null;
							entityCreatedDate = null;
							entityModifiedDate = null;
							OrganizationId = 0L;
							OrganizationName = null;
							effectiveFrom = null;
							effectiveTo = null;
							URL = null;
							CountryCode = null;
						}
					}
				}
			}
			
			reader.close();

		} catch (XMLStreamException e) {
			throw new SystemException(e.getMessage(),e);
		} 
	}

	public void Finalize() {		
	}
	
	public FileCategory getFileCatory(File FeedFile)
	{
		return FileCategory.getInstance("OA SDI");
		
	}

}
