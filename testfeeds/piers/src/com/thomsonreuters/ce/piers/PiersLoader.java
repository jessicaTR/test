package com.thomsonreuters.ce.piers;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.file.FileProcessor;

public class PiersLoader extends FileProcessor {
	private Logger logger = Starter.thisLogger;
	public void ProcessFile(File FeedFile) { 
		// TODO Auto-generated method stub
		Connection DBConn = null;	
		CallableStatement callableorderSM=null;
		CallableStatement callableDelOrderSM=null;
		String file_name=FeedFile.getName();
		
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			ZipFile ZippedFile = new ZipFile(FeedFile);
			Enumeration ZippedFiles = ZippedFile.entries();
			
			while (ZippedFiles.hasMoreElements()) {
				ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();
				
				InputStream IS = ZippedFile.getInputStream(entry);
				BufferedReader FR = new BufferedReader(new InputStreamReader(IS));
				
				CSVDataSet CDS=new CSVDataSet(FR,',','"','\b',0,false);
				
				while(CDS.next())
				{
					if(file_name.toUpperCase().indexOf("DEL")>0){
						try {
							String recnum = CDS.getValue("RECNUM");
							callableDelOrderSM = DBConn.prepareCall("{call PIERS_LOAD_PKG.delete_piers_transcation_proc(?)}");
							callableDelOrderSM.setString(1, recnum) ;
							callableDelOrderSM.execute();
							callableDelOrderSM.close();
							DBConn.commit();
						}catch(SQLException e){
							callableDelOrderSM.close();
							// TODO Auto-generated catch block
							this.LogDetails(MsgCategory.WARN, "Failed to insert row:" +CDS.toString()+ " because of exception: "+e.getMessage());
							logger.error("Failed to insert row:" +CDS.toString(), e);						
						}
						
					}
					else{
						//////////////////////////////
						//read values from current row
						try {
							
							String dir_in=CDS.getValue("DIR");
							String vdate_in=CDS.getValue("VDATE");
//							Date tmp_vdate = strToDate(vdate_in);
//							vdate_in = DateToStr(tmp_vdate);
							
							String commodity_in=CDS.getValue("COMMODITY");//??????
//							if(commodity_in.length()>35) commodity_in = commodity_in.substring(35);
							
							String comcode_in=CDS.getValue("COMCODE");
							String com7_desc_in=CDS.getValue("COM7_DESC");
							String com4_in=CDS.getValue("COM4");
							String com4_desc_in=CDS.getValue("COM4_DESC");
							String hscode_in=CDS.getValue("HSCODE");
							String harm_desc_in=CDS.getValue("HARM_DESC");
							String harm4_in=CDS.getValue("HARM4");
							String h4_desc_in=CDS.getValue("H4_DESC");
							String ctrycode_in=CDS.getValue("CTRYCODE");
							String country_in=CDS.getValue("COUNTRY");
							String countryl_in=CDS.getValue("COUNTRYL");
							String uscode_in=CDS.getValue("USCODE");
							String usport_in=CDS.getValue("USPORT");
							String fcode_in=CDS.getValue("FCODE");
							String fport_in=CDS.getValue("FPORT");
							String ultcode_in=CDS.getValue("ULTCODE");
							String ultport_in=CDS.getValue("ULTPORT");
							String usib_code_in=CDS.getValue("USIB_CODE");
							String usib_city_in=CDS.getValue("USIB_CITY");
							String usib_st_in=CDS.getValue("USIB_ST");
							String org_des_city_in=CDS.getValue("ORG_DES_CITY");
							String org_des_st_in=CDS.getValue("ORG_DES_ST");
							String fgnib_code_in=CDS.getValue("FGNIB_CODE");
							String fgnib_city_in=CDS.getValue("FGNIB_CITY");
							String fgnib_ctry_in=CDS.getValue("FGNIB_CTRY");
							String name_in=CDS.getValue("NAME");
							String city_in=CDS.getValue("CITY") ;//????????
//							if(city_in.length()>12) city_in = city_in.substring(12);
							
							String st_in=CDS.getValue("ST");
							String street_in=CDS.getValue("STREET");
							String street2_in=CDS.getValue("STREET2");
							String zipcode_in=CDS.getValue("ZIPCODE");
							String comp_nbr_in=CDS.getValue("COMP_NBR");
							String main_cmp_nbr_in=CDS.getValue("MAIN_CMP_NBR");
							String fname_in=CDS.getValue("FNAME");
							String fcity_in=CDS.getValue("FCITY");
							String fctrycode_in=CDS.getValue("FCTRYCODE");
							String fcountry_in=CDS.getValue("FCOUNTRY");
							String fcountryl_in=CDS.getValue("FCOUNTRYL");
							String fstreet_in=CDS.getValue("FSTREET");
							String fstreet2_in=CDS.getValue("FSTREET2");
							String fzipcode_in=CDS.getValue("FZIPCODE");
							String fcomp_nbr_in=CDS.getValue("FCOMP_NBR");
							String ntf_name_in=CDS.getValue("NTF_NAME");
							String ntf_city_in=CDS.getValue("NTF_CITY");
							String ntf_st_in=CDS.getValue("NTF_ST");
							String ntf_street_in=CDS.getValue("NTF_STREET");
							String ntf_street2_in=CDS.getValue("NTF_STREET2");
							String ntf_zipcode_in=CDS.getValue("NTF_ZIPCODE");
							String ntf_comp_nbr_in=CDS.getValue("NTF_COMP_NBR");
							String sline_in=CDS.getValue("SLINE");
							String vessel_in=CDS.getValue("VESSEL");
							String vessel_code_in=CDS.getValue("VESSEL_CODE");
							String registry_in=CDS.getValue("REGISTRY");//??????
//							if(registry_in.length()>3) registry_in = registry_in.substring(3);
							
							String voyage_in=CDS.getValue("VOYAGE");
							String manifest_nbr_in=CDS.getValue("MANIFEST_NBR");
							String bol_nbr_in=CDS.getValue("BOL_NBR");//?????????
//							if(bol_nbr_in.length() >12 ) bol_nbr_in = bol_nbr_in.substring(12);
							
							String qty_in=CDS.getValue("QTY");
							String u_m_in=CDS.getValue("U_M");//???????
//							if(u_m_in.length()>3) u_m_in = u_m_in.substring(3);
							
							String reefer_flag_in=CDS.getValue("REEFER_FLAG");
							String roro_flag_in=CDS.getValue("RORO_FLAG");
							String hazmat_flag_in=CDS.getValue("HAZMAT_FLAG");
							String nvocc_flag_in=CDS.getValue("NVOCC_FLAG");
							String conflag_in=CDS.getValue("CONFLAG");
							String consize_in=CDS.getValue("CONSIZE");
							
							Integer conqty_in=null;
							{
								String temp_conqty_in=CDS.getValue("CONQTY");
								if (temp_conqty_in!=null && !temp_conqty_in.equals(""))
								{
									conqty_in=Integer.valueOf(temp_conqty_in);
								}
							}
							
							Double convol_in=null;
							{
								String temp_convol_in=CDS.getValue("CONVOL");
								if (temp_convol_in!=null && !temp_convol_in.equals(""))
								{
									convol_in=Double.valueOf(temp_convol_in);
								}
							}
												
							Double teus_in=null;
							{
								String temp_teus_in=CDS.getValue("TEUS");
								if (temp_teus_in!=null && !temp_teus_in.equals(""))
								{
									teus_in=Double.valueOf(temp_teus_in);
								}
							}					
							
							Double feus_in=null;
							{
								String temp_feus_in=CDS.getValue("FEUS");
								if (temp_feus_in!=null && !temp_feus_in.equals(""))
								{
									feus_in=Double.valueOf(temp_feus_in);
								}
							}	
												
							Double pounds_in=null;
							{
								String temp_pounds_in=CDS.getValue("POUNDS");
								if (temp_pounds_in!=null && !temp_pounds_in.equals(""))
								{
									pounds_in=Double.valueOf(temp_pounds_in);
								}
							}
							
							Double mtons_in=null;
							{
								String temp_mtons_in=CDS.getValue("MTONS");
								if (temp_mtons_in!=null && !temp_mtons_in.equals(""))
								{
									mtons_in=Double.valueOf(temp_mtons_in);
								}
							}	
							
							Double ltons_in=null;
							{
								String temp_ltons_in=CDS.getValue("LTONS");
								if (temp_ltons_in!=null && !temp_ltons_in.equals(""))
								{
									ltons_in=Double.valueOf(temp_ltons_in);
								}
							}	
							
							
							Double stons_in=null;
							{
								String temp_stons_in=CDS.getValue("STONS");
								if (temp_stons_in!=null && !temp_stons_in.equals(""))
								{
									stons_in=Double.valueOf(temp_stons_in);
								}
							}
							
							Double kilos_in=null;
							{
								String temp_kilos_in=CDS.getValue("KILOS");
								if (temp_kilos_in!=null && !temp_kilos_in.equals(""))
								{
									kilos_in=Double.valueOf(temp_kilos_in);
								}
							}

							Integer shpmnts_in=null;
							{
								String temp_shpmnts_in=CDS.getValue("SHPMNTS");
								if (temp_shpmnts_in!=null && !temp_shpmnts_in.equals(""))
								{
									shpmnts_in=Integer.valueOf(temp_shpmnts_in);
								}
							}
							
							String financial_in=CDS.getValue("FINANCIAL");
							String bank_in=CDS.getValue("BANK");
							String payable_in=CDS.getValue("PAYABLE");
			 				
							Double value_in=null;
							{
								String temp_value_in=CDS.getValue("VALUE");
								if (temp_value_in!=null && !temp_value_in.equals(""))
								{
									value_in=Double.valueOf(temp_value_in); 
								}
							}
							
							
							String pdate_in=CDS.getValue("PDATE");
							String udate_in=CDS.getValue("UDATE");
//							Date tmp_udate = strToDate(udate_in);
//							udate_in = DateToStr(tmp_udate);
							String recnum_in=CDS.getValue("RECNUM");							
							String file_type_in = null;
							//(file_name.split("_"))[2];
							if(file_name.indexOf("DAILY")>0) file_type_in = "DAILY";
							if(file_name.indexOf("HISTORY")>0) file_type_in = "HISTORY";
							String raw_commodity_in = null;
//							if(file_type_in.equals("DAILY"))
							raw_commodity_in = CDS.getValue("RAW_COMMODITY");
							String origin_cty_in = CDS.getValue("CountryOfOrigin");
							
							//////////////////////////////////////////////////
							//Parsing file done
							//////////////////////////////////////////////////

							callableorderSM = DBConn.prepareCall("{call PIERS_LOAD_PKG.process_piers_transaction_proc(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");

							callableorderSM.setString("dir_in",dir_in);
							callableorderSM.setString("vdate_in",vdate_in);
							callableorderSM.setString("commodity_in",commodity_in);
							callableorderSM.setString("comcode_in",comcode_in);
							callableorderSM.setString("com7_desc_in",com7_desc_in);
							callableorderSM.setString("com4_in",com4_in);
							callableorderSM.setString("com4_desc_in",com4_desc_in);
							callableorderSM.setString("hscode_in",hscode_in);
							callableorderSM.setString("harm_desc_in",harm_desc_in);
							callableorderSM.setString("harm4_in",harm4_in);
							callableorderSM.setString("h4_desc_in",h4_desc_in);
							callableorderSM.setString("ctrycode_in",ctrycode_in);
							callableorderSM.setString("country_in",country_in);
							callableorderSM.setString("countryl_in",countryl_in);
							callableorderSM.setString("uscode_in",uscode_in);
							callableorderSM.setString("usport_in",usport_in);
							callableorderSM.setString("fcode_in",fcode_in);
							callableorderSM.setString("fport_in",fport_in);
							callableorderSM.setString("ultcode_in",ultcode_in);
							callableorderSM.setString("ultport_in",ultport_in);
							callableorderSM.setString("usib_code_in",usib_code_in);
							callableorderSM.setString("usib_city_in",usib_city_in);
							callableorderSM.setString("usib_st_in",usib_st_in);
							callableorderSM.setString("org_des_city_in",org_des_city_in);
							callableorderSM.setString("org_des_st_in",org_des_st_in);
							callableorderSM.setString("fgnib_code_in",fgnib_code_in);
							callableorderSM.setString("fgnib_city_in",fgnib_city_in);
							callableorderSM.setString("fgnib_ctry_in",fgnib_ctry_in);
							callableorderSM.setString("name_in",name_in);
							callableorderSM.setString("city_in",city_in);
							callableorderSM.setString("st_in",st_in);
							callableorderSM.setString("street_in",street_in);
							callableorderSM.setString("street2_in",street2_in);
							callableorderSM.setString("zipcode_in",zipcode_in);
							callableorderSM.setString("comp_nbr_in",comp_nbr_in);
							callableorderSM.setString("main_cmp_nbr_in",main_cmp_nbr_in);
							callableorderSM.setString("fname_in",fname_in);
							callableorderSM.setString("fcity_in",fcity_in);
							callableorderSM.setString("fctrycode_in",fctrycode_in);
							callableorderSM.setString("fcountry_in",fcountry_in);
							callableorderSM.setString("fcountryl_in",fcountryl_in);
							callableorderSM.setString("fstreet_in",fstreet_in);
							callableorderSM.setString("fstreet2_in",fstreet2_in);
							callableorderSM.setString("fzipcode_in",fzipcode_in);
							callableorderSM.setString("fcomp_nbr_in",fcomp_nbr_in);
							callableorderSM.setString("ntf_name_in",ntf_name_in);
							callableorderSM.setString("ntf_city_in",ntf_city_in);
							callableorderSM.setString("ntf_st_in",ntf_st_in);
							callableorderSM.setString("ntf_street_in",ntf_street_in);
							callableorderSM.setString("ntf_street2_in",ntf_street2_in);
							callableorderSM.setString("ntf_zipcode_in",ntf_zipcode_in);
							callableorderSM.setString("ntf_comp_nbr_in",ntf_comp_nbr_in);
							callableorderSM.setString("sline_in",sline_in);
							callableorderSM.setString("vessel_in",vessel_in);
							callableorderSM.setString("vessel_code_in",vessel_code_in);
							callableorderSM.setString("registry_in",registry_in);
							callableorderSM.setString("voyage_in",voyage_in);
							callableorderSM.setString("manifest_nbr_in",manifest_nbr_in);
							callableorderSM.setString("bol_nbr_in",bol_nbr_in);
							callableorderSM.setString("qty_in",qty_in);
							callableorderSM.setString("u_m_in",u_m_in);
							callableorderSM.setString("reefer_flag_in",reefer_flag_in);
							callableorderSM.setString("roro_flag_in",roro_flag_in);
							callableorderSM.setString("hazmat_flag_in",hazmat_flag_in);
							callableorderSM.setString("nvocc_flag_in",nvocc_flag_in);
							callableorderSM.setString("conflag_in",conflag_in);
							callableorderSM.setString("consize_in",consize_in);
							
							if (conqty_in!=null)
							{
								callableorderSM.setInt("conqty_in",conqty_in.intValue());
							}
							else
							{
								callableorderSM.setNull("conqty_in", Types.INTEGER);
							}
							
							if (convol_in!=null)
							{
								callableorderSM.setDouble("convol_in",convol_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("convol_in", Types.DOUBLE);
							}
							
							if(teus_in!=null)
							{
								callableorderSM.setDouble("teus_in",teus_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("teus_in", Types.DOUBLE);
							}
							
							if (feus_in!=null)
							{
								callableorderSM.setDouble("feus_in",feus_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("feus_in", Types.DOUBLE);
							}
							
							
							if (pounds_in!=null)
							{
								callableorderSM.setDouble("pounds_in",pounds_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("pounds_in", Types.DOUBLE);
							}
							
							
							if (mtons_in!=null)
							{
								callableorderSM.setDouble("mtons_in",mtons_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("mtons_in", Types.DOUBLE);
							}
							
							if (ltons_in!=null)
							{
								callableorderSM.setDouble("ltons_in",ltons_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("ltons_in", Types.DOUBLE);
							}
							
							if (stons_in!=null)
							{
								callableorderSM.setDouble("stons_in",stons_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("stons_in", Types.DOUBLE);
							}
							
							if (kilos_in!=null)
							{
								callableorderSM.setDouble("kilos_in",kilos_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("kilos_in", Types.DOUBLE);
							}
							
							if (shpmnts_in!=null)
							{
								callableorderSM.setInt("shpmnts_in",shpmnts_in.intValue());
							}
							else
							{
								callableorderSM.setNull("shpmnts_in", Types.INTEGER);
							}
							
							
							callableorderSM.setString("financial_in",financial_in);
							callableorderSM.setString("bank_in",bank_in);
							callableorderSM.setString("payable_in",payable_in);
							
							if (value_in!=null)
							{
								callableorderSM.setDouble("value_in",value_in.doubleValue());
							}
							else
							{
								callableorderSM.setNull("value_in", Types.DOUBLE);
							}
							
							callableorderSM.setString("pdate_in",pdate_in);
							callableorderSM.setString("udate_in",udate_in);
							callableorderSM.setString("recnum_in",recnum_in);
							callableorderSM.setString("origin_cty_in",origin_cty_in);
							if(raw_commodity_in!=null && raw_commodity_in.length()>1000) raw_commodity_in=raw_commodity_in.substring(0,999);
							callableorderSM.setString("raw_commodity_in",raw_commodity_in);
							callableorderSM.setString("file_type_in",file_type_in); 
							callableorderSM.execute();
							callableorderSM.close();
							DBConn.commit();
							

						} catch (Exception e) {
							if(callableorderSM != null) callableorderSM.close();
							// TODO Auto-generated catch block
							this.LogDetails(MsgCategory.WARN, "Failed to insert row with recnum=" +CDS.getValue("RECNUM")+ " because of exception: "+e.getMessage());
							logger.error("Failed to insert row with recnum=" +CDS.getValue("RECNUM"), e);
						}
						
					}//end of else
					
				}//end of loop
				
				FR.close();
			}
			
			ZippedFile.close();
						
		}
		catch (Exception e) {
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + file_name + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + file_name, e);
			throw new SystemException(e.getMessage(), e);
		}
		finally
		{
			try {				
				DBConn.close();
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(),e);
			}	
		}
	}
//	public java.sql.Date strToDate(String strDate) throws ParseException {
//		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yy",
//				Locale.ENGLISH);
//		java.util.Date date = formatter.parse(strDate);
//		java.sql.Date sqlDate = new java.sql.Date(date.getTime());
////		java.sql.Date sqlDate = java.sql.Date.valueOf( strDate );
//        
//		return sqlDate;
//	}
//	
//	public String DateToStr( java.sql.Date dDate) throws ParseException 
//	{ 
//		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd",Locale.ENGLISH);		   
//        String strDate=formatter.format(dDate);
//	    return strDate;		
//	} 	

	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("PIERS");
	}

}
