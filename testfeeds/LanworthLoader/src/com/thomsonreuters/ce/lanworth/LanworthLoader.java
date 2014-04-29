package com.thomsonreuters.ce.lanworth;

//import java.io.File;
//import java.io.FileInputStream;
import java.io.*;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.Collections;
import java.sql.*;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

import java.util.Calendar;

import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import java.util.Locale;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.ZippedCSVProcessor;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;
  
/**
 * The LanworthLoader class process the WASDE files provided by Lanworth. This
 * class will be called in FileScanner class. This class first check the 6
 * header rows, then will process the data row in loop. Each row has some fields
 * are dimension items, which will be passed to PL/SQL to get report cell id.
 * Other fields will be treated as values which will be inserted into value
 * table.
 * 
 * @author jessica.li
 * 
 * 
 */

public class LanworthLoader extends ZippedCSVProcessor {
	
	private Logger logger = Starter.thisLogger;
	public FileCategory getFileCatory(File FeedFile) {
		return FileCategory.getInstance("LANWORTH");
	}

	public void Initialize(File FeedFile) {

	}

	public void ProcessCSVFile(String FileName, List<String[]> CSVArray) {
		// TODO Auto-generated method stub
		Connection thisConnection = null;
		String reportId = null;
		Date reportTime = null;
		String reportTitle = null;
		String reportCommentNo = null;
		String reportUnit = null;
		String reportSource = null;
		String reportUnitCommentNo = null;
		int rowsOfCommit = 2000;

		String reportUnitCommentType = "REPORT UNIT";
		String reportCommentType = "REPORT NAME";
		String cellCommentType = "CELL VALUE";
		String cellUnitCommentType = "CELL UNIT";
		String month_Config_File = "../cfg/month.conf";
		// String file_Config_File = "cfg/lanworthdataset.conf";
		
		int rteID = -1;
		int processedRowsNumber = 0;
		String SQL_1 = "select full_name from ce_data_set where id=?";
		/**
		 * aloID is application_log id, application log has 3 types: Info,
		 * Warning, Error
		 */

		java.sql.Timestamp nowTime = currentDate();
		String noCommentCon = " has no corresponding comments!";

		try {
			thisConnection = new EasyConnection("processing_history");
			thisConnection.setAutoCommit(false);
			/**
			 * The Conn is declared for Array Descriptor, because
			 * ArrayDescriptor.createDescriptor must use native connecton
			 */
			Connection Conn = thisConnection;

			reportSource = FileName;
			List<String[]> contentList = CSVArray;

			HashMap<String, String> monthHm = new HashMap<String, String>();
			monthHm.put("JANUARY", "01");
			monthHm.put("FEBRUARY", "02");
			monthHm.put("MARCH", "03");
			monthHm.put("APRIL", "04");
			monthHm.put("MAY", "05");
			monthHm.put("JUNE", "06");
			monthHm.put("JULY", "07");
			monthHm.put("AUGUST", "08");
			monthHm.put("SEPTEMBER", "09");
			monthHm.put("OCTOBER", "10");
			monthHm.put("NOVEMBER", "11");
			monthHm.put("DECEMBER", "12");
			/**
			 * Below is to get CE_DATA_SET id
			 */
			int rprID = -1;
			String rprName = null;
			// We can get data set id by below 3 ways:

			/**
			 * get data set id by name
			 * 
			 * if(FileName.indexOf("WASDE")>=0) rprName="World Agricultural
			 * Supply and Demand Estimates"; else if(FileName.indexOf("USDA
			 * export sales")>=0) rprName="USDA - Export Sales Reports"; else
			 * if(FileName.indexOf("Crop Progress")>=0) rprName="USDA - Crop
			 * Progress"; else if(FileName.indexOf("CFTC")>=0)
			 * rprName="Commodity Futures Trading Commission"; else
			 * if(FileName.indexOf("NOPA")>=0) rprName="NOPA Crush"; else throw
			 * new LogicException("Unrecognized file file_Name="+FileName+".\n
			 * Please check file name!");
			 */
			/**
			 * get data set id by config file Properties fileProp = new
			 * Properties(); System.out.print("Loading file name config file:
			 * "+file_Config_File); try{ FileInputStream file_fis = new
			 * FileInputStream(file_Config_File); fileProp.load(file_fis);
			 * file_fis.close();
			 * 
			 * Enumeration en = fileProp.propertyNames(); while
			 * (en.hasMoreElements()) { String key = (String) en.nextElement();
			 * if(FileName.indexOf(key)>=0) { rprName=fileProp.getProperty
			 * (key); break; } }
			 * 
			 * }catch(FileNotFoundException e){ throw new LogicException("File
			 * not found : "+file_Config_File+".\n Please check config file."); }
			 * catch(IOException e){ throw new LogicException("Load config file
			 * "+file_Config_File+" failed!"); }
			 */
			CallableStatement cs;
			cs = thisConnection
					.prepareCall("{? = call report_distribution_pkg.get_data_set_byfile_fn(?)}");
			cs.registerOutParameter(1, Types.INTEGER);
			cs.setString(2, FileName);
			cs.execute();
			rprID = cs.getInt(1);
			if (rprID == -1) {
				throw new LogicException(
						"This file:"
								+ FileName
								+ " is not existing data set!");
			}

			try {
				PreparedStatement ps = thisConnection.prepareStatement(SQL_1);
				ps.setInt(1, rprID);
				ResultSet objResult = ps.executeQuery();
				if (objResult.next()) {
					rprName = objResult.getString(1);
				}
				objResult.close();
				ps.close();
			} catch (SQLException e) {
				throw new LogicException(
						"Get data set full name error when id=" + rprID
								+ "!");
			}

			// get report title, and then can get rteID earlier, because insert
			// application lod need rteID
			/**
			 * Below is to get report_template id
			 */
			try {
				if ((contentList.get(2))[0] != null
						&& (contentList.get(2))[0].equals("Report Title"))
					reportTitle = (contentList.get(2))[1].trim();
				else {
					throw new LogicException(
							"No Report Title row in file third row:"+FileName);
				}
			} catch (Exception e) {
				throw new LogicException(
						"No Report Title row in file third row:"+FileName);
			}
			// get report release date from filename
			// try{
			// // String tmp[]=FileName.substring(0,10).split("-");
			// // reportTime=strToDate(tmp[2]+"/"+tmp[1]+"/"+tmp[0]);
			// reportTime=strToDateLong(FileName.substring(0,19));
			// }catch(ParseException e){
			// throw new LogicException("File Name date
			// part:"+FileName.substring(0,10)+" can not be converted to date
			// 'yyyy-MM-dd'.\n Please check file name!");
			// }

			String monStr = null;
			String yearStr = null;
			String tmp[] = null;
			String rteName = null;
			if ((contentList.get(1))[0] != null
					&& (contentList.get(1))[0].equals("Report Time")) {
				if (rprName
						.equals("World Agricultural Supply and Demand Estimates")) {
					try {

						if ((contentList.get(1))[1].indexOf(" ") > 0) {
							tmp = (contentList.get(1))[1].split(" ");
							monStr = tmp[0];
							yearStr = tmp[1];
							rteName = reportTitle.replaceAll(monStr,
									"Current Month's");
							reportTime = monthstrToDate(yearStr + "-" + monStr
									+ "-01");
						} else if ((contentList.get(1))[1].indexOf("-") > 0) {
							Properties monProp = new Properties();
							System.out.print("Loading commodity config file: "
									+ month_Config_File);
							FileInputStream mon_fis = new FileInputStream(
									month_Config_File);
							monProp.load(mon_fis);
							tmp = (contentList.get(1))[1].split("-");
							monStr = tmp[0];
							yearStr = tmp[1];
							if (yearStr.trim().length() == 2)
								yearStr = "20" + yearStr;
							String fullMonth = monProp.getProperty(monStr);
							rteName = reportTitle.replaceAll(fullMonth,
									"Current Month's");
							reportTime = monthstrToDate(yearStr + "-"
									+ fullMonth + "-01");
							;
						} else if ((contentList.get(1))[1].indexOf("/") > 0) {
							tmp = (contentList.get(1))[1].split("/");
							monStr = tmp[1];
							yearStr = tmp[2];
							reportTime = strToDate(yearStr + "-" + monStr + "-"
									+ tmp[0]);
						} else
							throw new LogicException(
									"WASDE:Report Time is not in 'mmm-yy' or 'mmmm yyyy' or 'dd/mm/yyyy' format in file:"+FileName);
					} catch (Exception e) {
						if (e instanceof FileNotFoundException)
							throw new LogicException("Config file:"
									+ month_Config_File + " not found!");
						else
							throw new LogicException(
									"WASDE:Report Time is not in 'mmm-yy' or 'mmmm yyyy' or 'dd/mm/yyyy' format in file:"+FileName);
					}
				} else {
					try {
						tmp = (contentList.get(1))[1].split("/");
						monStr = tmp[1];
						yearStr = tmp[2];
						reportTime = strToDate(yearStr + "-" + monStr + "-"
								+ tmp[0]);
					} catch (Exception e) {
						throw new LogicException(
								"Report Time is not in dd/mm/yyyy format in file:"+FileName);
					}

					rteName = reportTitle;
				}
			} else {
				throw new LogicException(
						"No Report Time row in this file!");
			}

			// get report_template id
			cs = thisConnection
					.prepareCall("{? = call report_distribution_pkg.get_report_template_fn(?,?)}");
			cs.registerOutParameter(1, Types.INTEGER);
			// cs.setString(2, reportTitle);
			cs.setString(2, rteName);// because some reports 35-37 includes
										// current Month
			cs.setInt(3, rprID);
			cs.execute();
			rteID = cs.getInt(1);
			cs.close();
			if (rteID == -1) {
				throw new LogicException(
						"No Record exists in Table Report_Template when Report="
								+ reportTitle + " and DATA_SET=" + rprName
								+ "!");
			}

			// get report Id
			if ((contentList.get(0))[0] != null
					&& (contentList.get(0))[0].equals("Report ID"))
				reportId = ((String[]) contentList.get(0))[1].trim();
			else {
				throw new LogicException(
						"No Report ID row in this file first row!");
			}

			// get report comment No.
			if ((contentList.get(3))[0] != null
					&& (contentList.get(3))[0].equals("Report Comments No."))
				reportCommentNo = (contentList.get(3))[1].trim();
			else {
				throw new LogicException(
						"No Report Comments No. row in  file forth row!");
			}
			// get report unit
			if ((contentList.get(4))[0] != null
					&& (contentList.get(4))[0].equals("Report Unit"))
				reportUnit = (contentList.get(4))[1].trim();
			else {
				throw new LogicException(
						"No Report Unit row in file fifth row!");
			}
			// get report unit comment, this row is optional
			if ((contentList.get(5))[0] != null
					&& (contentList.get(5))[0].equals("Unit Comments No."))
				reportUnitCommentNo = (contentList.get(5))[1].trim();

			// insert into report_release and get the rls_id, no commit
			long rlsID = -1;

			CallableStatement insRlsCs = thisConnection
					.prepareCall("{? = call report_maintain_pkg.insert_report_release_fn(?,?,?,?,?,?)}");
			insRlsCs.registerOutParameter(1, Types.BIGINT);
			insRlsCs.setInt(2, rteID);
			insRlsCs.setDate(3, reportTime);
			insRlsCs.setString(4, reportSource);
			insRlsCs.setString(5, "");
			insRlsCs.setString(6, "");
			insRlsCs.setString(7, reportId);
			insRlsCs.execute();
			rlsID = insRlsCs.getLong(1);
			insRlsCs.close();
			if (rlsID == -1) {
				throw new LogicException(
						"Insert into report_release failed or get the rls_id failed!");
			}

			// Get column headers
			String[] colHeader = new String[(contentList.get(10)).length];
			String[] colData = new String[(contentList.get(10)).length];

			for (int i = 0; i < (contentList.get(10)).length; i++) {
				if ((contentList.get(10))[i] != null
						&& (contentList.get(10))[i].trim().length() > 0)
					colHeader[i] = (contentList.get(10))[i].trim();
				else
					throw new LogicException(
							"Column Header is empty, column number is " + i
									+ "!");
			}

			String valueStr = null;
			String unitStr = null;
			String beginyearStr = null;
			String endyearStr = null;
			String monthStr = null;
			String quarterStr = null;
			String weekStr = null;
			String cropYTDStr = null;
			String acculevelStr = null;
			String firstDayofMonth = null;
			String lastDayofMonth = null;
			String mktYearStart = null;
			String mktYearEnd = null;
			String reportdateStr = null;
			String rptdateyearStr = null;
			String rptdatemonStr = null;
			String rptdatedayStr = null;
			/**
			 * This part is to process comments content which is at bottom of
			 * file.
			 */
			long rcoID = -1;

			Map<String, Long> commentIdMap = Collections
					.synchronizedMap(new HashMap<String, Long>());

			CallableStatement delRcmCs = thisConnection
			.prepareCall("{call report_maintain_pkg.delete_comment_mapping_proc(?)}");
			CallableStatement insRcoCs = thisConnection
					.prepareCall("{? = call report_maintain_pkg.insert_comment_fn(?,?,?)}");
			CallableStatement insRcmCs = thisConnection
					.prepareCall("{call report_maintain_pkg.insert_comment_mapping_proc(?,?,?)}");
			CallableStatement getUnitCs = thisConnection
					.prepareCall("{? = call report_distribution_pkg.get_measurement_fn(?)}");
			long rmeaID = -1;

			String reportComm[] = reportCommentNo.split(","); // report level
																// can have
																// multi
																// comments, the
																// delimiter is
																// comma
			String reportUnitComm[] = reportUnitCommentNo.split(",");
			int m;
			for (m = 11; m < contentList.size(); m++) // get the row number of
														// seperator between
														// data and comments
			{
				int emptyCount = 0;
				for (int n = 0; n < (contentList.get(m)).length; n++)
					if ((contentList.get(m))[n].equals("")
							|| (contentList.get(m))[n].length() <= 0)
						emptyCount++;
				if (emptyCount == (contentList.get(m)).length)
					break;
			}

			if (m == 11)
				throw new LogicException(
						"There is no data row in file!");

			int dataRowNum = m;
			/**
			 * Delete old comments mapping of this file if this file was processed before
			 */
			try{
				delRcmCs.setLong(1, rlsID);
				delRcmCs.execute();
				delRcmCs.close();
			}catch (SQLException e) {
					this.LogDetails(MsgCategory.WARN,
							"lanworth:  Delete report comment mapping failed when rls_id="
									+ rlsID
									+ "!"
									);
					logger.warn("lanworth:  Delete report comment mapping failed when rls_id="
							+ rlsID
							+ "!"
							);
			}

			/**
			 * Insert comments, and insert comment mapping for report comment
			 * and report unit comment which is report level, not cell level.
			 */

			for (int n = m + 1; n < contentList.size(); n++) {
				insRcoCs.registerOutParameter(1, Types.BIGINT);
				insRcoCs.setLong(2, rlsID);
				insRcoCs.setString(3, (contentList.get(n))[0]);
				insRcoCs.setString(4, (contentList.get(n))[1]);
				insRcoCs.execute();
				rcoID = insRcoCs.getLong(1);
				commentIdMap.put((contentList.get(n))[0], rcoID);
				if (rcoID == -1) {
					thisConnection.rollback();
					throw new LogicException(
							"Insert into or get report comment failed!");
				}

				for (int i = 0; i < reportUnitComm.length; i++) {
					if ((contentList.get(n))[0].equals(reportUnitComm[i])) {
						// get measurement id
						if (reportUnit != null
								&& reportUnit.trim().length() > 0) {
							getUnitCs.registerOutParameter(1, Types.BIGINT);
							getUnitCs.setString(2, reportUnit.toUpperCase());
							getUnitCs.execute();
							rmeaID = getUnitCs.getLong(1);
							if (rmeaID == -1) {
								this.LogDetails(MsgCategory.WARN,
										"lanworth:  Get report measurement ID of"
												+ reportUnit.toUpperCase()
												+ " failed!");
								logger.warn("lanworth:  Get report measurement ID of"
												+ reportUnit.toUpperCase()
												+ " failed!");
//								System.out
//										.print("lanworth:  Get report measurement ID of"
//												+ reportUnit.toUpperCase()
//												+ " failed!");
							} else {
								// insert comment mapping: report unit type
								try {
									insRcmCs.setLong(1, rcoID);
									insRcmCs
											.setString(2, reportUnitCommentType);
									insRcmCs
											.setString(3, Long.toString(rmeaID));
									insRcmCs.execute();
								} catch (SQLException e) {
									if (e.getMessage().indexOf("ORA-20031") < 0) {
										
										this.LogDetails(MsgCategory.WARN,
												"lanworth:  Insert report comment mapping failed when rco_id="
														+ rcoID
														+ " and comment type="
														+ reportUnitCommentType
														+ " and meaID="
														+ rmeaID + "!"
														);
										logger.warn("lanworth:  Insert report comment mapping failed when rco_id="
														+ rcoID
														+ " and comment type="
														+ reportUnitCommentType
														+ " and meaID="
														+ rmeaID + "!"
														);
//										System.out
//												.print("lanworth:  Insert report comment mapping failed when rco_id="
//														+ rcoID
//														+ " and comment type="
//														+ reportUnitCommentType
//														+ " and meaID="
//														+ rmeaID
//														+ "!"
//														);
									}
								}

							}
						} else {// when has report unit comment No. but has no
								// report unit
							this
									.LogDetails(MsgCategory.WARN,
											"lanworth:  Report unit comment has value but report unit is empty!");
							logger.warn("lanworth:  Report unit comment has value but report unit is empty!");
//							System.out
//									.print("lanworth:  Report unit comment has value but report unit is empty!");
						}
					}
				}

				for (int i = 0; i < reportComm.length; i++) {
					if ((contentList.get(n))[0].equals(reportComm[i])) {
						// insert comment mapping: report type
						try {
							insRcmCs.setLong(1, rcoID);
							insRcmCs.setString(2, reportCommentType);
							insRcmCs.setString(3, Long.toString(rteID));
							insRcmCs.execute();
						} catch (SQLException e) {
							if (e.getMessage().indexOf("ORA-20031") < 0) {
								
								this.LogDetails(MsgCategory.WARN,
										"lanworth:  Insert report comment mapping failed when rco_id="
												+ rcoID + " and comment type="
												+ reportCommentType
												+ " and rteID=" + rteID + "!"
												);
								logger.warn("lanworth:  Insert report comment mapping failed when rco_id="
												+ rcoID + " and comment type="
												+ reportCommentType
												+ " and rteID=" + rteID + "!"
												);
//								System.out
//										.print("lanworth:  Insert report comment mapping failed when rco_id="
//												+ rcoID
//												+ " and comment type="
//												+ reportCommentType
//												+ " and rteID="
//												+ rteID
//												+ "!"
//												);
							}

						}
					}
				}
			}
			insRcmCs.close();
			insRcoCs.close();
			getUnitCs.close();

			// Below is to process data rows, variables are to pass vector to
			// plsql function to get report_cell_id
			String[] dimAttr = new String[2];
			STRUCT dimStruct = null;
			String[] commentAttr = new String[2];
			STRUCT commentStruct = null;
			StructDescriptor structdesc = StructDescriptor.createDescriptor(
					"CEF_CNR.COMPACT_DIMENSION_ITEM_T", Conn);
			ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor(
					"CEF_CNR.COMPACT_DIMENSION_ITEM_LST_T", Conn);

			CallableStatement insertCellCs = Conn
					.prepareCall("{call feed_app_lanworth.insert_cell_value_proc(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			/*2013-5-29 
			 PROCEDURE insert_cell_value_proc(dimension_item_list_in     IN compact_dimension_item_lst_t,
                                   report_template_id_in      IN NUMBER,
                                   report_release_id_in       IN NUMBER,
                                   report_cell_value_in       IN VARCHAR2,
                                   value_accury_level_in      IN VARCHAR2,
                                   period_start_year_in       IN NUMBER,
                                   period_end_year_in         IN NUMBER,
                                   minor_period_start_in      IN VARCHAR2,
                                   minor_period_end_in        IN VARCHAR2,
                                   minor_period_unit_in       IN VARCHAR2,
                                   market_year_from_in        IN DATE,
                                   market_year_to_in          IN DATE,
                                   report_date_in             IN DATE,
                                   period_text_in             IN VARCHAR2,
                                   begin_date_in              IN DATE DEFAULT SYSDATE,
                                   comment_list_in            IN compact_dimension_item_lst_t,
                                   report_cell_measurement_in IN VARCHAR2,
                                   data_set_in                IN VARCHAR2)
			*/
			// int rowNumber=contentList.size();
			rowLoop: for (int j = 11; j < dataRowNum; j++) { // when the
																// first column
																// is null, then
																// it's an empty
																// row, below
																// which is the
																// comments rows
			// if((contentList.get(j))[0].equals("")||(contentList.get(j))[0].length()<=0)
			// break;
				try {
					valueStr = null;
					unitStr = null;
					Vector<Object> vector = new Vector<Object>();
					Vector<Object> commentVector = new Vector<Object>();
					// Because some line's (contentList.get(j)).length is less
					// than colCounts, so use (contentList.get(j)).length as
					// loop limit
					for (int k = 0; k < (contentList.get(j)).length; k++) {
						if (colHeader[k] != null
								&& colHeader[k].trim().length() > 0) {
							colData[k] = (contentList.get(j))[k].trim();
							if (colHeader[k].equals("begin_year")) // Begin
																	// Year
																	// column
								beginyearStr = colData[k].trim();
							else if (colHeader[k].equals("end_year")) // End
																		// Year
																		// column
								endyearStr = colData[k].trim();
							else if (colHeader[k].equals("market_year_start"))//USDA EXPORT SALES has market year start
								mktYearStart = colData[k].trim();
							
							else if (colHeader[k].equals("market_year_end"))
								mktYearEnd = colData[k].trim();
							
							else if (colHeader[k].equals("month")) // Month
																	// column
								monthStr = colData[k].trim();
							else if (colHeader[k].equals("quarter")) // Quarter
																		// column
								quarterStr = colData[k].trim();
							else if (colHeader[k].equals("week")) // Quarter
																	// column
								weekStr = colData[k].trim();
							else if (colHeader[k].equals("crop_year_to_date"))
								cropYTDStr = colData[k].trim();
							else if (colHeader[k].equals("accuration_level")) // Accuration
																				// Level
																				// column
							{
								if (colData[k] != null
										&& colData[k].trim().length() > 0)
									acculevelStr = colData[k].toUpperCase();
								else
									acculevelStr = "ACTUAL"; // if
																// accuration_level
																// is empty,
																// then set to
																// Actual
							} else if (colHeader[k].equals("value")) // Value
																		// column
							{
								if (colData[k] != null
										&& colData[k].trim().length() > 0)
									valueStr = colData[k].trim();
								else
									valueStr = "-"; // if value is empty, then
													// insert "-"
							} else if (colHeader[k].equals("unit")) // Unit
																	// column
								unitStr = colData[k].trim().toUpperCase();
								
							else if (colHeader[k].equals("report_date"))  //report date   added on 2013_5_27 for new dataset
							{	
							if (colData[k] != null
										&& colData[k].trim().length() > 0)
							{			
								reportdateStr = colData[k].trim();
								if (reportdateStr.indexOf("/") > 0)
								{
								//tmp = reportdateStr.split("/");
								rptdatemonStr = reportdateStr.split("/")[0];
								rptdateyearStr = reportdateStr.split("/")[1];
								rptdatedayStr  = reportdateStr.split("/")[2];
								}
							}
  
						}
								
							else if (colHeader[k].endsWith("_com")) // comment
																	// column
							{
								// when comment No. not null, and comments
								// content not null, and this comment No. has
								// comment content
								if (colData[k] != null
										&& colData[k].trim().length() > 0
										&& !commentIdMap.isEmpty()) {
									String cellComm[] = colData[k].trim()
											.split(","); // maybe multi
															// comment No. for
															// one cell
									multiCommLoop: for (int i = 0; i < cellComm.length; i++) {
										// if(commentIdMap.get(colData[k].trim())==null)
										if (commentIdMap
												.get(cellComm[i].trim()) == null) {
											// throw new LogicException("Comment
											// No. "+cellComm[i]+":
											// "+noCommentCon);
//											this
//													.LogDetails(
//															MsgCategory.WARN,
//															"lanworth:  Comment No. "
//																	+ cellComm[i]
//																	+ noCommentCon
//																	+ " Insert comment mapping failed for row "
//																	+ j
//																	+ " of file "
//																	+ FileName);
											logger.debug("lanworth:  Comment No. "
																	+ cellComm[i]
																	+ noCommentCon
																	+ " Insert comment mapping failed for row "
																	+ j
																	+ " of file "
																	+ FileName);
//											System.out
//													.print("lanworth:  Comment No. "
//															+ cellComm[i]
//															+ noCommentCon
//															+ " Insert comment mapping failed for row "
//															+ j
//															+ " of file "
//															+ FileName);
											continue multiCommLoop;
										}
										if (colHeader[k].indexOf("value") >= 0) {
											commentAttr[0] = cellCommentType;
										} else if (colHeader[k].indexOf("unit") >= 0) {
											commentAttr[0] = cellUnitCommentType;
										} else {
											commentAttr[0] = colHeader[k]
													.substring(
															0,
															colHeader[k]
																	.indexOf("_com"))
													.replace("_", " ")
													.toUpperCase();
											;
										}
										// commentAttr[1]=commentIdMap.get(colData[k].trim()).toString();
										commentAttr[1] = commentIdMap.get(
												cellComm[i].trim()).toString();
										commentStruct = new STRUCT(structdesc,
												Conn, commentAttr);
										commentVector.add(commentStruct);
									}// end of multi cell comment loop
								}// end of if comment No. exists
								else if (colData[k] != null
										&& colData[k].trim().length() > 0
										&& commentIdMap.isEmpty()) {
//									this
//											.LogDetails(
//													MsgCategory.WARN,
//													"lanworth:  Comment No.: "
//															+ colData[k]
//															+ noCommentCon
//															+ "  Insert comment mapping failed for row "
//															+ j + " of file "
//															+ FileName);
									logger.debug("lanworth:  Comment No.: "
															+ colData[k]
															+ noCommentCon
															+ "  Insert comment mapping failed for row "
															+ j + " of file "
															+ FileName);
//									System.out
//											.print("lanworth:  Comment No.: "
//													+ colData[k]
//													+ noCommentCon
//													+ "  Insert comment mapping failed for row "
//													+ j + " of file "
//													+ FileName);
								}
							}// end of comment No. column
							else // dimension item columns
							{
								// String emptyStr = "NA";
								// dimAttr[0] = rteID;
								if (colData[k] != null
										&& colData[k].length() > 0) {
									if (colHeader[k].indexOf("statistic") != -1) {
										dimAttr[0] = "STATISTIC";
									} else if (colHeader[k].toLowerCase()
											.equals("geographic_unit")) {
										dimAttr[0] = "GEOGRAPHY";
									} else {
										dimAttr[0] = colHeader[k].toUpperCase();
									}

									dimAttr[1] = colData[k].trim()
											.toUpperCase();
									 System.out.print(" dimAttr[1]= "+
									 dimAttr[1] );
									dimStruct = new STRUCT(structdesc, Conn,
											dimAttr);
									vector.add(dimStruct);
								}

							}
						}
					}// end of k loop
					// get report_cell id
					// System.out.println(" begin year="+beginyearStr);
					//below is to set statement parameters
					Object obj_array[] = vector.toArray();
					ARRAY array = new ARRAY(descriptor, Conn, obj_array);
					Object comm_array[] = commentVector.toArray();
					ARRAY commArray = new ARRAY(descriptor, Conn, comm_array);
					//new parameters created for market_year and report_date in historical_cell_value table
//					Date  period_start;
//					Date  period_end;
					Date  report_date= null;

					if (beginyearStr == null
							|| beginyearStr.trim().length() == 0)
						beginyearStr = yearStr;
					if (endyearStr == null || endyearStr.trim().length() == 0)
						endyearStr = yearStr;
					// if(monthStr==null || monthStr.trim().length()==0)
					// monthStr=monStr;
					// if(quarterStr==null || quarterStr.trim().length()==0)
					// quarterStr=monStr;
					String periodText = null;
					insertCellCs.setArray(1, array);
					insertCellCs.setInt(2, rteID);
					insertCellCs.setLong(3, rlsID);
					valueStr = valueStr.replace(",", "");
					// double valudDouble;
					// if(valueStr!="-")
					// try{
					// Double.parseDouble(valueStr);
					// }catch(Exception e)
					// {
					// this.LogDetails(MsgCategory.ERROR, "["+valueStr + "] is
					// not a valid number for value column of row "+ j +" of
					// file "+ FileName+".");
					// LoggerHelper.log(LogLevel.ERROR,
					// "["+valueStr + "] is not a valid number for value column
					// of row "+ j +" of file "+ FileName+".");
					// System.out.print("["+valueStr + "] is not a valid number
					// for value column of row "+ j +" of file "+ FileName+".");
					// continue;
					// }
					insertCellCs.setString(4, valueStr);
					insertCellCs.setString(5, acculevelStr);
					int beginYear=0;
					int endYear=0;
					if (beginyearStr != null && !beginyearStr.equals("")
							&& beginyearStr.trim().length() > 0) {
						if (beginyearStr.length() > 4){
							beginYear = Integer.parseInt(beginyearStr.substring(0, 4));
							insertCellCs.setInt(6, beginYear);
						}
						else{
							beginYear = Integer.parseInt(beginyearStr);
							insertCellCs.setInt(6, beginYear);
						}
					} else {
//						insertCellCs.setNull(6, java.sql.Types.INTEGER);
						this.LogDetails(MsgCategory.WARN,
								"lanworth:  Start year is empty for row " + j
										+ " of file " + FileName
										+ ". This row failed!");
						logger.warn("lanworth:  Start year is empty for row " + j
										+ " of file " + FileName
										+ ". This row failed!");
//						System.out
//								.print("lanworth:  Start year is empty for row "
//										+ j
//										+ " of file "
//										+ FileName
//										+ ". This row failed!");
						continue rowLoop;
					}

					if (endyearStr != null && !endyearStr.equals("")
							&& endyearStr.trim().length() > 0) {
						if (endyearStr.length() > 4){
							endYear = Integer.parseInt(endyearStr.substring(0, 4));
							insertCellCs.setInt(7, endYear);
						}
						else{
							endYear = Integer.parseInt(endyearStr);
							insertCellCs.setInt(7, endYear);
						}

						periodText = beginyearStr + "/" + endyearStr;
					} else {// when end year is null
//						insertCellCs.setNull(6, java.sql.Types.INTEGER);
						this.LogDetails(MsgCategory.WARN,
								"lanworth:  End year is empty for row " + j
										+ " of file " + FileName
										+ ". This row failed!");
						logger.warn("lanworth:  End year is empty for row " + j
										+ " of file " + FileName
										+ ". This row failed!");
//						System.out
//								.print("lanworth:  End year is empty for row "
//										+ j + " of file " + FileName
//										+ ". This row failed!");
						continue rowLoop;
					}
					
					if (monthStr != null && !monthStr.equals("")
							&& monthStr.trim().length() > 0) {

						if (monthHm.get(monthStr.toUpperCase()) != null)
							monthStr = monthHm.get(monthStr.toUpperCase());
						firstDayofMonth = monthStr + "-01";
						String lastDayBeginyear =  LastDayOfMonth(Integer
								.parseInt(beginyearStr), Integer
								.parseInt(monthStr)).substring(5) ;
						String lastDayEndyear =  LastDayOfMonth(Integer
								.parseInt(endyearStr), Integer
								.parseInt(monthStr)).substring(5) ;
						 
						if (lastDayBeginyear.compareTo(lastDayEndyear) <= 0)
							lastDayofMonth = lastDayBeginyear;
						else
							lastDayofMonth = lastDayEndyear;

						// lastDayofMonth=lastDayofMonth.substring(5);
						insertCellCs.setString(8, firstDayofMonth);
						insertCellCs.setString(9, lastDayofMonth);
						insertCellCs.setString(10, "MONTH");
						
						insertCellCs.setDate(11, strToDate(beginyearStr+"-"+firstDayofMonth));
						insertCellCs.setDate(12, strToDate(endyearStr+"-"+lastDayofMonth));
						insertCellCs.setString(14, periodText + " " + monthStr);
						
					} else if (quarterStr != null && !quarterStr.equals("")
							&& quarterStr.trim().length() > 0) {

						int beginMonInt = (Integer.parseInt(quarterStr) - 1) * 3 + 1;
						int endMonInt = (Integer.parseInt(quarterStr)) * 3;
						String mon=null;
						if (beginMonInt < 10)
							mon = "0" + beginMonInt;
						else
							mon = Integer.toString(beginMonInt);
						firstDayofMonth = mon + "-01";
						lastDayofMonth = (LastDayOfMonth(Integer
								.parseInt(beginyearStr), endMonInt))
								.substring(5);
						insertCellCs.setString(8, firstDayofMonth);
						insertCellCs.setString(9, lastDayofMonth);
						insertCellCs.setString(10, "QUARTER");
						insertCellCs.setDate(11,strToDate(beginyearStr+"-"+firstDayofMonth) );
						insertCellCs.setDate(12,strToDate(beginyearStr+"-"+lastDayofMonth));
						insertCellCs.setString(14, periodText + " "
								+ quarterStr);
					} else if (weekStr != null && !weekStr.equals("")
							&& weekStr.trim().length() > 0) {
						String weekArr[] = weekStr.split("-");
						int weekMon;
						int weekDate;
						int beginyearInt;
						int endYearInt;
						// Date dateWeek=null;
						try {
							weekMon = Integer.parseInt(weekArr[0]);
							weekDate = Integer.parseInt(weekArr[1]);
							// beginyearInt =
							// Integer.parseInt(beginyearStr.substring(0, 4));
							// endYearInt =
							// Integer.parseInt(endyearStr.substring(0, 4));
							// dateWeek = strToDate(beginyearStr+"-"+weekStr);
						} catch (Exception e) {
							this
									.LogDetails(
											MsgCategory.WARN,
											"lanworth:  Week:"
													+ weekStr
													+ " is not in mm-dd format for row "
													+ j + " of file "
													+ FileName + "! "
													);
							logger.warn("lanworth:  Week:"
									+ weekStr
									+ " is not in mm-dd format for row " + j
									+ " of file " + FileName + "! ");
//							System.out.print("lanworth:  Week:" + weekStr
//									+ " is not in mm-dd format for row " + j
//									+ " of file " + FileName + "! ");
							continue rowLoop;
						}

						// int week = getWeekOfYear(dateWeek);
						// String
						// weekFirstDate=getFirstLastDayOfWeek(beginyearInt,week,true);
						// String
						// weekLastDate=getFirstLastDayOfWeek(beginyearInt,week,false);
						// int yearInt = Integer.parseInt(yearStr);
						// if(beginyearInt==endYearInt)
						// yearInt = beginyearInt;
						// String
						// weekFirstDate=getSixDaysBefore(yearInt,weekMon,weekDate);
						// int newBeginYear =
						// Integer.parseInt(weekFirstDate.substring(0,4));
						// int newEndYear =
						// Integer.parseInt(weekLastDate.substring(0,4));
						// if(newBeginYear<beginyearInt)
						// insertCellCs.setInt(6,newBeginYear);
						// if(newEndYear>endYearInt)
						// insertCellCs.setInt(7,newEndYear);
						// insertCellCs.setString(8,
						// weekFirstDate.substring(5));
						// insertCellCs.setString(9, weekLastDate.substring(5));
						insertCellCs.setString(8, weekStr);
						insertCellCs.setString(9, weekStr);// the week date in
															// the file is put
															// in
															// minor_period_end
															// field
						insertCellCs.setString(10, "WEEK");
						if (Integer.parseInt(beginyearStr) == Integer.parseInt(endyearStr)){
							//if  begin year=end year then use beginyear+week
							insertCellCs.setDate(11,strToDate(beginyearStr+"-"+weekStr));
							insertCellCs.setDate(12,strToDate(beginyearStr+"-"+weekStr));
						}else{          // else  use reportime
						insertCellCs.setDate(11,strToDate(reportTime.toString().substring(0,5)+weekStr));
						insertCellCs.setDate(12,strToDate(reportTime.toString().substring(0,5)+weekStr));
						}
						insertCellCs.setString(14, periodText + " " + weekStr);
					} else if (cropYTDStr != null && !cropYTDStr.equals("")
							&& cropYTDStr.trim().length() > 0) {
						// String weekArr[] = weekStr.split("-");
						// int weekMon;
						// int weekDate;
						// int yearInt;
						// try{
						// weekMon = Integer.parseInt(weekArr[0]);
						// weekDate = Integer.parseInt(weekArr[1]);
						// yearInt = Integer.parseInt(beginyearStr);
						// }catch(Exception e){
						// this.LogDetails(MsgCategory.ERROR, "Week:"+weekStr+"
						// is not in mm-dd format for row "+ j +" of file "+
						// FileName + ": " + e.getMessage());
						// LoggerHelper.log(LogLevel.ERROR,
						// "Week:"+weekStr+" is not in mm-dd format for row "+ j
						// +" of file "+ FileName + ": " + e.getMessage());
						// System.out.print("Week:"+weekStr+" is not in mm-dd
						// format for row "+ j +" of file "+ FileName + ": " +
						// e.getMessage());
						// continue rowLoop;
						// }
						// int week = weekOfYear(yearInt,weekMon ,weekDate);
						String YTDFirstDate = "01-01";
						// String YTDEndDate=cropYTDStr;
						// String weekLastDate=getDayByWeek(yearInt,week,false);
						insertCellCs.setString(8, YTDFirstDate);
						insertCellCs.setString(9, cropYTDStr.substring(5));
						insertCellCs.setString(10, "YEAR TO DATE");
						insertCellCs.setDate(11,strToDate(cropYTDStr.substring(0, 5)+YTDFirstDate));
						insertCellCs.setDate(12,strToDate(cropYTDStr));
						insertCellCs.setString(14, beginyearStr + "/"
								+ cropYTDStr);
					} else {
						insertCellCs.setString(8, "01-01");
						insertCellCs.setString(9, "12-31");
						insertCellCs.setString(10, "YEAR");
						insertCellCs.setDate(11,strToDate(beginYear+"-01-01"));
						insertCellCs.setDate(12,strToDate(endYear+"-12-31"));
						insertCellCs.setString(14, periodText);
					}
					
//					 for all datasets, if file has mktYearStart and mktYearEnd, will insert them into year_start/year_end, minor_period_start/end
					String MarketYearStart = null;
					String MarketYearEnd = null;
					if(mktYearStart !=null && !mktYearStart.equals("") && mktYearEnd !=null && !mktYearEnd.equals("")){
						
						String[] tmpStart = mktYearStart.split("\\.");
						String[] tmpEnd = mktYearEnd.split("\\.");
						
						String beginYearStr = tmpStart[0];
						String endYearStr = tmpEnd[0];
						if(tmpStart[1].length()==1) tmpStart[1] = '0'+tmpStart[1];//month should be 2 digit
						if(tmpStart[2].length()==1) tmpStart[2] = '0'+tmpStart[2];//day should be 2 digit
						if(tmpEnd[1].length()==1) tmpEnd[1] = '0'+tmpEnd[1];
						if(tmpEnd[2].length()==1) tmpEnd[2] = '0'+tmpEnd[2];
						
						MarketYearStart = tmpStart[0]+"-"+tmpStart[1]+"-"+tmpStart[2];
						MarketYearEnd = tmpEnd[0]+"-"+tmpEnd[1]+"-"+tmpEnd[2];
						
						insertCellCs.setInt(6, Integer.parseInt(beginYearStr));
						insertCellCs.setInt(7, Integer.parseInt(endYearStr));
						
					}
					
					Date  marketYearStart= null;
					Date  marketYearEnd= null;
					if(MarketYearStart != null){ 
						marketYearStart = strToDate(MarketYearStart);
						insertCellCs.setDate(11, marketYearStart);
					}
					if(MarketYearEnd != null){ 
						marketYearEnd = strToDate(MarketYearEnd);
						insertCellCs.setDate(12, marketYearEnd);
					}
					
					//below is to get report_date
					if(rprName.equals("World Agricultural Supply and Demand Estimates")){
//						insertCellCs.setString(10, "MONTH");//ALL WASDE files are month data
//						period_start = strToDate(beginYear+"-01-01");
//						period_end = strToDate(endYear+"-12-31");
//						acculevelStr
						String reportTimeArr[] = reportTime.toString().split("-");
						if(monthStr != null && !monthStr.equals("")){							
							//month not null
								  if(acculevelStr.equals("PROJECTION")){
										if(reportTimeArr[1].equals(monthStr)) //month in report_time == month column
											report_date = reportTime;
										else if(Integer.parseInt(monthStr) < Integer.parseInt(reportTimeArr[1]))
		//									if month is smaller than month in report time,then get the year from report time
											report_date = strToDate(reportTimeArr[0] + "-"+ monthStr + "-"+ reportTimeArr[2]);
										else {//if month is larger than month in report time
											if(Integer.parseInt(monthStr)==12 &&  Integer.parseInt(reportTimeArr[1])==1){
	    										int year =Integer.parseInt(reportTimeArr[0]) - 1;									
												report_date = strToDate(year + "-"+ monthStr + "-"+ reportTimeArr[2]);
										    }else{//when report_time=Jan, month=Feb.
										    	report_date = strToDate(reportTimeArr[0] + "-"+ monthStr + "-"+ reportTimeArr[2]);
										    }
										}
								  }else if(acculevelStr.equals("ESTIMATE")){//estimate
									  
									    int year =Integer.parseInt(reportTimeArr[0]) - 1;			
									    if(reportTimeArr[1].equals(monthStr)||(Integer.parseInt(monthStr) < Integer.parseInt(reportTimeArr[1])))
									    	//month column<=month in report_time  
									    	report_date = strToDate(year + "-"+ monthStr + "-"+ reportTimeArr[2]);
										else 									
											//if month is larger than month in report time,then set report year-1 as report year																	
											report_date = strToDate((year-1) + "-"+ monthStr + "-"+ reportTimeArr[2]);
								  }								
						}//end of month not null
//						else if (quarterStr != null && !quarterStr.equals("")
//								&& quarterStr.trim().length() > 0) {
//						         
//						}
//						
					  }// end of if WASDE
					else{//for other week datasets
						if(weekStr != null && !weekStr.equals("")){		
							if(MarketYearStart !=null && MarketYearStart !=null){
							
								String StartYearMonStr = MarketYearStart.substring(5,7);
								String EndYearMonStr = MarketYearEnd.substring(5,7);
								String WeekMonStr = weekStr.substring(0,2);
								if(Integer.parseInt(WeekMonStr)>=Integer.parseInt(StartYearMonStr))
								   report_date  = strToDate(MarketYearStart.substring(0,4)+"-"+weekStr);
								else 
								   report_date  = strToDate(MarketYearEnd.substring(0,4)+"-"+weekStr);
															
							}
							else if(Integer.parseInt(beginyearStr) == Integer.parseInt(endyearStr)){
								// if begin year = end year
								report_date = strToDate(beginyearStr+"-"+weekStr);
							}
							else{
							String year = reportId.substring((reportId.length()-4),reportId.length());
//							period_start = strToDate(year+weekStr);
//							period_end = strToDate(year+weekStr);
							report_date = strToDate(year+"-"+weekStr);			
							}
							
						}
					}
									

										
					if(report_date == null) report_date = reportTime;//if report_date is null, then set as  reportTime
					
					if (reportdateStr != null && !reportdateStr.equals("")&& reportdateStr.trim().length() > 0) // if report date column is not null
					{ 
					report_date = strToDate(rptdateyearStr + "-" + rptdatemonStr + "-"
									+ rptdatedayStr);;
					}
					insertCellCs.setDate(13, report_date);
					insertCellCs.setTimestamp(15, nowTime);
					insertCellCs.setArray(16, commArray);
					if(unitStr == null || unitStr.equals(""))
						unitStr = reportUnit.toUpperCase();
					insertCellCs.setString(17, unitStr);
					insertCellCs.setString(18, rprName);
					
					try {
						insertCellCs.execute();
						System.out.println("value="+valueStr);
					} catch (SQLException e) {
						
						this.LogDetails(MsgCategory.WARN,
								"lanworth:  Execute failed for row " + j
										+ " of file " + FileName + ": "
										+ e.getMessage());
						logger.warn("lanworth:  Execute failed for row " + j
										+ " of file " + FileName + ": "
										+ e.getMessage());
						System.out.print("lanworth:  Execute failed for row "
								+ j + " of file " + FileName + ": "
								+ e.getMessage());
					}
					processedRowsNumber++;
					if (processedRowsNumber == rowsOfCommit) {
						try {
							// insertCellCs.execute();
							thisConnection.commit();
							// insertCellCs.clearBatch();
							processedRowsNumber = 0;
						} catch (SQLException e) {
							
							this.LogDetails(MsgCategory.WARN,
									"lanworth:  Commit failed for row " + j
											+ " of file " + FileName + ": "
											+ e.getMessage());
							logger.warn("lanworth:  Commit failed for row " + j
											+ " of file " + FileName + ": "
											+ e.getMessage());
							System.out
									.print("lanworth:  Commit failed for row "
											+ j + " of file " + FileName + ": "
											+ e.getMessage());

						}

					}
				} catch (NumberFormatException e) {
					this
							.LogDetails(
									MsgCategory.WARN,
									"lanworth:  "
											+ monthStr
											+ " in Month column or "
											+ quarterStr
											+ " in Quarter column"
											+ " is not a valid month(full month name) or quarter(1,2,3,4) for row "
											+ j + " of file " + FileName + ": "
											+ e.getMessage());
					logger.warn("lanworth:  "
											+ monthStr
											+ " in Month column or "
											+ quarterStr
											+ " in Quarter column"
											+ " is not a valid month(full month name) or quarter(1,2,3,4) for row "
											+ j + " of file " + FileName + "!"
											 );
					System.out
							.print("lanworth:  "
									+ monthStr
									+ " in Month column or "
									+ quarterStr
									+ " in Quarter column"
									+ " is not a valid month(full month name) or quarter(1,2,3,4) for row "
									+ j + " of file " + FileName + "!"
									 );

				}

			}// end of j loop
	
			//insert week number for Crop Progress
			if(rprName.equals("USDA - Crop Progress")||rprName.equals("Grain Statistics Weekly")||rprName.equals("Agricultural Marketing Service")){
				Vector<Object> vector = new Vector<Object>();			
				dimAttr[0] = "COMMODITY";
				if (rprName.equals("USDA - Crop Progress")){
					dimAttr[1] = "NASS";			
				}else if(rprName.equals("Grain Statistics Weekly")){
					dimAttr[1] = "GSW";
				}
				else
				{
					dimAttr[1] = "REPORT";
				}
				dimStruct = new STRUCT(structdesc, Conn,
						dimAttr);
				vector.add(dimStruct);
				
				dimAttr[0] = "GEOGRAPHY";
				if(rprName.equals("Grain Statistics Weekly")){
					dimAttr[1] = "CANADA";
				}
				else {
					dimAttr[1] = "WORLD";			
				}
				
				dimStruct = new STRUCT(structdesc, Conn,
						dimAttr);
				vector.add(dimStruct);
				
				dimAttr[0] = "STATISTIC";
				dimAttr[1] = "WEEK NUMBER";
				dimStruct = new STRUCT(structdesc, Conn,
						dimAttr);
				vector.add(dimStruct);			
				
				Object obj_array[] = vector.toArray();
				ARRAY array = new ARRAY(descriptor, Conn, obj_array);
				
				insertCellCs.setArray(1, array);
				insertCellCs.setInt(2, rteID);
				insertCellCs.setLong(3, rlsID);				
				valueStr = reportId.substring(reportId.trim().length() - 7, reportId.trim().length() - 5).trim();
				String strYear = reportId.substring(reportId.trim().length() - 4);
				String strMM = reportTime.toString().substring(5, 7);
				String strDD = reportTime.toString().substring(8, 10);
				int year = Integer.parseInt(strYear);
				insertCellCs.setString(4, valueStr);///---------reportId
				insertCellCs.setString(5, "ACTUAL");//actual
				insertCellCs.setInt(6, year);
				insertCellCs.setInt(7, year);
				insertCellCs.setString(8, strMM + "-" + strDD);  //reportTime  mm-dd
				insertCellCs.setString(9, strMM + "-" + strDD);
				insertCellCs.setString(10, "WEEK");
				String periodText = year + "-" + strMM + "-" + strDD;
				insertCellCs.setDate(11, strToDate(periodText));
				insertCellCs.setDate(12, strToDate(periodText));
				insertCellCs.setString(14, periodText);
				
				Date report_date;			
				report_date = strToDate(periodText);			
				insertCellCs.setDate(13, report_date);
				insertCellCs.setTimestamp(15, nowTime);
	//			insertCellCs.setArray(16, null);
				insertCellCs.setString(17, "NUMBER");
				insertCellCs.setString(18, rprName);			
			
				try {
					insertCellCs.execute();
				} catch (SQLException e) {
					 
					this.LogDetails(MsgCategory.WARN,
							"lanworth:  Execute failed for row WEEK Number"
									+ " of file " + FileName + ": "
									+  e.getMessage());
					logger.warn("lanworth:  Execute failed for row WEEK Number"
									+ " of file " + FileName + ": "
									+ e.getMessage());
//					System.out.print("lanworth:  Execute failed for row WEEK Number"
//							+ " of file " + FileName + ": "
//							+ LoggerHelper.escapeLF(e.getMessage()));
				}
			}// end of if USDA - Crop Progress

			// process the rest rows of file
			try {
				// insertCellCs.executeBatch();
				// // insertCellCs.execute();
				thisConnection.commit();
			} catch (SQLException e) {
				// int beginRow = rowNumber - (rowNumber + 1) % rowsOfCommit +
				// 1;
				// int endRow = rowNumber + 1;
				 
				this.LogDetails(MsgCategory.WARN,
						"lanworth: Commit failed of file " + FileName + ": "
								+ e.getMessage());
				logger.warn("lanworth: Commit failed of file " + FileName + ": "
								+ e.getMessage());
//				System.out.print("lanworth: Commit failed of file " + FileName
//						+ ": " + e.getMessage());
			}
			insertCellCs.close();

			if (rprName.indexOf("NOPA") >= 0) {
				CallableStatement aggCs;
				aggCs = thisConnection
						.prepareCall("{call REPORT_CALCULATION_PKG.periodical_aggregation_proc(?,?,?,?,?,?,?,?)}");
				aggCs.setString(1, rprName);
				aggCs.setString(2, reportTitle);
				aggCs.setDate(3, reportTime);
				// calculate YEARLY aggregation
				if(monthStr == null || monthStr.trim().length()==0)
				{
					this.LogDetails(MsgCategory.WARN,
							"lanworth: Month is empty for NOPA file: " + FileName
									+ ". Will not calculate yearly aggregation.");
					logger.warn("lanworth: Month is empty for NOPA file: " + FileName
							+ ". Will not calculate yearly aggregation.");
//					System.out
//							.print("lanworth: Month is empty for NOPA file: " + FileName
//									+ ". Will not calculate yearly aggregation.");
				}
				else if (monthStr.equals("12")) {
					try {
						int Year = Integer.parseInt(beginyearStr);
						Date PeriodStart = strToDate(String.valueOf(Year)
								+ "-01-01");
						Date PeriodEnd = strToDate(String.valueOf(Year)
								+ "-12-31");
						aggCs.setDate(4, PeriodStart);
						aggCs.setDate(5, PeriodEnd);
						aggCs.setString(6, "YEAR");
						aggCs.setString(7, "MONTH");
						aggCs.setString(8, acculevelStr);
						aggCs.execute();
						thisConnection.commit();
					} catch (ParseException e) {
						throw new LogicException(
								"Month: "
										+ monthStr
										+ ", year: "
										+ beginyearStr
										+ " are not valid format. Can't execute YEARLY aggregation!");
					}
				}
				// calculate YTD aggregation
				try {
					Date PeriodStart = strToDate(beginyearStr + "-01-01");
					Date PeriodEnd = strToDate(beginyearStr
							+ "-"
							+ monthStr
							+ "-"
							+ (LastDayOfMonth(Integer.parseInt(beginyearStr),
									Integer.parseInt(monthStr))).substring(8));
					aggCs.setDate(4, PeriodStart);
					aggCs.setDate(5, PeriodEnd);
					aggCs.setString(6, "YEAR TO DATE");
					aggCs.setString(7, "MONTH");
					aggCs.setString(8, acculevelStr);
					aggCs.execute();
					thisConnection.commit();
					aggCs.close();
				} catch (ParseException e) {
					throw new LogicException(
							"Month: "
									+ monthStr
									+ ", year: "
									+ beginyearStr
									+ " are not valid format. Can't execute YTD aggregatione!");
				}

			}// end of if "NOPA"
		}// end of try

		catch (SQLException e) {
			throw new SystemException(e.getMessage(), e);

		} catch (Exception e) {
			throw new SystemException(e.getMessage(), e);
		} finally {
			try {

				thisConnection.close();
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(), e);
			}

		}

	}

	public void Finalize() {

	}

	// public java.sql.Timestamp strToDateLong(String strDate) throws
	// ParseException
	// {
	// SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd
	// hh-mm-ss",Locale.ENGLISH);
	// ParsePosition pos = new ParsePosition(0);
	// java.util.Date a=formatter.parse(strDate,pos);
	// java.sql.Date date = new java.sql.Date(a.getTime());
	// java.sql.Time time = new java.sql.Time(a.getTime());
	// java.sql.Timestamp longdate=java.sql.Timestamp.valueOf(date.toString()+"
	// "+time.toString());
	// return longdate;
	// }
	public java.sql.Date strToDate(String strDate) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd",
				Locale.ENGLISH);
		java.util.Date date = formatter.parse(strDate);
		java.sql.Date sqlDate = new java.sql.Date(date.getTime());
//		java.sql.Date sqlDate = java.sql.Date.valueOf( strDate );
        
		return sqlDate;
	}

	public java.sql.Date monthstrToDate(String strDate) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MMMMM-dd",
				Locale.ENGLISH);
		java.util.Date date = formatter.parse(strDate);
		java.sql.Date sqlDate = new java.sql.Date(date.getTime());

		return sqlDate;
	}

	public String LastDayOfMonth(int year, int month) {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();

		lastDate.set(year, month, 1);// set as the first day of that month
		// lastDate.add(Calendar.MONTH,1);//add 1 month, then become the first
		// day of next month
		lastDate.add(Calendar.DATE, -1);// minus 1 day, then become the last day
										// of the month
		str = sdf.format(lastDate.getTime());
		return str;
	}
	


	public String getSixDaysBefore(int year, int month, int date) {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();

		lastDate.set(year, month, date);// set as the date
		lastDate.add(Calendar.MONTH, -1);// minus 1 month
		lastDate.add(Calendar.DATE, -6);// minus 6 day
		str = sdf.format(lastDate.getTime());
		return str;
	}

	// public int weekOfYear(int year, int month, int date)
	// {
	// Calendar cale = Calendar.getInstance();
	// // SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	// // try {
	// // cale.setTime(format.parse(year+"-"+month+"-"+date));
	// // } catch (ParseException e){
	// // e.printStackTrace();
	// // }
	//
	// cale.set(year, month-1, date);
	// int week = cale.get(Calendar.WEEK_OF_YEAR);
	// return week;
	// }
	// /**
	// * get the first day and last day of the week of the year
	// * @param year
	// * @param week: the number of week
	// * @param flag : when it's true, return the first day of week, false
	// return the last day of week
	// * @return
	// */
	// public String getDayByWeek(int year,int week,boolean flag)
	// {
	// String str = "";
	// SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
	//          
	// Calendar cal=Calendar.getInstance();
	// cal.set(Calendar.YEAR,year);
	// if (flag)
	// {
	// cal.set(Calendar.WEEK_OF_YEAR,week);
	// cal.set(Calendar.DAY_OF_WEEK,1);
	// }
	// else
	// {
	// cal.set(Calendar.WEEK_OF_YEAR,week);
	// cal.set(Calendar.DAY_OF_WEEK,7);
	// }
	//		  
	// str=sdf.format(cal.getTime());
	// return str;
	// }

	// public String getDayOfWeek(int year,int month, int date,boolean flag)
	// {
	// String str = "";
	// SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
	//         
	// Calendar cal=new GregorianCalendar();
	// cal.setFirstDayOfWeek(Calendar.SUNDAY);
	// cal.set(year, month, date);
	//		  
	// // int week = cal.get(Calendar.WEEK_OF_YEAR);
	// // cal.set(Calendar.YEAR,year);
	//		 
	// if (flag)
	// {
	// cal.set(cal.getActualMaximum(Calendar.DAY_OF_WEEK),
	// cal.getFirstDayOfWeek());
	// }
	// else
	// {
	// cal.set(cal.getActualMaximum(Calendar.DAY_OF_WEEK),
	// cal.getFirstDayOfWeek() + 6);
	// }
	//		  
	// str=sdf.format(cal.getTime());
	// return str;
	// }

	/**
	 * get the week of some day
	 * 
	 * @param date
	 * @return
	 */
	public static int getWeekOfYear(Date date) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.setMinimalDaysInFirstWeek(7);
		c.setTime(date);
		return c.get(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * get first day of some day's week
	 * 
	 * @param date
	 * @return
	 */
	public static java.util.Date getFirstDayOfWeek(java.util.Date date) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.SUNDAY);
		c.setTime(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		return c.getTime();
	}

	/** */
	/**
	 * get last day of some day's week
	 * 
	 * @param date
	 * @return
	 */
	public static java.util.Date getLastDayOfWeek(java.util.Date date) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.SUNDAY);
		c.setTime(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6);
		return c.getTime();
	}

	/** 
	 * get the first day and last day of one week in one year, if flag=true, it's first day, else it's last day
	 * for 2008-12-28-- 2009-01-03,it's last week of 2008,2009-01-04 is the first day of first week of 2009 
	 * @param year 
	 * @param week 
	 * @return 
	 */
	public static String getFirstLastDayOfWeek(int year, int week, boolean flag) {
		Calendar calFirst = Calendar.getInstance();
		calFirst.set(year, 0, 7);
		java.util.Date firstDate;
		java.util.Date lastDate;
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		if (flag)
			firstDate = getFirstDayOfWeek(calFirst.getTime());
		else
			firstDate = getLastDayOfWeek(calFirst.getTime());

		Calendar firstDateCal = Calendar.getInstance();
		firstDateCal.setTime(firstDate);

		Calendar c = new GregorianCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, Calendar.JANUARY);
		c.set(Calendar.DATE, firstDateCal.get(Calendar.DATE));

		Calendar cal = (GregorianCalendar) c.clone();
		cal.add(Calendar.DATE, (week - 1) * 7);
		if (flag) {
			firstDate = getFirstDayOfWeek(cal.getTime());
			str = sdf.format(cal.getTime());
		} else {
			lastDate = getLastDayOfWeek(cal.getTime());
			str = sdf.format(cal.getTime());
		}

		return str;
	}

	public java.sql.Timestamp currentDate() {
		java.util.Date longdate = new java.util.Date();
		java.sql.Date date = new java.sql.Date(longdate.getTime());
		java.sql.Time time = new java.sql.Time(longdate.getTime());
		java.sql.Timestamp curdate = java.sql.Timestamp.valueOf(date.toString()
				+ " " + time.toString());

		return curdate;
	}
}
//	public boolean isNumeric(String str) {   
//		 Pattern pattern = Pattern.compile("[0-9]*");   
//		 return pattern.matcher(str).matches();   
//		    }  

//	public java.sql.Date shortMonthstrToDate(String strDate) throws ParseException 
//	{ 
//		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MMM-dd",Locale.ENGLISH);
//		java.util.Date date = formatter.parse(strDate);
//		java.sql.Date sqlDate=new java.sql.Date(date.getTime());			   
//
//	     return sqlDate;		
//	} 	
//	public java.sql.Date strToShortDate(String strDate) throws ParseException 
//	{ 
//		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yy",Locale.ENGLISH);
//		java.util.Date date = formatter.parse(strDate);
//		java.sql.Date sqlDate=new java.sql.Date(date.getTime());			   
//
//	     return sqlDate;		
//	} 	

//	public String DateToStr( java.sql.Date dDate) throws ParseException 
//	{ 
//		SimpleDateFormat formatter = new SimpleDateFormat("dd/MMMMM/yy",Locale.ENGLISH);		   
//        String strDate=formatter.format(dDate);
//	    return strDate;		
//	} 	

