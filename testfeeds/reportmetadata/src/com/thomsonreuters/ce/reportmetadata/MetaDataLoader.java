/**
 * @(#)MetaDataLoader.java	1.0.0 24/09/2010
 *
 * Copyright 2010 Thomsonreuters, Inc. All rights reserved.
 * 
 * THOMSONREUTERS PROPRIETARY/CONFIDENTIAL. 
 * 
 */

package com.thomsonreuters.ce.reportmetadata;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Vector;

import org.apache.log4j.Logger;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import jxl.Sheet;
import jxl.Cell;
import jxl.CellType;
import jxl.DateCell;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.file.ExcelProcessor;

/**
 * This is sub class of
 * <code>ExcelProcessor<code> which provides common interfaces to process excel datafiles.
 * It achieves the functionalitiy that is aimed to parse specific tab of report model configuration spreadsheet 
 * files and load configuration data into correspanding database tables properly with dedicated report model 
 * interfaces by implementing <code>Initialize<code>,<code>Finalize<code> and <code>ProcessSheet<code> methods
 * of super class. To reduce the overhead of stream replication from CNR to APDS databse,it actually commits 
 * changes to database after specified number of rows that has been processed. Any error raised during the 
 * process will be record into database log tables. 
 * 
 * 
 * @author   Weizhen.Jin
 * @version 1.0.0 , 24/09/2010
 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor
 *
 */

public class MetaDataLoader extends ExcelProcessor {
	private Logger logger = Starter.thisLogger;
	/**
	 * Private database connection variable that will be only one connection to
	 * database,even multiple sheets are processed.
	 */
	private Connection conn = null;

	/**
	 * Private string variable that is holding dataSetFullName after dataa set
	 * sheet is porocessed, and it will be used later when processing rest of
	 * sheets.
	 */
	private String dataSetFullName = null;

	/**
	 * Private string variable that is holding dataSetAlias after data set sheet
	 * is porocessed, and it will be used later when processing dimension item
	 * and report cell sheets.
	 */
	private String dataSetAlias = null;
	
	/**
	 * Private string variable that is holding name of reportTemplate after
	 * report template sheet is porocessed,and it will be used later when
	 * processing rest sheets.
	 */
	private String reportTemplate = null;	
	/**
	 * Private int variable that is holding number of processed rows for each
	 * issued commit.
	 */
	private int rowsOfCommit=2000;
	/**
	 * Private boolean variable that is to indicate if it is in debug mode.
	 * 
	 */
	private boolean debug=false;
	private String fileName=null;
	
	/**
	 * Public method that implempents super class interface, and is responsible
	 * for returning specific instance of <code>FileCategory<code>.
	 * 
	 * @see com.thomsonreuters.ce.feeds.actions.FileProcessor#getFileCatory()
	 */
	public FileCategory getFileCatory(File FeedFile) {
		
		return FileCategory.getInstance("REPORT METADATA TOOL");
	}
	
    /**
	 * Public method that implempents super class interface, and is responsible
	 * for initializing database connection.
	 * 
	 * @exception throw <code>SystemException<code> expection with failed database connection initializing message.             
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#Initialize(java.io.File).
	 */
	public void Initialize(File FeedFile) {
        
		// set up DB connection
		try {
			
			conn = new EasyConnection(DBConnNames.CEF_CNR);
			conn.setAutoCommit(false);
			fileName = FeedFile.getName();
			
		} catch (Exception e) {
			
			logger.fatal("ReportMetaData: Failed when initilizing DB connection: "+e.getMessage());
			
			throw new SystemException("DB connection initilizing failed",e);
		}
	}
	
	/**
	 * Public method that implempents the super class interface, and is responsible
	 * for closing database connection.
	 * 
	 * @exception throw <code>SystemException<code> expection with failed database connection closing message.              
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#Finalize()
	 */
	public void Finalize() {
     
		// close DB connection
		try {
			
			conn.setAutoCommit(true);
			conn.close();
			
		} catch (SQLException e) {
			
			logger.fatal("ReportMetaData: Failed when closing DB connection: "+e.getMessage());
			throw new SystemException("DB connection closing failed",e);
		}
	}
    
	/**
	 * Public method that implempents the super class interface, and is
	 * responsible for processing each sheets with different solution according
	 * to different sheet names.It is actually trying to process sheet by
	 * followint the database table dependency.
	 * 
	 * @param rs  specific <code>Sheet<code> object of current <code>WorkBook<code>        
	 * @exception exceptions will be recorded in database log tables               
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#ProcessSheet(jxl.Sheet)
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 */
	public void ProcessSheet(Sheet rs) {
		    
	        // print out sheet content, if debug mode is enabled
//		    if (debug) {
//			   printSheetContent(rs);
//		    }
		
		    // get sheet name
			String sheetName = rs.getName().toUpperCase().trim();
			
			// process sheet with different method according to sheet name
			
			// in case of data set
			if (sheetName.startsWith("DATA_SET")) {
				try {
					
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing started!");
					processDataSet(rs, conn);
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing completed,please check log for errors!");
					
				} catch (Exception e) {
					
                    dataSetFullName=null;
                    logger.warn("ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
					this.LogDetails(MsgCategory.WARN,"ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
				}
			}

			// in case of report template
			if (sheetName.startsWith("REPORT_TEMPLATE")) {
				try {
					
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing started!");
					processReportTemplate(rs, conn);
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing completed,please check log for errors!");
					
				} catch (Exception e) {
					
					reportTemplate=null;
					logger.warn("ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
					this.LogDetails(MsgCategory.WARN,"ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
				}
			}

			// in case of dimension item
			if (sheetName.startsWith("DIMENSION_ITEM")&& (!sheetName.equals("DIMENSION_ITEM_RSHIP"))) {
				try {
					
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing started!");
					processDimensionItem(rs, conn);
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing completed,please check log for errors!");
					
				} catch (Exception e) {
					
					logger.warn("ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
					this.LogDetails(MsgCategory.WARN,"ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
				}
			}

			// in case of report cell
			if (sheetName.startsWith("REPORT_CELL")) {
				
				try {
					
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing started!");
					processReportCell(rs, conn);
					logger.info("ReportMetaData: "+ fileName+": Sheet "+sheetName+" processing completed,please check log for errors!");

				} catch (Exception e) {
					
					logger.warn("ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
					this.LogDetails(MsgCategory.WARN,"ReportMetaData: "+ fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
				}
			}
	}
    
	/**
	 * Private method that is responsible for processing data set sheet.
	 * 
	 * @param rs  specific <code>Sheet<code> object of current <code>WorkBook<code>           
	 * @param conn specific database <code>Connection<code> object 
	 * @exception exceptions will be recorded in database log tables     
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#ProcessSheet(jxl.Sheet)
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 * @see java.sql.Connection
	 */
	private void processDataSet(Sheet rs, Connection conn)  {
		
		// get head row
		Cell[] headRow = rs.getRow(0);
		
        // get valid number of coloums
		int cn = getValidColumns(headRow); 
		int rn = rs.getRows(); // get number of rows including blank row
        
		// prepare callable statements to call store porcedures in DB
		CallableStatement insertDimensionItemCS = null;
		CallableStatement insertDataSetCS = null;
		
		try {
			
			insertDimensionItemCS = conn.prepareCall("{call csc_maintain_pkg.insert_dimension_item_proc('CONTENT CLASS',?)}");
			insertDataSetCS=conn.prepareCall("{call report_maintain_pkg.insert_data_set_proc(full_name_in =>? ,alias_in =>? ,content_class_in =>? ,report_producer_in =>? ,report_source_in =>? )}");
			
		} catch (SQLException e) {
			
			logger.warn(
					"ReportMetaData: "+ fileName+" Prepare callable statements failed when processing sheet "
							+ rs.getName() + ": " + e.getMessage());
		}
		
		// initialize processed row count
		int countsOfProcessedRow=0;
		
		// process each row 
		for (int r = 1; r < rn; r++) {

			// filter out blank rows
			if (!isBlankRow(rs.getRow(r),cn)) {
				
				// print out conent for debug
				//printSheetRow(rs,r);

				// feed data into DB
				
				// commit update for specified number of rows according to rowsOfCommit parameter to reduce APDS stream replication overhead
                if( countsOfProcessedRow==rowsOfCommit){

            		try {
            			
                        // batch executed in DB
						insertDimensionItemCS.executeBatch();
	            		insertDataSetCS.executeBatch();
	            		
	            		// commit chage to DB
	            		conn.commit();
	            		
	                    //clear batch
	            		insertDimensionItemCS.clearBatch();
	            		insertDataSetCS.clearBatch();
	            		
					} catch (SQLException e) {
						 
						int beginRows=r-rowsOfCommit+1;
						int endRows=r+1;
						logger.warn(
								"ReportMetaData: "+ fileName+" ReportMetaData: Batch executed,commit and clear batch for rows between "
										+ beginRows + " and " + endRows
										+ " failed when processing sheet "
										+ rs.getName() + ": " + e.getMessage());
					} 
            		
            		// reset counter
            		countsOfProcessedRow=0;
                }
				
				// get specified element values from current row
				String alias = formatCell(rs.getCell(getColumnIndex(headRow, "ALIAS"),r));
				String fullName= formatCell(rs.getCell(getColumnIndex(headRow, "FULL NAME"),r));
				String contentClass = formatCell(rs.getCell(getColumnIndex(headRow, "CONTENT CLASS"),r));
				String reportProducer = formatCell(rs.getCell(getColumnIndex(headRow,"REPORT PRODUCER"),r));
				String reportSource = formatCell(rs.getCell(getColumnIndex(headRow, "SOURCE"),r));

				// assign dataSet value to keep dataSet in meomery for usage later
				dataSetFullName = fullName;
				dataSetAlias=alias;
                
				// set callable statement values for specific placeholder
				
				try {
					
                    // for content class
					insertDimensionItemCS.setString(1, contentClass);
					insertDimensionItemCS.addBatch();
					
					// for data set	
					insertDataSetCS.setString(1,fullName);
					insertDataSetCS.setString(2, alias);
					insertDataSetCS.setString(3,contentClass);
					insertDataSetCS.setString(4, reportProducer);
					insertDataSetCS.setString(5, reportSource);
					insertDataSetCS.addBatch();
					
				} catch (SQLException e) {
					
					logger.warn(
							"ReportMetaData: "+ fileName+"  Batch executed,commit and clear batch for rows between "
									+ r + " failed when processing sheet "
									+ rs.getName() + ": " + e.getMessage());
	
				}
				
				 // increase number of processed rows
				 countsOfProcessedRow++;
			}
		}
		
		// deal with rest rows
		try {
			
            // batch executed in DB
			insertDimensionItemCS.executeBatch();
			insertDataSetCS.executeBatch();
			// commit chage to DB
			conn.commit();
			
		} catch (SQLException e) {
			
			int beginRow=rn-(rn+1)%rowsOfCommit+1;
			int endRow=rn+1;
			logger.warn(
					"ReportMetaData: "+ fileName+"  Batch executed,commit and clear batch for rows between "
							+ beginRow +" and "+endRow +" failed when processing sheet "
							+ rs.getName() + ": " + e.getMessage());
			
		} 
		
		
		// release statement source
		try {
			
			insertDimensionItemCS.close();
			insertDataSetCS.close();
			
		} catch (SQLException e) {
			
			logger.warn(
					"ReportMetaData: "+ fileName+"  Release callable Statements resources failed when processing sheet "
							+ rs.getName() + ": " + e.getMessage());
			
		} 
	
	}
	
	/**
	 * Private method that is responsible for processing report template sheet.
	 * 
	 * @param rs  specific <code>Sheet<code> object of current <code>WorkBook<code>                       
	 * @param conn  specific database <code>Connection<code> object 
	 * @exception exceptions will be recorded in database log tables              
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#ProcessSheet(jxl.Sheet)
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 * @see java.sql.Connection
	 */
	private void processReportTemplate(Sheet rs, Connection conn)
			 {
		
		// prepare callable statement to call store procedures in DB
		CallableStatement insertMeasurementCS =null;
		CallableStatement insertReportTemplateCS =null;
		
		try {
			
			insertMeasurementCS =conn.prepareCall("{call report_maintain_pkg.insert_measurement_proc(name_in => ?)}");			
			insertReportTemplateCS =conn.prepareCall("{call report_maintain_pkg.insert_report_template_proc(report_template_name_in => ?,report_template_code_in =>? ,release_frequency_in =>? ,measurement_in =>? ,data_set_in =>?)}");
		
		} catch (SQLException e) {
			
			logger.warn(
					"ReportMetaData: "+ fileName+"  Prepare callable statements failed when processing file:"+" sheet "
							+ rs.getName() + ": " + e.getMessage());
			
		}
        
		// to ensure that dataSet sheet has been processed
		if ((null != dataSetFullName) && (!dataSetFullName.isEmpty())) {
			
			// get head row
			Cell[] headRow = rs.getRow(0);

			int cn = getValidColumns(headRow); // get valid number of coloums
			int rn = rs.getRows(); // get number of rows including blank row

			//System.out.println(cn);
			//System.out.println(rn);
			
            // initialize processed row count
//			int countsOfProcessedRow=0;
			
			// process each row
			for (int r = 1; r < rn; r++) {

				// filter out blank rows
				if (!isBlankRow(rs.getRow(r),cn)) {

					// print out conent for debug
					//printSheetRow(rs, r);

					// feed data into DB
//					 if( countsOfProcessedRow==rowsOfCommit){
		                   
//		            		try {
//		            			
//		                        // batch executed in DB
//		            			insertMeasurementCS.executeBatch();
//		            			insertReportTemplateCS.executeBatch();
//			            		
//			            		// commit chage to DB
//			            		conn.commit();
//			            		
//			                    // clear batch
//			                    insertMeasurementCS.clearBatch();
//			                    insertReportTemplateCS.clearBatch();
//			            		
//							} catch (SQLException e) {
//								int beginRows=r-rowsOfCommit+1;
//								int endRows=r+1;
//								LoggerHelper.log(LogLevel.ERROR,
//										"Bath executed,commit and clear bath for rows between "
//												+ beginRows + " and " + endRows
//												+ " failed when processing sheet "
//												+ rs.getName() + ": " + e.getMessage());
//								
//							} 
//		            		
//		            		// reset counter
//		            		countsOfProcessedRow=0;
//		                }
//					 
					
					// get specified element values from current row
					String name = formatCell(rs.getCell(getColumnIndex(headRow, "NAME"),r));
					String releaseFrequency = formatCell(rs.getCell(getColumnIndex(headRow,"RELEASE FREQUENCY"),r));
					String code = formatCell(rs.getCell(getColumnIndex(headRow, "CODE"),r));
					String measurement = formatCell(rs.getCell(getColumnIndex(headRow,"MEASUREMENT UNIT"),r));

					// assign dataSet value to keep dataSet in meomery for usage later
					reportTemplate = name;
					
					// set values for placehoders of specified callable statements
					try {

						// for measurement
						if ((null != measurement) && (!measurement.isEmpty())) {

							insertMeasurementCS.setString(1, measurement);
							insertMeasurementCS.execute();

						}

						// for report template
						insertReportTemplateCS.setString(1, name);
						insertReportTemplateCS.setString(2, code);
						insertReportTemplateCS.setString(3, releaseFrequency);
						insertReportTemplateCS.setString(4, measurement);
						insertReportTemplateCS.setString(5, dataSetFullName);
						insertReportTemplateCS.execute();
						
					} catch (SQLException e) {
						
						logger.warn(
								"ReportMetaData: "+ fileName+" Set placeholder values of callable statements and Executed for row "
										+ r + " failed when processing sheet "
										+ rs.getName() + ": " + e.getMessage());
						
					}
					
                    // increase number of processed rows
//					countsOfProcessedRow++;
				}// enf of if blank row
			}// end of for loop
			
			// deal with the rest of rows
			try {
                
//				// batch executed relest rows in DB
//				insertMeasurementCS.executeBatch();
//				insertReportTemplateCS.executeBatch();
				
				// commit change to DB
				conn.commit();
				
			} catch (SQLException e) {
				
//				int beginRow=rn-(rn+1)%rowsOfCommit+1;
//				int endRow=rn+1;
				logger.warn(
						"ReportMetaData: "+ fileName+" Commit failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			}
			
			// release statement resource
			try {
				
				insertMeasurementCS.close();
				insertReportTemplateCS.close();
				
			} catch (SQLException e) {
				
				logger.warn(
						"ReportMetaData: "+ fileName+" Release callable Statements resources failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			}
			
		} else {
			
			logger.warn("ReportMetaData: "+ fileName+" Failed when processing sheet "
					+ rs.getName()
					+ ":Sheet DATA_SET has not been processed yet! ");
			
		}
	}

	/**
	 * Private method that is responsible for processing dimension item sheet.
	 * 
	 * @param rs  specific <code>Sheet<code> object of current <code>WorkBook<code>         
	 * @param conn specific database <code>Connection<code> object 
	 * @exception exceptions will be recorded in database log tables           
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#ProcessSheet(jxl.Sheet)
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 * @see java.sql.Connection
	 */
	private void processDimensionItem(Sheet rs, Connection conn) {

		// prepare callable statements to call store procedure in DB
		CallableStatement insertBasicDimensionCS=null;
		CallableStatement insertCollectiveDimensionCS = null;
		CallableStatement insertBasicDimensionItemCS = null;
		CallableStatement insertCollectiveDimensionItemCS = null;
		CallableStatement insertReportTemplateDimensionCS = null;
		
		try {
			
			insertBasicDimensionCS = conn.prepareCall("{call csc_maintain_pkg.insert_dimension_proc(official_name_in =>? ,management_level_in => 'APPLICATION')}");
			insertCollectiveDimensionCS = conn.prepareCall("{call csc_maintain_pkg.insert_dimension_proc(official_name_in =>? ,group_type_in =>'COLLECTIVE DIMENSION' ,base_dimension_name_in =>? ,management_level_in => 'SYSTEM')}");
			insertBasicDimensionItemCS = conn.prepareCall("{call csc_maintain_pkg.insert_dimension_item_proc(official_name_in =>?,value_in =>?,unique_code_in =>? )}");
			insertCollectiveDimensionItemCS =conn.prepareCall("{call csc_maintain_pkg.insert_dimension_item_proc(official_name_in =>?,value_in =>?,unique_code_in =>? )}");
			insertReportTemplateDimensionCS = conn.prepareCall("{call report_maintain_pkg.insert_template_dimension_proc(dimension_name_in => ?,report_template_name_in => ?,dataset_name_in => ?)}");
			
		} catch (SQLException e) {
			
			logger.warn(
					"ReportMetaData: "+ fileName+" Prepare callable statements failed when processing sheet "
							+ rs.getName() + ": " + e.getMessage());
			
		}
		
		// to ensure that dataSet sheet has been processed
		if ((null != dataSetAlias) && (!dataSetAlias.isEmpty())
				&& (null != reportTemplate) && (!reportTemplate.isEmpty())) {
			
			// get head row
			Cell[] headRow = rs.getRow(0);
		    // get valid number of coloums
			int cn = getValidColumns(headRow);
			// get number of rows including blank row
			int rn = rs.getRows(); 
			
			// initialize processed row count
			int countsOfProcessedRow=0;
			
			// process each row
			for (int r = 1; r < rn; r++) {

				// filter out blank rows
				if (!isBlankRow(rs.getRow(r),cn)) {

					// print out conent for debug
					//printSheetRow(rs,r);

					// feed data into DB
					if( countsOfProcessedRow==rowsOfCommit){
		                   
	            		try {
	            			
	                        // batch executed in DB
	            			insertBasicDimensionCS.executeBatch();
	            			insertCollectiveDimensionCS.executeBatch();
	            			insertBasicDimensionItemCS.executeBatch();
	            			insertCollectiveDimensionItemCS.executeBatch();
	            			insertReportTemplateDimensionCS.executeBatch();
		            		
		            		// commit chage to DB
		            		conn.commit();
		            		
		                    // clear batch
		            		insertBasicDimensionCS.clearBatch();
		            		insertCollectiveDimensionCS.clearBatch();
		            		insertBasicDimensionItemCS.clearBatch();
		            		insertCollectiveDimensionItemCS.clearBatch();
		            		insertReportTemplateDimensionCS.clearBatch();
		            		
						} catch (SQLException e) {
							
							int beginRows=r-rowsOfCommit+1;
							int endRows=r+1;
							logger.warn(
									"ReportMetaData: "+ fileName+" Batch executed,commit and clear batch for rows between "
											+ beginRows + " and " + endRows
											+ " failed when processing sheet "
											+ rs.getName() + ": " + e.getMessage());
					
							
						} 
	            		
	            		// reset counter
	            		countsOfProcessedRow=0;
	                }
					
					// get specified values
					String collectiveDimension = formatCell(rs.getCell(getColumnIndex(headRow, "DIMENSION"), r)).toUpperCase();
					
					  // find basicDimension seperated with data set alias
					String basicDimension=null;
					if (-1 != collectiveDimension.indexOf(dataSetAlias)) {
						
						basicDimension = collectiveDimension.substring(0,collectiveDimension.indexOf(dataSetAlias) - 1);
					}
					else {
						
                        basicDimension = collectiveDimension;
					}
					
					String dimensionItemValue = formatCell(rs.getCell(getColumnIndex(headRow, "NAME"), r));
					String uniqueCode = formatCell(rs.getCell(getColumnIndex(headRow, "UNIQUE CODE"), r));

					// set values for placeholders of specified callable statements
					try {
						// for basic dimension
						insertBasicDimensionCS.setString(1, basicDimension.trim());
						insertBasicDimensionCS.addBatch();

						// for collective dimension
						insertCollectiveDimensionCS.setString(1,
								collectiveDimension.trim());
						insertCollectiveDimensionCS
								.setString(2, basicDimension.trim());
						insertCollectiveDimensionCS.addBatch();

						// for basic dimension item
						insertBasicDimensionItemCS.setString(1, basicDimension.trim());
						insertBasicDimensionItemCS.setString(2,dimensionItemValue.trim());
						insertBasicDimensionItemCS.setString(3, uniqueCode.trim());
						insertBasicDimensionItemCS.addBatch();

						// for collective dimension item
						insertCollectiveDimensionItemCS.setString(1,collectiveDimension.trim());
						insertCollectiveDimensionItemCS.setString(2,dimensionItemValue.trim());
						insertCollectiveDimensionItemCS.setString(3, uniqueCode.trim());
						insertCollectiveDimensionItemCS.addBatch();
						
						// for report template dimension
						insertReportTemplateDimensionCS.setString(1,collectiveDimension.trim());
						insertReportTemplateDimensionCS.setString(2,reportTemplate);
						insertReportTemplateDimensionCS.setString(3,dataSetFullName);
						insertReportTemplateDimensionCS.addBatch();
					
					} catch (SQLException e) {
						
						logger.warn(
								"ReportMetaData: "+ fileName+" Set placeholder values of callable statements for row "
										+ r + " failed when processing sheet "
										+ rs.getName() + ": " + e.getMessage());
						
					}
					
					// increase number of processed rows
					countsOfProcessedRow++;
				}
			}
			
			// deal with  rest of rows
			try {

				// batch executed rest rows in DB
				insertBasicDimensionCS.executeBatch();
				insertCollectiveDimensionCS.executeBatch();
				insertBasicDimensionItemCS.executeBatch();
				insertCollectiveDimensionItemCS.executeBatch();
				insertReportTemplateDimensionCS.executeBatch();

				// commit change to DB
				conn.commit();

			} catch (SQLException e) {
				
				int beginRow = rn - (rn + 1) % rowsOfCommit + 1;
				int endRow = rn + 1;
				logger.warn(
						"ReportMetaData: "+ fileName+" Batch executed,commit and clear batch for rows between "
								+ beginRow + " and " + endRow
								+ " failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			}
			
			// release statement resource
			try {
				
				insertBasicDimensionCS.close();
				insertCollectiveDimensionCS.close();
				insertBasicDimensionItemCS.close();
				insertCollectiveDimensionItemCS.close();
				insertReportTemplateDimensionCS.close();
				
			} catch (SQLException e) {
				
				logger.warn(
						"ReportMetaData: "+ fileName+" Release callable Statements resources failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			}
			
		} else {
			
			logger.warn("ReportMetaData: "+ fileName+" Failed when processing sheet "
					+ rs.getName()
					+ ":Sheet DATA_SET or REPORT_TEMPLATE has not been processed yet! ");
							
		}
	}

	/**
	 * Private method that is responsible for processing report cell sheet.
	 * 
	 * @param rs specific <code>Sheet<code> object of current <code>WorkBook<code>        
	 * @param conn specific database <code>Connection<code> object 
	 * @exception exceptions will be recorded in database log tables               
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#ProcessSheet(jxl.Sheet)
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 * @see java.sql.Connection
	 */
	private void processReportCell(Sheet rs, Connection conn) {

		// prepare callable statements to call store procedures in DB
		CallableStatement insertReportCellCS=null;
		
		try {
			
			insertReportCellCS=conn.prepareCall("{call report_maintain_pkg.insert_report_cell_proc(report_template_in =>? ,data_set_in =>? ,report_cell_short_name_in =>? ,report_cell_unique_code_in =>? ,report_cell_measurement_in =>? ,dimension_item_list_in =>? )}");
		
		} catch (SQLException e) {
			
			logger.warn(
					"ReportMetaData: "+ fileName+" Prepare callable statements failed when processing sheet "
							+ rs.getName() + ": " + e.getMessage());
			//e.printStackTrace();
		}
		
		// to ensure that dataSet and report template sheets have been processed
		if ((null != dataSetFullName) && (null != reportTemplate)
				&& (!dataSetFullName.isEmpty()) && (!reportTemplate.isEmpty())) {
			
			// get head row
			Cell[] headRow = rs.getRow(0);
			// get valid number of coloums
			int cn = getValidColumns(headRow);
            // get number of rows including blank row
			int rn = rs.getRows(); 
			
			// initialize processed row count
			int countsOfProcessedRow=0;
			
            // process each row
			for (int r = 1; r < rn; r++) {

				// filter out blank rows
				if (!isBlankRow(rs.getRow(r),cn)) {
					
					if( countsOfProcessedRow==rowsOfCommit){
		                   
	            		try {
	                        // batch executed in DB
	            			insertReportCellCS.executeBatch();

		            		// commit chage to DB
		            		conn.commit();
		            		
		            		// clear batch
		            		insertReportCellCS.clearBatch();
		            		
						} catch (SQLException e) {
							
							int beginRows=r-rowsOfCommit+1;
							int endRows=r+1;
							logger.warn(
									"ReportMetaData: "+ fileName+" Batch executed,commit and clear batch for rows between "
											+ beginRows + " and " + endRows
											+ " failed when processing sheet "
											+ rs.getName() + ": " + e.getMessage());
							
						} 
	            		
	            		// reset counter
	            		countsOfProcessedRow=0;
	                }
					
					// get specified values from current row
					int maxIndex=cn-6; // max index of dimension item rang searching
					String collectiveDimension=null; 
					String shortName = formatCell(rs.getCell(getColumnIndex(headRow,"SHORT NAME"), r));
					String uniqueCode = formatCell(rs.getCell(getColumnIndex(headRow, "UNIQUE CODE"), r));
					String measurementUnit = formatCell(rs.getCell(getColumnIndex(headRow, "MEASUREMENT UNIT"), r));
                    
					// generate dimension item list
					String [] dimensionItem=new String[2]; // to hold dimension and dimension items
					Vector<STRUCT> dimensionItemVector=new Vector<STRUCT>();
					STRUCT dimensionItemSTRUCT=null;
					StructDescriptor dimensionItemType =null;
					ArrayDescriptor dimensionItemListType =null;
					
					try {
						dimensionItemType = new StructDescriptor("COMPACT_DIMENSION_ITEM_T", conn);
						dimensionItemListType = new ArrayDescriptor("COMPACT_DIMENSION_ITEM_LST_T", conn);

						// trave each dimension items for report cell in current
						// row
						for (int i = 0; i < maxIndex; i = i + 2) {

							collectiveDimension = formatCell(rs.getCell(i, r));

							if (!collectiveDimension.isEmpty()) {

                                // dimension
								dimensionItem[0] = collectiveDimension;
                                // dimension item value
								dimensionItem[1] = formatCell(rs.getCell(i + 1,r)); 
								// STRUCT based on current dimension item
								dimensionItemSTRUCT = new STRUCT(dimensionItemType, conn, dimensionItem);
								dimensionItemVector.add(dimensionItemSTRUCT);
								
							}
						}

						// ARRAY based on current dimension item STRUCT array
						ARRAY dimensionItemList = new ARRAY(dimensionItemListType, conn,dimensionItemVector.toArray());

						// set values for placeholders of specified callable
						// statements
						insertReportCellCS.setString(1, reportTemplate);
						insertReportCellCS.setString(2, dataSetFullName);
						insertReportCellCS.setString(3, shortName);
						insertReportCellCS.setString(4, uniqueCode);
						insertReportCellCS.setString(5, measurementUnit);
						insertReportCellCS.setArray(6, dimensionItemList);
						insertReportCellCS.addBatch();
						
					} catch (SQLException e) {
						
						logger.warn(
								"ReportMetaData: "+ fileName+" Set placeholder values of callable statements for row "
										+ r + " failed when processing sheet "
										+ rs.getName() + ": " + e.getMessage());
						
					}
					
					// increase number of processed rows
					countsOfProcessedRow++;
				}
			}
			
			// deal with the rest of rows
			try {
				// batch executed in DB
				insertReportCellCS.executeBatch();

				// commit change to DB
				conn.commit();

			} catch (SQLException e) {
				
				int beginRow = rn - (rn + 1) % rowsOfCommit + 1;
				int endRow = rn + 1;
				logger.warn(
						"ReportMetaData: "+ fileName+" Batch executed,commit and clear batch for rows between "
								+ beginRow + " and " + endRow
								+ " failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			}
			
			// release statement source
			try {
				
				insertReportCellCS.close();
				
			} catch (SQLException e) {
				 
				logger.warn(
						"ReportMetaData: "+ fileName+" Release callable Statements resources failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			}
			
		} else {
			
			logger.warn("ReportMetaData: "+ fileName+" Errors when porcessing REPORT_CELL sheet: Sheet DATA_SET or REPORT_TEMPLATE has not been processed yet!");
//			System.out.println("ReportMetaData: "+ fileName+" ERROR:Sheet DATA_SET or REPORT_TEMPLATE has not been processed yet!");
		}
	}
    
	/**
	 * Private method that is responsible for printing out content of specified
	 * <code>Sheet<code>.It ingores blank rows by default.It is only for debug purpose.
	 * 
	 * @param rs specific <code>Sheet<code> object of current <code>WorkBook<code>    
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 */
	private void printSheetContent(Sheet rs) {
        
		// valid number of columns
		int cn = getValidColumns(rs.getRow(0));
		// number of rows, including blank rows
		int rn=rs.getRows();
        System.out.println(rs.getName()+" with "+rn+" rows,"+cn+"columns");
		// print out content of data
		for (int r = 0; r < rn; r++) {

			if (0 == r) {
				for (int c = 0; c < cn; c++) {

					System.out.print(formatCell(rs.getCell(c, r)) + " ");
				}
				
				System.out.println();
			} else {
				
				for (int c = 0; c < cn; c++) {

					System.out.print("("+r+","+c+")"+formatCell(rs.getCell(c, r)));
				}
				
				System.out.println();
			}
		}
	}
	
	/**
	 * Pivate method that is responsible for returning valid number of columns
	 * for specific sheet.
	 * 
	 * @param headRow array of <code>Cell<code> object         
	 * @return number of valid columns 
	 * @see jxl.Cell;
	 */
	private int getValidColumns(Cell[] headRow) {
		
		// Assume that the colum number of trimed headrow is valid colum number
		int validColumns = 0;
		for (int i = 0; i < headRow.length; i++) {
			if (!formatCell(headRow[i]).isEmpty()) {
				validColumns++;
			}
		}
		return validColumns;
	}

	/**
	 * Private method that is responsible for returning index of specific
	 * column.
	 * 
	 * @param headRow array of <code>Cell<code> object  
	 * @param columnName name of specific column
	 * @return index of specific cloumn
	 * @see jxl.Cell
	 * @see
	 */
	private int getColumnIndex(Cell[] headRow, String columnName) {

		int index = -1;
		for (int i = 0; i < headRow.length; i++) {
			if (formatCell(headRow[i]).toUpperCase().equals(
					columnName.toUpperCase())) {
				index = i;
				break;
			}
		}

		return index;
	}

	/**
	 * Private method that is responsible for checking if the input row is blank
	 * or not.
	 * 
	 * @param row array of <code>Cell<code> object         
	 * @param columns number of valid columns
	 * @return <code>true<code>, if input row is blank,otherwise return 
	 *         <cod>flase<code>
	 */
	private boolean isBlankRow(Cell[] row,int columns) {

		boolean blank = true;
		// int
		for (int i = 0; i < row.length; i++) {
			if (!formatCell(row[i]).isEmpty()) {
				blank = false;
			}
		}
		return blank;
	}

	/**
	 * Private method that is responsible for converting content of specific
	 * <code>Cell<code> to a <code>String<code>.
	 * 
	 * @param cellContent <code>Cell<code> object
	 * @return string of cell content
	 * @see java.lang.String
	 * @see jxl.Cell
	 */
	
	private String formatCell(Cell cellContent) {
		
		String cellString=null;
		
		if (cellContent.getType().equals(CellType.DATE)) {
			
			DateCell dc = (DateCell) cellContent;
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			cellString = formatter.format(dc.getDate());
			
		} else {
			
			cellString = cellContent.getContents().trim();			
		}

		return cellString;
	}
}
