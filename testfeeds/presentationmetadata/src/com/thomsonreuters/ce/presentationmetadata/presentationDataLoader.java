package com.thomsonreuters.ce.presentationmetadata;

import java.io.File;
import java.sql.*;

import org.apache.log4j.Logger;

import jxl.Cell;
import jxl.Sheet;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.file.ExcelProcessor;
/**
 * This presentation meta data loader class is to load presentation meta data, 
 * which is for front-end team to get the mapping between front-end and database.
 * The presentation meta data file is .xls file, which has 2 sheets, 
 * one is PRESENTATION TEMPLATE, which will be inserted into presentation_template table,
 * another is TEMPLATE_DATA_ITEM, which will be inserted into template_data_item table.
 * @author j.li
 *
 */

public class presentationDataLoader  extends ExcelProcessor {
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
	
	private String fileName;
    private String reportID;   
    
	/**
	 * Private int variable that is holding number of processed rows for each
	 * issued commit.
	 */
	private int rowsOfCommit=2000;
	
	public FileCategory getFileCatory(File a)
	{
		return FileCategory.getInstance("PRESENTATION METADATA TOOL");
	}
		
	public void Initialize(File FeedFile)
	{
		try {
					
					conn = new EasyConnection(DBConnNames.CEF_CNR);
					conn.setAutoCommit(false);
					fileName = FeedFile.getName();
					
				} catch (Exception e) {
					
					logger.warn("Failed when initilizing DB connection: "+e.getMessage());
					
					throw new SystemException("DB connection initilizing failed",e);
				}		
	}
	
	public void ProcessSheet(Sheet rs)
	{
		String sheetName = rs.getName().toUpperCase().trim();
		
		if (sheetName.startsWith("PRESENTATION TEMPLATE")) {
			try {
				
				logger.info(fileName+": Sheet "+sheetName+" processing started!");
				processTemplate(rs);
				logger.info(fileName+": Sheet "+sheetName+" processing completed,please check log for errors!");
				
			} catch (Exception e) {
				
                dataSetFullName=null;
                logger.warn(fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
				this.LogDetails(MsgCategory.WARN,fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
			}
		} 
		else if(sheetName.startsWith("TEMPLATE_DATA_ITEM"))
		{
			try {
				
				logger.info(fileName+": Sheet "+sheetName+" processing started!");
				processDataItem(rs);
				logger.info(fileName+": Sheet "+sheetName+" processing completed,please check log for errors!");
				
			} catch (Exception e) {
				
                dataSetFullName=null;
                logger.warn(fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
				this.LogDetails(MsgCategory.WARN,fileName+": Failed when porcessing " +sheetName+ ": "+e.getMessage());
			}
		}
	}
	
	/**
	 * Private method that is responsible for processing presentation template sheet.
	 * 
	 * @param rs  specific <code>Sheet<code> object of current <code>WorkBook<code>           
	 * @exception exceptions will be recorded in database log tables     
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#ProcessSheet(jxl.Sheet)
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 * @see java.sql.Connection
	 */
	private void processTemplate(Sheet rs)  {
		
		// get head row
		Cell[] headRow = rs.getRow(0);
		int rn = rs.getRows(); // get number of rows including blank row
		int cn = getValidColumns(headRow); //get number of columns
		// prepare callable statements to call store porcedures in DB
		CallableStatement insertPresentTemplateCS = null;
		CallableStatement insertDataItemCS = null;
		
		try {
			
			insertPresentTemplateCS = conn.prepareCall("{? =call presentation_template_pkg.insert_template_fn(?,?)}");
			insertDataItemCS=conn.prepareCall("{call presentation_template_pkg.insert_data_item_proc(?,?,?,?,? )}");
			
		} catch (SQLException e) {
			
			logger.warn("Prepare callable statements failed when processing "+fileName+" sheet "
							+ rs.getName() + ": " + e.getMessage());
		}
		
		// get specified element values from current row
		
		for (int r = 1; r < rn; r++) 
		{
//			 filter out blank rows
			if (!isBlankRow(rs.getRow(r),cn)) 
			{
				reportID = rs.getCell(getColumnIndex(headRow, "REPORT ID"),r).getContents();
				dataSetFullName = rs.getCell(getColumnIndex(headRow, "DATA SET NAME"),r).getContents();
	
				try {		
	                    insertPresentTemplateCS.registerOutParameter(1, Types.VARCHAR);
						insertPresentTemplateCS.setString(2, reportID);
						insertPresentTemplateCS.setString(3, dataSetFullName);
						insertPresentTemplateCS.execute();					
					} catch (SQLException e) {				
						this.LogDetails(MsgCategory.WARN, "Executed failed for row: "	+ r + " when processing file:"+fileName+" sheet "
								+ rs.getName() + ": " + e.getMessage());
						logger.warn("Executed failed for row: "	+ r + " when processing file:"+fileName+" sheet "
										+ rs.getName() + ": " + e.getMessage());
						System.out.print("Executed failed for row: "	+ r + " when processing file:"+fileName+" sheet "
								+ rs.getName() + ": " + e.getMessage());
						
					}				
			}
		}
		try{
				conn.commit();					
			} catch (SQLException e) {				
				logger.warn("Commit failed for file "+fileName+" sheet "
									+ rs.getName() + ": " + e.getMessage());
					
			} 		
			
			// release statement source
		try {
				insertPresentTemplateCS.close();
				
			} catch (SQLException e) {
				
				logger.warn("Close callable Statements resources failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			} 
		
	}
	
	/**
	 * Private method that is responsible for processing presentation template sheet.
	 * 
	 * @param rs  specific <code>Sheet<code> object of current <code>WorkBook<code>           
	 * @exception exceptions will be recorded in database log tables     
	 * @see com.thomsonreuters.ce.feeds.actions.ExcelProcessor#ProcessSheet(jxl.Sheet)
	 * @see jxl.Sheet
	 * @see jxl.Workbook
	 * @see java.sql.Connection
	 */
	private void processDataItem(Sheet rs)  {
		
		// get head row
		Cell[] headRow = rs.getRow(0);
		int rn = rs.getRows(); // get number of rows including blank row
		int cn = getValidColumns(headRow); //get number of columns
		int uncommitRow=0;
		// prepare callable statements to call store porcedures in DB
		CallableStatement insertDataItemCS = null;
		
		try {
			insertDataItemCS=conn.prepareCall("{call presentation_template_pkg.insert_data_item_proc(?,?,?,?,? )}");
			
			} catch (SQLException e) {			
				logger.warn("Prepare callable statements failed when processing "+fileName+" sheet "
							+ rs.getName() + ": " + e.getMessage());
			}
		
		// get specified element values from current row

		for (int r = 1; r < rn; r++) 
		{
//			 filter out blank rows
			if (!isBlankRow(rs.getRow(r),cn)) 
			{
				String businessId = rs.getCell(getColumnIndex(headRow, "BUSINESS ID"),r).getContents();
				String reportName = rs.getCell(getColumnIndex(headRow, "REPORT NAME"),r).getContents();
				String cellShortName = rs.getCell(getColumnIndex(headRow, "CELL SHORT NAME"),r).getContents();
					
				try {		
						insertDataItemCS.setString(1, reportID);
						insertDataItemCS.setString(2, businessId);
						insertDataItemCS.setString(3, dataSetFullName);
						insertDataItemCS.setString(4, reportName);
						insertDataItemCS.setString(5, cellShortName);
						insertDataItemCS.execute();
					} catch (SQLException e) {		
						this.LogDetails(MsgCategory.WARN,"Executed failed for row: "	+ r + " when processing file:"+fileName+" sheet "
								+ rs.getName() + ": " + e.getMessage());	
						logger.warn("Executed failed for row: "	+ r + " when processing file:"+fileName+" sheet "
										+ rs.getName() + ": " + e.getMessage());		
						System.out.print("Executed failed for row: "	+ r + " when processing file:"+fileName+" sheet "
								+ rs.getName() + ": " + e.getMessage());		
				
					}	
					
				uncommitRow=uncommitRow+1;	
				if(uncommitRow==rowsOfCommit){
					try{
//							insertDataItemCS.executeBatch();
							conn.commit();		
//							insertDataItemCS.clearBatch();
					} catch (SQLException e) {
						int beginRows=r-rowsOfCommit+1;
						int endRows=r+1;
						this.LogDetails(MsgCategory.WARN,"Batch commit for rows between "
								+ beginRows + " and " + endRows
								+ " failed when processing file "+fileName+" sheet "
								+ rs.getName() + ": " + e.getMessage());	
						logger.warn("Batch commit for rows between "
										+ beginRows + " and " + endRows
										+ " failed when processing file "+fileName+" sheet "
										+ rs.getName() + ": " + e.getMessage());		
						System.out.print("Batch commit for rows between "
								+ beginRows + " and " + endRows
								+ " failed when processing file "+fileName+" sheet "
								+ rs.getName() + ": " + e.getMessage());		
					} 		
					
					uncommitRow = 0;
				} //enf of if uncommitRow==rowsOfCommit
			}//enf of !isBlankRow(rs.getRow(r),cn)
		}//enf of loop r
		
		//execute the rest rows
		try {
//				insertDataItemCS.executeBatch();
				conn.commit();
		} catch (SQLException e) {
			int beginRow = rn - (rn + 1) % rowsOfCommit + 1;
			int endRow = rn + 1;
			this.LogDetails(MsgCategory.WARN,"Batch commit for rows between "
					+ beginRow + " and " + endRow
					+ " failed when processing file "+fileName+" sheet "
					+ rs.getName() + ": " + e.getMessage());			
			logger.warn("Batch commit for rows between "
							+ beginRow + " and " + endRow
							+ " failed when processing file "+fileName+" sheet "
							+ rs.getName() + ": " + e.getMessage());			
			System.out.print("Batch commit for rows between "
					+ beginRow + " and " + endRow
					+ " failed when processing file "+fileName+" sheet "
					+ rs.getName() + ": " + e.getMessage());		
		}
			// release statement source
		try {
				insertDataItemCS.close();
				
			} catch (SQLException e) {
				
				logger.warn("Close callable Statements resources failed when processing sheet "
								+ rs.getName() + ": " + e.getMessage());
				
			} 
		finally
			{
				try {
						conn.rollback();
						conn.close();
				} catch (SQLException e) {
					throw new SystemException(e.getMessage(),e);
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
				if (!headRow[i].getContents().isEmpty()) {
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
			if (headRow[i].getContents().toUpperCase().equals(
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
			if (!row[i].getContents().isEmpty()) {
				blank = false;
			}
		}
		return blank;
	}
	
	public void Finalize()
	{
		
	}

}
