package com.thomsonreuters.ce.ChinaDataImportExport;

//import java.io.File;
//import java.io.FileInputStream;
import java.io.*;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;
import java.sql.*;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;

import oracle.jdbc.OracleTypes;

import com.thomsonreuters.ce.ChinaDataImportExport.Starter;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.ZippedCSVProcessor;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;

/**
 * The LanworthLoader class  process the WASDE files provided by Lanworth. 
 * This class will be called in FileScanner class.
 * This class first check the 6 header rows, then will process the data row in loop. 
 * Each row has some fields are dimension items, which will be passed to PL/SQL to get report cell id.
 * Other fields will be treated as values which will be inserted into value table.
 * @author jessica.li
 * 
 */
public class ChinaDataImportExportLoader extends ZippedCSVProcessor {
	
	private Logger logger  = Starter.thisLogger;
	public FileCategory getFileCatory(File a)
	{
		return FileCategory.getInstance("China Data Monthly Import & Export");
	}	
	
	public void Initialize(File FeedFile)
	{
		
	}
	
	public void ProcessCSVFile(String FileName, List<String[]> CSVArray)
	{
		// TODO Auto-generated method stub
		Connection thisConnection=null;		
		String reportId=null;
		Date  reportTime=null;				
		String reportSource=null;	
//		int rowsOfCommit=1;
//		String fileName=FileName;
		int rteID=-1;			
//		int processedRowsNumber=0;
		java.sql.Timestamp nowTime=currentDate();
 
		try {			
			thisConnection=new EasyConnection(DBConnNames.CEF_CNR);
			thisConnection.setAutoCommit(false); 			
			/**
			 * The Conn is declared for Array Descriptor, because ArrayDescriptor.createDescriptor must use native connecton
			 */
			Connection Conn=thisConnection; 			
	
			reportSource=FileName; 
			List<String[]> contentList=CSVArray; 
			
			/**
			 * Below is to get CE_DATA_SET id
			 */
			int rprID=-1;	
			String rprName="China Data Monthly Import & Export";			
			String monStr=null;
			String yearStr=null;
			String reportTitle="China Data Monthly Import & Export";
			CallableStatement cs;
			cs = thisConnection.prepareCall("{? = call report_distribution_pkg.get_data_set_fn(?)}"); 
			cs.registerOutParameter(1, Types.INTEGER);
			cs.setString(2, rprName);
			cs.execute();
			rprID = cs.getInt(1);
			if(rprID==-1)
			{
				throw new LogicException("No Record exists in Table CE_DATA_SET when full_Name="+rprName+".\n Please check table CE_DATA_SET!");
			}
			
//			get report title, and then can get rteID earlier, because insert application lod need rteID
			/**
			 * Below is to get report_template id
			 */
			try{
					if((contentList.get(0))[1]!=null &&(contentList.get(0))[1].length()>0)
						monStr=(contentList.get(0))[1].trim();
					else
					{
						throw new LogicException("No Month row in this file.\n Please check file first row!");
					}
			}catch(Exception e){
				throw new LogicException("Month row is wrong in this file.\n Please check file first row!");
			}
			try{
					if((contentList.get(1))[1]!=null &&(contentList.get(1))[1].length()>0)
						yearStr=(contentList.get(1))[1].trim();
					else
					{
						throw new LogicException("No Year row in this file.\n Please check file second row!");
					}
			}catch(Exception e){
				throw new LogicException("Year row is wrong in this file.\n Please check file second row!");
			}
//			get report release date from filename		    
//			 try{	
//					String tmp[]=fileName.substring(0,10).split("-");
//					reportTime=strToDate(tmp[2]+"/"+tmp[1]+"/"+tmp[0]);					
//				  }catch(ParseException e){
//					  throw new LogicException("File Name date part:"+fileName.substring(0,10)+" can not be converted to date 'yyyy-MM-dd'.\n Please check file name!");
//				  }
			if(monStr.trim().length()==1)
				monStr="0"+monStr;	
			try{	  
		    	  reportTime=strToDate(yearStr+"-"+monStr+"-"+"01");
			}catch(Exception e){
				   throw new LogicException("Month: "+ monStr+ ", year: "+yearStr+" are not valid format.\n Please check file!");
			   }
				// get report_template id			
			cs = thisConnection.prepareCall("{? = call report_distribution_pkg.get_report_template_fn(?,?)}"); 
			cs.registerOutParameter(1, Types.INTEGER);
			cs.setString(2, reportTitle);//because some reports 35-37 includes current Month
			cs.setInt(3, rprID);
			cs.execute();
			rteID = cs.getInt(1);
			cs.close();
			if(rteID==-1)
			{
				throw new LogicException("No Record exists in Table Report_Template when Report="+reportTitle+" and DATA_SET="+rprName+".\n Please check Report Title!");
			}
			
			reportId=yearStr+"-"+monStr;
			
			long    rlsID=-1;

			CallableStatement insRlsCs = thisConnection.prepareCall("{? = call report_maintain_pkg.insert_report_release_fn(?,?,?,?,?,?)}"); 
			insRlsCs.registerOutParameter(1, Types.BIGINT);
			insRlsCs.setInt(2, rteID);
			insRlsCs.setDate(3,reportTime);
			insRlsCs.setString(4,reportSource);
			insRlsCs.setString(5, "");
			insRlsCs.setString(6, "");
			insRlsCs.setString(7, reportId);
			insRlsCs.execute();
			rlsID = insRlsCs.getLong(1);
			insRlsCs.close();
			if (rlsID==-1) { 
						throw new LogicException("Insert into report_release failed or get the rls_id failed!");
					}
						
			// Get column headers
			int colNum=(contentList.get(4)).length;
			String[] colHeader= new String[colNum];
			String[] colData= new String[colNum];
			
			for (int i=0; i< colNum; i++)
			{
				if((contentList.get(4))[i]!=null && (contentList.get(4))[i].trim().length()>0)
					colHeader[i]=(contentList.get(4))[i].trim();	
				else
					throw new LogicException("Column Header is empty, column number is "+i+". Please check file column header!");				
			}
			String valueStr=null;
			String unitStr=null;
			String acculevelStr=null;
			String minorPeriodStart=null;
			String minorPeriodEnd=null;
			String minorPeriodUnit=null;
			/**
			 * This part is to process comments content which is at bottom of file.
			 */
			//			 Below variables are to pass vector to plsql function to get report_cell_id			
			String[] dimAttr = new String[2]; 
			STRUCT dimStruct =null; 
			StructDescriptor structdesc = StructDescriptor.createDescriptor("CEF_CNR.COMPACT_DIMENSION_ITEM_T",Conn);
			ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor("CEF_CNR.COMPACT_DIMENSION_ITEM_LST_T",Conn);

			CallableStatement insertCellCs = Conn.prepareCall("{call feed_app_lanworth.insert_cell_value_proc(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			int rowNumber=contentList.size();			
			outerloop:for(int j=5;j<rowNumber;j++)
			{  
				valueStr=null;
			    unitStr=null;
			    Vector<Object> vector = new Vector<Object>(); 
			    Boolean isCountry=true;
			    //check if it's empty row, if it's empty row, break
			    int emptyCount=0;
				for(int n=0;n<(contentList.get(j)).length;n++)
					if((contentList.get(j))[n].equals("")||(contentList.get(j))[n].length()<=0) emptyCount++;			
				if(emptyCount==(contentList.get(j)).length) break;
			   
//			    Because some line's (contentList.get(j)).length is less than colCounts, so use (contentList.get(j)).length as loop limit
				for(int k=0;k<(contentList.get(j)).length;k++)	
				{	
				  	colData[k]=(contentList.get(j))[k].trim();
					if(colHeader[k].equals("Value"))  // Begin Year column
						if(colData[k]!=null && colData[k].trim().length()>0)
							valueStr = colData[k];
						else
							valueStr = "-"; // if value is empty, then insert "-"
					
					else if(colHeader[k].equals("ValueType"))
					{						    
					    if(colData[k].equals("YTD") || colData[k].equals("TYD"))
					    {
					    	minorPeriodStart="01-01";
					        minorPeriodEnd=(LastDayOfMonth(Integer.parseInt(yearStr),Integer.parseInt(monStr))).substring(5);			
					        minorPeriodUnit="YEAR TO DATE";
					    }
						else if(colData[k].toUpperCase().equals("MONTHLY"))
						{
							minorPeriodStart=monStr+"-01";;
					        minorPeriodEnd=(LastDayOfMonth(Integer.parseInt(yearStr),Integer.parseInt(monStr))).substring(5);			
					        minorPeriodUnit="MONTH";
						}
					}		
					else
					{	
						if(colHeader[k].equals("Trade Type"))
							
						   {
							   dimAttr[0] = "STATISTIC";	
							   dimAttr[1] = colData[k].toUpperCase(); 
						   }
						else if(colHeader[k].equals("Commodity"))  // commodity code column
						{
							dimAttr[0] = "COMMODITY";
							
							if (colData[k].length()>120) 
							   continue outerloop; 
							else
								dimAttr[1] = colData[k].toUpperCase();
						
						}							
						else if(colHeader[k].equals("Country") || colHeader[k].equals("Port"))
						{
							if(colHeader[k].equals("Country") && colData[k]!=null && colData[k].length()>0)
							{
								dimAttr[0] = "GEOGRAPHY";
								if(colData[k].toUpperCase().equals("WORLD")) 
									dimAttr[1] = "COUNTRY ALL";
								else
								    dimAttr[1] = colData[k].toUpperCase();
								isCountry=true;
							}		
							else if(colHeader[k].equals("Country") && (colData[k]==null || colData[k].length()==0))
							{	 
								 isCountry=false; 
								 continue;
							}
							else if(colHeader[k].equals("Port") && !isCountry && colData[k]!=null && colData[k].length()>0 )
							{
								dimAttr[0] = "PHYSICAL_ASSET";
								if(colData[k].toUpperCase().equals("WORLD"))
									dimAttr[1] = "PORT ALL";
								else
								    dimAttr[1] = colData[k].toUpperCase();
							}
							else
							{	
								continue;
							}
						}
						else if(colHeader[k].equals("Units"))
						{   
							unitStr=colData[k].trim().toUpperCase();
							dimAttr[0] = "STATISTIC";	
//							if(colData[k].toUpperCase().equals("TONNES"))
							if(colData[k].indexOf("$")>=0)
									dimAttr[1] ="WORTH";
							else
									dimAttr[1] ="WEIGHT";
						}
						else if(colData[k]!=null && colData[k].length()>0)
							{	
							    dimAttr[0] = colHeader[k].toUpperCase();	
							    dimAttr[1] = colData[k].toUpperCase();
							}									
						
						dimStruct=new STRUCT(structdesc, Conn, dimAttr);									
						vector.add(dimStruct);
					} //end of else (not 'Value')			
				}// end of k loop
				//get report_cell id
				acculevelStr="ACTUAL";
				Object obj_array[] = vector.toArray();
				ARRAY array = new ARRAY(descriptor,Conn,obj_array);
				Object null_obj_array[]=null;
				ARRAY nullarray = new ARRAY(descriptor,Conn,null_obj_array);

			  	insertCellCs.setArray(1, array);
			  	insertCellCs.setInt(2, rteID);
			  	insertCellCs.setLong(3,rlsID);
			  	valueStr=valueStr.replace(",", "");
			  	double valueDouble=0;
			  	DecimalFormat df = new DecimalFormat("0.00");

			  	if(!valueStr.equals("-"))
			  		try{
			  			if(unitStr.equals("10,000 TONNES")) //convert 10,000 TONNES to TONNES 
			  			{
			  				valueDouble = Double.parseDouble(valueStr)*10000;
			  				unitStr = "TONNES";
			  			}
			  			else if(unitStr.equals("KG")) //convert KG to GRAMS 
			  			{
			  				valueDouble = Double.parseDouble(valueStr)*1000;
			  				unitStr = "GRAMS";
			  			}
			  			else
			  				valueDouble = Double.parseDouble(valueStr);
			  			valueStr = String.valueOf(df.format(valueDouble));
			  		}catch(Exception e)
			  		{
			  			this.LogDetails(MsgCategory.WARN, "ChinaDataImportExport: ["+valueStr + "] is not a valid number for value column of row "+ j +" of file "+ FileName+".");
			  			logger.warn("ChinaDataImportExport: ["+valueStr + "] is not a valid number for value column of row "+ j +" of file "+ FileName+".");
						System.out.print("ChinaDataImportExport: ["+valueStr + "] is not a valid number for value column of row "+ j +" of file "+ FileName+".");
						continue;
			  		}
			  	
			  	Date reportDateStart=null;	
			  	Date reportDateEnd = null;
			    try{	
				  	 reportDateStart = strToDate(yearStr+"-"+minorPeriodStart);	
				  	 reportDateEnd = strToDate(yearStr+"-"+minorPeriodEnd);	
			    }catch(Exception e){
			    	this.LogDetails(MsgCategory.WARN,
							"ChinaDataImportExport: start date/end date is Not in yyyy-mm-dd format: " +yearStr+"-"+minorPeriodStart+"/"+yearStr+"-"+minorPeriodEnd+ FileName
									+ ".");
			    	logger.warn("ChinaDataImportExport: start date/end date is Not in yyyy-mm-dd format: " +yearStr+"-"+minorPeriodStart+"/"+yearStr+"-"+minorPeriodEnd+ FileName
							+ ".");
					System.out
							.print("ChinaDataImportExport: start date/end date is Not in yyyy-mm-dd format: " +yearStr+"-"+minorPeriodStart+"/"+yearStr+"-"+minorPeriodEnd+ FileName
									+ ".");
			    	
			    }
			  	
			  	insertCellCs.setString(4,valueStr);
			  	insertCellCs.setString(5,acculevelStr);
			  	insertCellCs.setInt(6, Integer.parseInt(yearStr));
			  	insertCellCs.setInt(7, Integer.parseInt(yearStr));
			  	insertCellCs.setString(8, minorPeriodStart);
			  	insertCellCs.setString(9, minorPeriodEnd);
			  	insertCellCs.setString(10, minorPeriodUnit);
			  	insertCellCs.setDate(11, reportDateStart);	
			  	insertCellCs.setDate(12, reportDateEnd);	
			  	
			  	insertCellCs.setDate(13, reportTime);	  // set reportTime as ReportDate
			  	insertCellCs.setString(14, reportId);	  			
			   	insertCellCs.setTimestamp(15, nowTime);
			  	insertCellCs.setArray(16, nullarray);
                insertCellCs.setString(17, unitStr);
		  		insertCellCs.setString(18, rprName);
//		  		insertCellCs.setDate(16, reportTime);
//			  	insertCellCs.addBatch();	
//		  		insertCellCs.execute();
//			  	processedRowsNumber++;
//			  	if(processedRowsNumber==rowsOfCommit)
//			  	{
			  		try{
//			  			insertCellCs.executeBatch();
			  			insertCellCs.execute();
				  		thisConnection.commit();
//						insertCellCs.clearBatch();
//						processedRowsNumber=0;
			  		} catch (SQLException e) {
//						int beginRow=j-rowsOfCommit+1;
//						int endRow=j+1;
//			  			ExpProcess.CheckSqlExp(e,reportSource);
			  			this.LogDetails(MsgCategory.WARN, "ChinaDataImportExport: Execute failed for row "+ j + " of file "+ FileName + ": " + e.getMessage());
			  			logger.warn(
//								"Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
								"ChinaDataImportExport: Execute failed for row "+ j + " of file "+ FileName + ": " , e);
//						System.out.print("Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
						System.out.print("ChinaDataImportExport: Execute failed for row "+ j + " of file "+ FileName + ": " + e.getMessage());
					} 		  		
			  		
//			  	}			  	
	
			}// end of j loop
			
			
//			process the rest rows of file
//			try{
//	  			insertCellCs.executeBatch();
////		  		insertCellCs.execute();
//		  		thisConnection.commit();
//	  		} catch (SQLException e) {
//	  			int beginRow = rowNumber - (rowNumber + 1) % rowsOfCommit + 1;
//				int endRow = rowNumber + 1;
//				LoggerHelper.log(LogLevel.WARN,
//						"Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
//				System.out.print("Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
//			}					
			

			insertCellCs.close();	
			
//			Below calculate aggregation 

			CallableStatement aggCs;
			aggCs = thisConnection.prepareCall("{call REPORT_CALCULATION_PKG.periodical_aggregation_proc(?,?,?,?,?,?,?,?)}"); 
			aggCs.setString(1, rprName);
			aggCs.setString(2, reportTitle);
			aggCs.setDate(3,reportTime);
			
			// calculate yearly aggregation when month=12
			if(monStr.equals("12")){
				try{ 
					int Year=Integer.parseInt(yearStr);
					Date PeriodStart=strToDate(String.valueOf(Year)+"-01-01");				
					Date PeriodEnd=	strToDate(String.valueOf(Year)+"-12-31");
					aggCs.setDate(4,PeriodStart);
					aggCs.setDate(5,PeriodEnd);
					aggCs.setString(6, "YEAR");
					aggCs.setString(7, "MONTH");
					aggCs.setString(8, acculevelStr);
					aggCs.execute();
					thisConnection.commit();				
				}catch(ParseException e){
					throw new LogicException("Month: "+ monStr+ ", year: "+yearStr+" are not valid format. Can't execute aggregation. \n Please check file!");
				}
//				catch(SQLException e){
//		  			ExpProcess.CheckSqlExp(e,reportSource);
//		  			this.LogDetails(MsgCategory.WARN, "Execute YEAR aggregation failed for file "+ FileName + ": " + e.getMessage());
//					LoggerHelper.log(LogLevel.WARN,
////							"Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
//							"Execute YEAR aggregation failed for file "+ FileName + ": " + e.getMessage());
////					System.out.print("Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
//					System.out.print("Execute YEAR aggregation failed for file "+ FileName + ": " + e.getMessage());
//				}
			}// end of if
			//calculate quarterly aggregation
			try{
				Date PeriodStart=strToDate(yearStr+"-01-01");
				Date PeriodEnd=strToDate(yearStr+"-"+monStr+"-"+(LastDayOfMonth(Integer.parseInt(yearStr),Integer.parseInt(monStr))).substring(8));	
				aggCs.setDate(4,PeriodStart);
				aggCs.setDate(5,PeriodEnd);
				aggCs.setString(6, "QUARTER");
				aggCs.setString(7, "MONTH");
				aggCs.setString(8, acculevelStr);
				aggCs.execute();
				thisConnection.commit();
				aggCs.close();
			}catch(ParseException e){
				throw new LogicException("Month: "+ monStr+ ", year: "+yearStr+" are not valid format. Can't execute aggregation. \n Please check file!");
			}
//			catch(SQLException e){
//	  			ExpProcess.CheckSqlExp(e,reportSource);
//	  			this.LogDetails(MsgCategory.WARN, "Execute QUARTER aggregation failed for file "+ FileName + ": " + e.getMessage());
//				LoggerHelper.log(LogLevel.WARN,
////						"Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
//						"Execute QUARTER aggregation failed for file "+ FileName + ": " + e.getMessage());
////				System.out.print("Execute batch failed for rows between "+ beginRow + " and " + endRow	+ " of file "+ FileName + ": " + e.getMessage());
//				System.out.print("Execute QUARTER aggregation failed for file "+ FileName + ": " + e.getMessage());
//			}
			
		}
		
		catch (SQLException e)
		{    
			throw new SystemException(e.getMessage(),e);
		
		}finally
		{
			try {
			
				thisConnection.close();
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(),e);
			}									

		}
			
	}
	
	public void Finalize()
	{
		
	}
	

 
	public java.sql.Date strToDate(String strDate) throws ParseException 
	{ 
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd",Locale.ENGLISH);
		java.util.Date date = formatter.parse(strDate);
		java.sql.Date sqlDate=new java.sql.Date(date.getTime());			   

	     return sqlDate;		
	} 	
	public java.sql.Timestamp currentDate() 
	{ 
		java.util.Date longdate = new java.util.Date();
		java.sql.Date date = new java.sql.Date(longdate.getTime());
        java.sql.Time time = new java.sql.Time(longdate.getTime());
        java.sql.Timestamp curdate=java.sql.Timestamp.valueOf(date.toString()+" "+time.toString());

	     return curdate;		
	} 	
	public String LastDayOfMonth (int year, int month)
	{
		String str = "";    
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");        
    
        Calendar lastDate = Calendar.getInstance();    
  
        lastDate.set(year, month, 1);//set as the first day of that month
//        lastDate.add(Calendar.MONTH,1);//add 1 month, then become the first day of next month    
        lastDate.add(Calendar.DATE,-1);//minus 1 day, then become the last day of the month    
        str=sdf.format(lastDate.getTime());    
        return str;      
	}

}
