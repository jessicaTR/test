package com.thomsonreuters.ce.loadFixture;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import jxl.Cell;
import jxl.DateCell;
import jxl.Sheet;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.ZippedXLSProcessor;
import com.thomsonreuters.ce.dbor.file.ExcelProcessor;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;

public class ICAPFixtureLoader extends ExcelProcessor {
	private Logger logger;
	private String fileName;
	private String commodityType = null;
	Hashtable mineCodeHt = new Hashtable();

	public FileCategory getFileCatory(File FeedFile) {
		return FileCategory.getInstance("ICAPFixtureLoader");
	}

	public void Initialize(File FeedFile) {
		fileName = FeedFile.getName();
	}
 
	public void ProcessSheet(Sheet rs) {
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);
			conn.setAutoCommit(false);
			CallableStatement callableInitialize = conn
					.prepareCall("{call  cef_cnr.fixture_load_pkg.initialize_session_variables}");			
			callableInitialize.execute();
			callableInitialize.close();
			conn.commit();
			
			 
			String sheetName = rs.getName().toUpperCase().trim();
			if (fileName.indexOf("ICAP") >= 0) {

				System.out.println("Start to process file:" + fileName
						+ " sheet " + sheetName);
				processData(rs, conn);

			}
			CallableStatement callableUpdatedate = conn
					.prepareCall("{call  ce_refresh_maintain_pkg.update_last_upd_date_proc('FIXTURE')}");			
			callableUpdatedate.execute();
			callableUpdatedate.close();
			CallableStatement callableUpdateTs = conn
					.prepareCall("{call  fixture_load_pkg.update_refresh_timestamp_proc}");			
			callableUpdateTs.execute();
			callableUpdateTs.close();
			conn.commit();
			
		} catch (Exception e) {
			this.LogDetails(
					MsgCategory.WARN,
					"Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
			logger.error("Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
		} finally {
			try {
				CallableStatement callableReleasee = conn
						.prepareCall("{call  cef_cnr.fixture_load_pkg.release_session_variables}");			
				callableReleasee.execute();
				callableReleasee.close();
				conn.commit();
				conn.close();
			} catch (SQLException e) {
				logger.warn("Failed to close DB connection or statement: "
								+ e.getMessage());
				throw new SystemException(
						"Failed to close DB connection or statement.", e);
			}

		}
	}

	private void processData(Sheet rs, Connection conn) {

		Cell[] headRow = rs.getRow(1);
		int rn = rs.getRows(); // get number of rows including blank row
		int cn = getValidColumns(headRow); // get number of columns

		int dateReportedIndex = -1;
		int vesselIndex = -1;
		int cargoIndex = -1;
		int loadIndex = -1;
		int dischargeIndex = -1;
		int laycanIndex = -1;
		int currencyIndex = -1;
		int rateIndex = -1;
		int chartererIndex = -1;
		int cargoTypeIndex = -1;
		int cleanDirtyIndex = -1;

		// get column index
		for (int i = 0; i < headRow.length; i++) {
			if (headRow[i].getContents().toUpperCase().equals("DATE REPORTED")) {
				dateReportedIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("VESSEL")) {
				vesselIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.equals("CARGO SIZE (KT)")) {
				cargoIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("LOAD")) {
				loadIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.indexOf("DISCHARGE") >= 0) {
				dischargeIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("LAYCAN")) {
				laycanIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.equals("CURRENCY")) {
				currencyIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("RATE")) {
				rateIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.equals("CHARTERER")) {
				chartererIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.equals("CARGO TYPE")) {
				cargoTypeIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.equals("CLEAN/DIRTY")) {
				cleanDirtyIndex = i;
			}
		}

		for (int r = 2; r < rn; r++) {
			try {
				if (!isBlankRow(rs.getRow(r), cn)) {
//					Date report_date = null;
					DateCell cell = (DateCell) rs.getCell(dateReportedIndex, r);
					SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");  
					String strdate = dateFormat.format(cell.getDate());
//					System.out.println(str);
//					String temp_report_date = rs.getCell(dateReportedIndex, r)
//							.getContents();
//					String[] tmp = temp_report_date.split("/");
//					temp_report_date = tmp[0]+"/"+tmp[1]+"/"+"20"+tmp[2];
//					System.out.println(temp_report_date);
					// if (!temp_report_date.equals(""))
					// {
					// report_date=strToDate(temp_report_date);
					// }

					String vessel = rs.getCell(vesselIndex, r).getContents();

					String temp_cargoSize = rs.getCell(cargoIndex, r)
							.getContents();
					// Double cargoSize=null;
					// if (!temp_cargoSize.equals(""))
					// {
					// cargoSize=Double.valueOf(temp_cargoSize);
					// }

					String load = rs.getCell(loadIndex, r).getContents();

					String discharge = rs.getCell(dischargeIndex, r)
							.getContents();

					Date laycan = null;
					String temp_laycan = rs.getCell(laycanIndex, r)
							.getContents();
					// if (!temp_laycan.equals(""))
					// {
					// Calendar a=Calendar.getInstance();
					// int year = a.get(Calendar.YEAR);
					// String[] tmp = temp_laycan.split("/");
					// temp_laycan = tmp[1]+"/"+tmp[0]+"/"+year;
					// laycan=strToDate(temp_laycan);
					// }

					String currency = rs.getCell(currencyIndex, r)
							.getContents();

					String tmp_rate = rs.getCell(rateIndex, r).getContents();
					// Double rate=null;
					// if (!tmp_rate.equals(""))
					// {
					// rate=Double.valueOf(tmp_rate);
					// }

					String charterer = rs.getCell(chartererIndex, r)
							.getContents();

					String cargoType = rs.getCell(cargoTypeIndex, r)
							.getContents();

					String cleanDirty = rs.getCell(cleanDirtyIndex, r)
							.getContents();

					ArrayList<Object> parmVector = new ArrayList<Object>();
					STRUCT parmStruct = null;
					StructDescriptor structdesc = StructDescriptor
							.createDescriptor("CEF_CNR.CE_VAR_NV_TYPE", conn);
					ArrayDescriptor descriptor = ArrayDescriptor
							.createDescriptor("CEF_CNR.CE_VAR_NV_LST_T", conn);

					String[] parmsArray = { "DATE REPORTED", strdate };
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
 				
					 
					parmsArray[0] = "VESSEL";
					parmsArray[1] = vessel;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
					
					
					parmsArray[0] = "Cargo Size (kt)";
					parmsArray[1] = temp_cargoSize;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
					
					
					parmsArray[0] = "Load";
					parmsArray[1] = load;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
					
					

					parmsArray[0] = "Discharge";
					parmsArray[1] = discharge;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
					
					

					parmsArray[0] = "Laycan";
					parmsArray[1] = temp_laycan;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
 				

					parmsArray[0] = "Currency";
					parmsArray[1] = currency;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
					

					parmsArray[0] = "Rate";
					parmsArray[1] = tmp_rate;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
					
					parmsArray[0] = "Charterer";
					parmsArray[1] = charterer;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);
					
					parmsArray[0] = "Cargo Type";
					parmsArray[1] = cargoType;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);


					parmsArray[0] = "Clean/Dirty";
					parmsArray[1] = cleanDirty;
					parmStruct = new STRUCT(structdesc, conn, parmsArray);
					parmVector.add(parmStruct);


					CallableStatement callableInsertFixture = conn
							.prepareCall("{call  cef_cnr.fixture_load_pkg.insert_fixture_proc(?,?)}");
					Object obj_array[] = parmVector.toArray();
					ARRAY array = new ARRAY(descriptor, conn, obj_array);
					callableInsertFixture.setString(1, "ICAP");
					callableInsertFixture.setArray(2, array);
					callableInsertFixture.execute();
					callableInsertFixture.close();
					conn.commit();

					// callableInsertFixture.setString(1, "TANKER");
					// callableInsertFixture.setString(2, null);
					// callableInsertFixture.setString(3, null);
					// callableInsertFixture.setString(4, null);
					// callableInsertFixture.setString(5, null);
					// if (cargoSize!=null)
					// {
					// callableInsertFixture.setDouble(6,
					// cargoSize.doubleValue());
					// }
					// else
					// {
					// callableInsertFixture.setNull(6, Types.DOUBLE);
					// }
					// String privacy_type="PUBLIC";
					// callableInsertFixture.setString(7, privacy_type);
					// callableInsertFixture.setString(8, null);
					//
					// if (laycan!=null)
					// {
					// callableInsertFixture.setDate(9, laycan);
					// }
					// else
					// {
					// callableInsertFixture.setNull(9, Types.DATE);
					// }
					//
					// if (laycan!=null)
					// {
					// callableInsertFixture.setDate(10, laycan);
					// }
					// else
					// {
					// callableInsertFixture.setNull(10, Types.DATE);
					// }
					//
					// if (rate!=null)
					// {
					// callableInsertFixture.setDouble(11, rate.doubleValue());
					// }
					// else
					// {
					// callableInsertFixture.setNull(11, Types.DOUBLE);
					// }
					//
					// callableInsertFixture.setString(12, null);
					//
					// callableInsertFixture.setString(13, cleanDirty);
					//
					// callableInsertFixture.setString(14, null);
					// callableInsertFixture.setNull(15, Types.INTEGER);
					// callableInsertFixture.setNull(16, Types.INTEGER);
					// callableInsertFixture.setNull(17, Types.DOUBLE);
					//
					// callableInsertFixture.setString(18, vessel);
					//
					// callableInsertFixture.setString(19, null);
					//
					// callableInsertFixture.setString(20, charterer);
					//
					// callableInsertFixture.setString(21, null);
					//
					// callableInsertFixture.setString(22, null);
					//
					// callableInsertFixture.setString(23,
					// cargoType);//commodity
					//
					// callableInsertFixture.setNull(24, Types.BIGINT);
					//
					// callableInsertFixture.setString(25, load);// load area
					//
					// callableInsertFixture.setString(26, null);//location
					//
					// callableInsertFixture.setNull(27, Types.BIGINT);
					//
					// callableInsertFixture.setString(28, discharge);// disch
					// area
					//
					// callableInsertFixture.setString(29, null);//location
					//
					// callableInsertFixture.setNull(30, Types.BIGINT);
					// callableInsertFixture.setString(31, null);
					// callableInsertFixture.setNull(32, Types.BIGINT);
					// callableInsertFixture.setString(33, null);
					// callableInsertFixture.setNull(34, Types.BIGINT);
					// callableInsertFixture.setString(35, null);
					// callableInsertFixture.setNull(36, Types.BIGINT);
					// callableInsertFixture.setString(37, null);
					//
					// callableInsertFixture.setNull(38, Types.BIGINT);
					// callableInsertFixture.setString(39, null);
					// callableInsertFixture.setNull(40, Types.BIGINT);
					// callableInsertFixture.setString(41, null);
					// callableInsertFixture.setNull(42, Types.BIGINT);
					// callableInsertFixture.setString(43, null);
					//
					// callableInsertFixture.setNull(44, Types.BIGINT);
					// callableInsertFixture.setString(45, null);
					// callableInsertFixture.setNull(46, Types.BIGINT);
					// callableInsertFixture.setString(47, null);
					// callableInsertFixture.setNull(48, Types.BIGINT);
					// callableInsertFixture.setString(49, null);
					// callableInsertFixture.setNull(50, Types.BIGINT);
					// callableInsertFixture.setString(51, null);
					//
					// callableInsertFixture.setString(52, "U");
					//
					//
					// // if (report_date!=null)
					// // {
					// // callableInsertFixture.setDate(1, report_date);
					// // }
					// // else
					// // {
					// // callableInsertFixture.setNull(1, Types.DATE);
					// // }
					//
					// callableInsertFixture.execute();
					// callableInsertFixture.close();
					// conn.commit();

				} else {// if it's blank row, break the for loop

					break;
				}
			} catch (SQLException e) {
				this.LogDetails(MsgCategory.ERROR, "Executed failed for row: "
						+ r + " in file:" + fileName + " sheet " + rs.getName()
						+ ": " + e.getMessage());
				logger.error("Executed failed for row: " + r
						+ " in file:" + fileName + " sheet " + rs.getName()
						+ ": " + e.getMessage());
				// System.out.print("Executed failed for row: " + r +
				// " when processing file:"+fileName+" sheet "
				// + rs.getName() + ": " + e.getMessage());
			} catch (Exception e) {
				this.LogDetails(MsgCategory.ERROR, "Executed failed for row: "
						+ r + " in file:" + fileName + " sheet " + rs.getName()
						+ ": " + e.getMessage());
				logger.error("Executed failed for row: " + r
						+ " in file:" + fileName + " sheet " + rs.getName()
						+ ": " + e.getMessage());
			}
		}
	}

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

	private boolean isBlankRow(Cell[] row, int columns) {

		boolean blank = true;
		// int
		for (int i = 0; i < row.length; i++) {
			if (!row[i].getContents().isEmpty()) {
				blank = false;
			}
		}
		return blank;
	}

	private Date strToDate(String strDate) {
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		ParsePosition pos = new ParsePosition(0);
		Date d = new Date(formatter.parse(strDate, pos).getTime());
		return d;
	}

	public void Finalize() {

	}

}
