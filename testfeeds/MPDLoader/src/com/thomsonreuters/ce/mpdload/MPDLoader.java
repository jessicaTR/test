package com.thomsonreuters.ce.mpdload;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import jxl.Cell;
import jxl.Sheet;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.ExcelProcessor;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class MPDLoader extends ExcelProcessor {
	private Logger logger = Starter.thisLogger;
	private String fileName;
	private String commodityType = null;
	Hashtable mineCodeHt = new Hashtable();

	public FileCategory getFileCatory(File FeedFile) {
		return FileCategory.getInstance("MPDLoader");
	}

	public void Initialize(File FeedFile) {
		fileName = FeedFile.getName();
	}

	public void ProcessSheet(Sheet rs) {
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);
			conn.setAutoCommit(false);

			String sheetName = rs.getName().toUpperCase().trim();
			if (fileName.indexOf("BaseMetal") < 0) {// gold file
				commodityType = fileName
						.substring(5, fileName.lastIndexOf("_"));
				if (sheetName.startsWith("MINE CODE MAP")) {
					System.out.println("Start to process sheet:" + sheetName);
					processMineCodeMap(rs, conn);
				} else if (sheetName.startsWith("DATA QUERY")) {
					System.out.println("Start to process sheet:" + sheetName);
					processDataQuery(rs, conn);
				}
			} else {// BaseMetal file
					// if( sheetName.startsWith("DATA EXPORT")) {
				System.out.println("Start to process sheet:" + sheetName);
				processDataQuery(rs, conn);
				// }
			}
		} catch (Exception e) {
			this.LogDetails(
					MsgCategory.WARN,
					"Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
			logger.error("Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.warn("Failed to close DB connection or statement: "
								+ e.getMessage());
				throw new SystemException(
						"Failed to close DB connection or statement.", e);
			}

		}
	}

	private void processMineCodeMap(Sheet rs, Connection conn) {
		Cell[] headRow = rs.getRow(1);
		int rn = rs.getRows(); // get number of rows including blank row
		int cn = getValidColumns(headRow); // get number of columns
		int mineCodeIndex = -1;
		int ricIndex = -1;

		// get column index
		for (int i = 0; i < headRow.length; i++) {
			if (headRow[i].getContents().toUpperCase().equals("MINEID")) {
				mineCodeIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("RIC")) {
				ricIndex = i;
			}
		}

		try {
			CallableStatement callableInsertMineMapSM = conn
					.prepareCall("{call  mpd2_cnr.gfms_load_pkg.update_mine_code_map_proc(?,?,?)}");
			// put minecode and ric into hashtable
			for (int r = 2; r < rn; r++) {
				try {
					if (!isBlankRow(rs.getRow(r), cn)) {
						String mineCode = rs.getCell(mineCodeIndex, r)
								.getContents();
						String Ric = rs.getCell(ricIndex, r).getContents();

						mineCodeHt.put(mineCode, Ric);
						callableInsertMineMapSM.setString(1, Ric);
						callableInsertMineMapSM.setInt(2,
								Integer.parseInt(mineCode));
						callableInsertMineMapSM.setString(3, commodityType);
						callableInsertMineMapSM.execute();
						conn.commit();

					}
				} catch (SQLException e) {
					if (e.getMessage().indexOf("-20999") > 0) {
						throw new LogicException("File " + fileName
								+ " process will be stopped because of error:"
								+ e.getMessage());
					} else {
						this.LogDetails(MsgCategory.WARN,
								"Executed failed for row: " + r + " in file:"
										+ fileName + " sheet " + rs.getName()
										+ ": " + e.getMessage());
						logger.warn("Executed failed for row: " + r + " in file:"
										+ fileName + " sheet " + rs.getName()
										+ ": " + e.getMessage());
						// System.out.print("Executed failed for row: " + r +
						// " when processing file:"+fileName+" sheet "
						// + rs.getName() + ": " + e.getMessage());
					}
				}
			}
			callableInsertMineMapSM.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			this.LogDetails(
					MsgCategory.WARN,
					"Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
			logger.error("Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
		}
	}

	private void processDataQuery(Sheet rs, Connection conn) {
		Cell[] headRow = rs.getRow(0);
		int rn = rs.getRows(); // get number of rows including blank row
		int cn = getValidColumns(headRow); // get number of columns
		int mineIdIndex = -1;
		int yearIndex = -1;
		int quarterIndex = -1;
		int valueIndex = -1;
		int meaIndex = -1;
		int ricIndex = -1;

		String yearStr = "";
		String quarter = "";
		String valueStr = "";
		String mea = "";
		// int lastyear=0;
		int year = 0;
		float value;
		float yearSum = 0;
		// get column index
		for (int i = 0; i < headRow.length; i++) {
			if (headRow[i].getContents().toUpperCase().equals("MINEID")) {
				mineIdIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("YEAR")) {
				yearIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("QUARTER")) {
				quarterIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.indexOf("RECOVEREDTOTAL") >= 0) {
				valueIndex = i;
			} else if (headRow[i].getContents().toUpperCase()
					.equals("MEASUREMENT")) {
				meaIndex = i;
			} else if (headRow[i].getContents().toUpperCase().equals("RIC")) {
				ricIndex = i;
			}
		}

		try {
			CallableStatement callableInsertMPDSM = conn
					.prepareCall("{call  mpd2_cnr.gfms_load_pkg.update_pcd_proc(?,?,?,?,?)}");
			String mineRic = null;
			for (int r = 1; r < rn; r++) {
				try {
					if (!isBlankRow(rs.getRow(r), cn)) {
						if (ricIndex != -1) {// ricIndex!= -1 means ric in
												// BaseMetal file
							mineRic = rs.getCell(ricIndex, r).getContents();
						} else {
							String mineId = rs.getCell(mineIdIndex, r)
									.getContents();
							if (mineCodeHt.get(mineId) != null) {
								mineRic = mineCodeHt.get(mineId).toString();
							}
						}
						if (mineRic != null && mineRic != "") {

							yearStr = rs.getCell(yearIndex, r).getContents();
							year = Integer.parseInt(yearStr);
							if (quarterIndex == -1)
								quarter = "0";// quarter=0 means yearly data in
												// BaseMetal file
							else
								quarter = rs.getCell(quarterIndex, r)
										.getContents();
							valueStr = rs.getCell(valueIndex, r).getContents();
							value = Float.parseFloat(valueStr);
							mea = rs.getCell(meaIndex, r).getContents();
							yearSum = yearSum + value;
							if (quarter.equals("4") || quarter.equals("0")) {
								callableInsertMPDSM.setString(1, mineRic);
								callableInsertMPDSM.setString(2, commodityType);
								callableInsertMPDSM.setInt(3, year);
								callableInsertMPDSM.setFloat(4, yearSum);
								callableInsertMPDSM.setString(5, mea);
								callableInsertMPDSM.execute();
								conn.commit();
								yearSum = 0;
							}

						}
					} else {// if it's blank row, break the for loop

						break;
					}
				} catch (SQLException e) {
					this.LogDetails(MsgCategory.WARN,
							"Executed failed for row: " + r + " in file:"
									+ fileName + " sheet " + rs.getName()
									+ ": " + e.getMessage());
					logger.warn("Executed failed for row: " + r + " in file:"
									+ fileName + " sheet " + rs.getName()
									+ ": " + e.getMessage());
					// System.out.print("Executed failed for row: " + r +
					// " when processing file:"+fileName+" sheet "
					// + rs.getName() + ": " + e.getMessage());
				} catch (Exception e) {
					this.LogDetails(MsgCategory.WARN,
							"Executed failed for row: " + r + " in file:"
									+ fileName + " sheet " + rs.getName()
									+ ": " + e.getMessage());
					logger.error("Executed failed for row: " + r + " in file:"
									+ fileName + " sheet " + rs.getName()
									+ ": " + e.getMessage());
				}
			}
			callableInsertMPDSM.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			this.LogDetails(
					MsgCategory.WARN,
					"Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
			logger.error("Failed to process file:" + fileName + " sheet "
							+ rs.getName() + " : " + e.getMessage());
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

	public void Finalize() {

	}

}
