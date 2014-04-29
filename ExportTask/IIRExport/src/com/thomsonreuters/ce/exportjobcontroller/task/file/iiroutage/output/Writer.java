package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.output;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.cache.ProcessingStatus;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.IIRTSProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.ConcurrentDateUtil;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Utility;

public class Writer {

	protected Utility util;
	private static Logger logger = Starter.getLogger(IIRTSProcessor.SERVICE_NAME);
	protected String fileNamePrefix;
	long historyId = -1;
	protected String fileLocation;

	protected static String preDateStr = "0000";
	protected static long serialNum = 0;

	public Writer() throws IOException {
		this.loadConfig();
		this.util = new Utility();
	}

	private void loadConfig() throws IOException {
		Properties pros = new Properties();
		FileInputStream fs = new FileInputStream(Starter.Config_File);
		pros.load(fs);

		this.fileLocation = pros.getProperty("IIR_TS.fileLocation");
		this.fileLocation = FileUtilities.GetAbsolutePathFromEnv(this.fileLocation);
		this.fileNamePrefix = pros.getProperty("IIR_TS.fileNamePrefix");
	}

	protected BufferedWriter buildFileWriter(File tempFile, String fileName,
			String header) throws IOException {
		// Delete temp file if it exists
		if (tempFile.exists() == true) {
			tempFile.delete();
		}
		tempFile.createNewFile();

		ZipOutputStream zos = new ZipOutputStream(
				new FileOutputStream(tempFile));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zos,
				"UTF8"));

		ZipEntry entry = new ZipEntry(fileName + ".csv");
		zos.putNextEntry(entry);

		bw.write(header);
		return bw;
	}

	protected void moveFile(File tempFile, String fileName) {
		File destFile = new File(this.fileLocation, fileName + ".zip");

		// Delete temp file if it exists
		if (destFile.exists()) {
			destFile.delete();
		}

		util.moveFile(tempFile, destFile);

		if (!Constants.ISTEST)
			completeFileHis(historyId);

		logger.info(Constants.IIR_EMSG_PREFIX + "IIROutageExport File: "
				+ fileName + " has been generated");
	}

	/**
	 * create file name with the information which were got from DB and config
	 * file
	 * 
	 * @throws SQLException
	 */
	protected synchronized String buildFileName() throws SQLException {

		String fileName = "default";
		if (Constants.ISTEST) {
			return fileName;
		}

		String cdateStr = ConcurrentDateUtil.formatToyMd(new Date());

		if (serialNum == 0) {
			Connection conn = null;
			try {
				conn = new EasyConnection(DBConnNames.CEF_CNR);
				PreparedStatement ps = conn
						.prepareStatement(Constants.SQL_GET_PRE_FILENAME);
				ps.setInt(1, FileCategory.getInstance("IIROutageExport")
						.getID());
				ps.setString(2, cdateStr);
				ps.setInt(3, ProcessingStatus.COMPLETED.getID());
				ResultSet rs = ps.executeQuery();

				if (rs.next()) {
					String latestFilename = rs.getString(1);
					String serialStr = latestFilename.substring(latestFilename
							.lastIndexOf("_") + 1);
					serialNum = Long.parseLong(serialStr) + 1;
				} else {
					serialNum = 1;
				}

				rs.close();
				ps.close();

			} finally {
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			}
		} else {
			if (preDateStr.equals(cdateStr)) {
				serialNum++;
			} else {
				serialNum = 1;
			}
		}
		// TODO:should be controled if multi threads.
		String fileNameDate = preDateStr = cdateStr;
		String strSerialNum = String.valueOf(serialNum);
		String fileNameSerial = Constants.ORIGINAL_SERIALNUM.substring(
				0,
				Constants.ORIGINAL_SERIALNUM.length()
						- String.valueOf(serialNum).length())
				+ strSerialNum;

		fileName = this.fileNamePrefix + "_" + fileNameDate + "_"
				+ fileNameSerial;

		historyId = createFileProcessHistory(fileName);

		return fileName;
	}

	protected long createFileProcessHistory(String fileName)
			throws SQLException {
		long historyId = 0;
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);

			DatabaseMetaData dmd = conn.getMetaData();
			PreparedStatement objPreStatement = conn
					.prepareStatement(Constants.SQL_CREATE_PROCESS_HISTORY,
							new String[] { "ID" });
			objPreStatement.setString(1, fileName);
			objPreStatement.setInt(2,
					FileCategory.getInstance("IIROutageExport").getID());
			objPreStatement
					.setInt(3, ProcessingStatus.PROCESSING.getID());

			objPreStatement.executeUpdate();

			// get ID
			if (dmd.supportsGetGeneratedKeys()) {
				ResultSet rs = objPreStatement.getGeneratedKeys();
				while (rs.next()) {
					historyId = rs.getLong(1);
				}
			}
			conn.commit();
			objPreStatement.close();

		} finally {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
		return historyId;

	}

	protected void completeFileHis(long historyId) {
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);
			PreparedStatement objPreStatement = null;
			objPreStatement = conn
					.prepareStatement(Constants.SQL_COMPLETE_FILE_HISTORY);

			objPreStatement.setInt(1, ProcessingStatus.COMPLETED.getID());

			objPreStatement.setLong(2, historyId);
			objPreStatement.executeUpdate();
			conn.commit();
			objPreStatement.close();

		} catch (SQLException e) {
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "IIROutageExport: Failed to update the status of history record in db: SQLException");
			e.printStackTrace();
		} finally {
			try {
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(Constants.IIR_EMSG_PREFIX
						+ "Faild to close db connection in completeFileHis");
			}
		}
	}
}
