/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.output;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.IIRTSProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.AnalyticTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DateRegion;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.ConcurrentDateUtil;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.AnalyticType;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.DataType;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

/**
 * @author lei.yang
 * 
 */
public class IIRWriter extends Writer implements Writable {

	private Map<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> outputContents;
	private static Logger logger = Starter.getLogger(IIRTSProcessor.SERVICE_NAME);

	public IIRWriter(
			Map<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> contents)
			throws IOException {
		super();
		this.outputContents = contents;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.output.
	 * Write#write(java.lang.Object)
	 */
	@Override
	public void write() throws IOException, SQLException {
		logger.info(Constants.IIR_EMSG_PREFIX
				+ " Create file and write content...");

		this.createInstallFile(this.outputContents);
		this.createRealCapacityFile(this.outputContents);
	}

	private void createInstallFile(
			Map<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> outputContents)
			throws IOException, SQLException {

		String fileName = buildFileName();
		File tempFile = new File(this.fileLocation, fileName + ".zip.temp");
		BufferedWriter bw = buildFileWriter(tempFile, fileName,
				Constants.IIR_INSTALL_FILE_HEADER);

		long count = 0;
		Iterator<Entry<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>>> itAll = outputContents
				.entrySet().iterator();
		while (itAll.hasNext()) {
			Entry<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> bigEntry = itAll
					.next();
			AnalyticTask task = bigEntry.getKey();
			if (task.getAnalyticTypeName().equals(
					AnalyticType.IIR_AGG_REAL_CAPACITY)
					|| task.getAnalyticTypeName().equals(
							AnalyticType.IIR_REAL_CAPACITY))
				continue;

			HashMap<DataType, TreeMap<DateRegion, Long>> contents = bigEntry
					.getValue();
			TreeMap<DateRegion, Long> installContents = contents
					.get(DataType.INSTALL_CAPACITY);

			if (installContents == null || installContents.size() <= 0) {
				continue;
			}

			Iterator<Entry<DateRegion, Long>> installIT = installContents
					.entrySet().iterator();
			while (installIT.hasNext()) {
				Entry<DateRegion, Long> entry = installIT.next();
				DateRegion clRegion = entry.getKey();
				Long value = entry.getValue();
				Date from = clRegion.getStartDate();
				Date to = clRegion.getEndDate();
				while (!from.after(to)) {
					String content = task.getRic() + ","
							+ ConcurrentDateUtil.formatToyMd(from) + ","
							+ value + "\n";
					bw.write(content);
					count++;
					if (count == Constants.FILESIZE) {
						bw.flush();
						bw.close();
						moveFile(tempFile, fileName);
						fileName = buildFileName();
						tempFile = new File(this.fileLocation, fileName
								+ ".zip.temp");
						bw = buildFileWriter(tempFile, fileName,
								Constants.IIR_INSTALL_FILE_HEADER);
						count = 0;
					}
					from = util.addDays(from, 1);
				}
			}
		}
		bw.flush();
		bw.close();
		if (count % Constants.FILESIZE > 0) {
			moveFile(tempFile, fileName);
		} else if (count == 0) {
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "createInstallFile->"
					+ fileName
					+ ": no record had been writted into current temp file, clear up!");

			deleteEmptyFile(tempFile, fileName);
		}

	}

	private void createRealCapacityFile(
			Map<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> aggContents)
			throws IOException, SQLException {

		String fileName = buildFileName();
		File tempFile = new File(this.fileLocation, fileName + ".zip.temp");
		BufferedWriter bw = buildFileWriter(tempFile, fileName,
				Constants.IIR_REAL_FILE_HEADER);

		long count = 0;
		Iterator<Entry<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>>> itAll = aggContents
				.entrySet().iterator();
		while (itAll.hasNext()) {
			Entry<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> bigEntry = itAll
					.next();
			AnalyticTask task = bigEntry.getKey();
			if (task.getAnalyticTypeName().equals(
					AnalyticType.IIR_AGG_INSTALLED_CAPACITY))
				continue;

			HashMap<DataType, TreeMap<DateRegion, Long>> contents = bigEntry
					.getValue();
			TreeMap<DateRegion, Long> installContents = contents
					.get(DataType.INSTALL_CAPACITY);
			TreeMap<DateRegion, Long> outageContents = contents
					.get(DataType.OUTAGE);

			Iterator<Entry<DateRegion, Long>> installIT = installContents
					.entrySet().iterator();
			if (installContents.isEmpty()) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "installContents of task: " + task.getRreId()
						+ " is empty!");
				continue;
			}
			while (installIT.hasNext()) {
				Entry<DateRegion, Long> entry = installIT.next();
				DateRegion clRegion = entry.getKey();
				Long outage = 0L;
				if (!outageContents.isEmpty()
						&& outageContents.get(clRegion) != null)
					outage = outageContents.get(clRegion);

				Long value = entry.getValue() - outage;
				value = value < 0L ? 0L : value;

				Date from = clRegion.getStartDate();
				Date to = clRegion.getEndDate();
				while (!from.after(to)) {
					String content = task.getRic() + ","
							+ ConcurrentDateUtil.formatToyMd(from) + ","
							+ value + "\n";
					bw.write(content);
					count++;
					if (count == Constants.FILESIZE) {
						bw.flush();
						bw.close();
						moveFile(tempFile, fileName);
						fileName = buildFileName();
						tempFile = new File(this.fileLocation, fileName
								+ ".zip.temp");
						bw = buildFileWriter(tempFile, fileName,
								Constants.IIR_REAL_FILE_HEADER);
						count = 0;
					}
					from = util.addDays(from, 1);
				}
			}
		}
		bw.flush();
		bw.close();
		if (count % Constants.FILESIZE > 0) {
			moveFile(tempFile, fileName);
		} else if (count == 0) {
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "createRealCapacityFile->"
					+ fileName
					+ ": no record had been writted into current temp file, clear up!");

			deleteEmptyFile(tempFile, fileName);
		}

	}

	private void deleteEmptyFile(File tempFile, String fileName)
			throws SQLException {
		// delete from local disk
		if (tempFile.exists() == true) {
			tempFile.delete();
		}

		// delete record from file process history table
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);

			PreparedStatement ps = conn
					.prepareStatement(Constants.SQL_DELETE_EMPTYFILE_RECORD);
			ps.setLong(1, this.historyId);
			ps.executeUpdate();
			conn.commit();
			ps.close();
			if (serialNum > 0) {
				serialNum--;
			}
		} finally {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}
}
