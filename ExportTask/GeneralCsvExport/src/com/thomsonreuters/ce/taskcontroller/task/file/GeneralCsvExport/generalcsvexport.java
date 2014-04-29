package com.thomsonreuters.ce.taskcontroller.task.file.GeneralCsvExport;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import oracle.jdbc.OracleTypes;
import au.com.bytecode.opencsv.CSVWriter;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTaskProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.emailExport.emailConfigGenerator;

public class generalcsvexport extends ExportTaskProcessor {
	public static final String FILE_PREFIX = "GENERAL_CSV_EXPORT";
	public static final String SERVICE_NAME = "GENERAL_CSV_EXPORT";
	private static Logger logger = Starter.getLogger(SERVICE_NAME);
	private static final String GetCursor = "{?=call task_data_collector_pkg.collect_general_csv_data_fn(?,?,?,?,?)}";
	// private static final String
	// GetTaskType="select tad.name from task_queue taq,task_def tad where tas.tad_id = tad.id and tas.id=?";
	static final String sender = "cne.vessels@thomsonreuters.com";
	String task_type = null;
	String receivers = null;
	String cc = null;
	String parameters = null;

	public void processTask(ArrayList<ExportTask> taskList) {
		Connection DBConn = null;
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			List<Integer> taskIdList = emailConfigGenerator
					.getTaskIDs(taskList);
			System.out.println("GENERAL_CSV_EXPORT is started.");

			for (int i = 0; i < taskIdList.size(); i++) {
				if (this.isShuttingDown()) {
					return;
				}
				receivers = null;
				cc = null;
				parameters = null;
				task_type = null;

				long tas_id = taskIdList.get(i);
				this.UpdateTaskStatus(Long.valueOf(tas_id), "PROCESSING");

				System.out.println("GENERAL_CSV_EXPORT Start to process task:"
						+ tas_id);

				try {
					// String FileNamePrefix="GENERAL_CSV_EXPORT";

					File File = getResultFile(DBConn, tas_id);
					long fileSizeInBytes = File.length();
					long fileSizeInMB = fileSizeInBytes / 1024 / 1024;
					String Subject = null;
					if (fileSizeInMB > 15) {
						Subject = "[C&E APEX] Attention: new files generated for ["
								+ task_type
								+ "] with id=["
								+ tas_id
								+ "]"
								+ ", as file size is bigger than 15M please contact support to download manually.";
						File cfgFile = emailConfigGenerator.GenEmailCfgFile(
								task_type, Subject, sender, receivers, cc,
								parameters, null);
						File[] SourceFiles = new File[] { cfgFile };
						String zipFileName = "EMAIL_" + task_type + "_"
								+ tas_id + "_";
						emailConfigGenerator.fileToZip(SourceFiles,
								zipFileName, "GENERAL_CSV_EXPORT");

					} else {
						Subject = "[C&E APEX] Attention: new files generated for ["
								+ task_type + "] with id=[" + tas_id + "]";
						String attachedFileNames = File.getName();
						// System.out.println(RAW_DESTINATION_PREFIX+"attachedFileNames:"+attachedFileNames);
						File cfgFile = emailConfigGenerator.GenEmailCfgFile(
								task_type, Subject, sender, receivers, cc,
								parameters, attachedFileNames);
						File[] SourceFiles = new File[] { File, cfgFile };
						String zipFileName = "EMAIL_" + task_type + "_"
								+ tas_id + "_";
						emailConfigGenerator.fileToZip(SourceFiles,
								zipFileName, "GENERAL_CSV_EXPORT");

					}
					// System.out.println(RAW_DESTINATION_PREFIX+" Matched raw destination file generated.");
					this.UpdateTaskStatus(Long.valueOf(tas_id), "COMPLETED");
					logger.info("GENERAL_CSV_EXPORT complete task:" + tas_id);
				} catch (Exception e) {
					this.UpdateTaskStatus(Long.valueOf(tas_id), "FAILED");
					logger.error(FILE_PREFIX + " failed to create file.", e);
				}
			}
		} catch (Exception e) {
			logger.error(FILE_PREFIX, e);
		} finally {
			try {
				if (DBConn != null && !DBConn.isClosed()) {
					DBConn.close();
				}
			} catch (SQLException e) {
				logger.error(FILE_PREFIX + " Faild to close db connection.");
			}
		}
	}

	// public String getTaskType(Connection DBConn,long tas_id){
	// String tasktype=null;
	// try {
	// PreparedStatement ps = DBConn.prepareStatement(GetTaskType);
	// ps.setLong(1, tas_id);
	// ResultSet objResult = ps.executeQuery();
	// if (objResult.next()) {
	// tasktype = objResult.getString(1);
	// }
	// objResult.close();
	// ps.close();
	// } catch (SQLException e) {
	// throw new LogicException(
	// "Get task type error when id=" + tas_id
	// + "!");
	// }
	// return tasktype;
	// }

	public File getResultFile(Connection DBConn, long tas_id) {
		// String FileContent = null;
		File file = null;
		// String locheader = null;
		CallableStatement objStatement = null;
		String tempfilelocation = Starter.getTempFolder(SERVICE_NAME);
		File gzfile = null;

		try {
			String strTimeStamp = emailConfigGenerator
					.getStringDate(emailConfigGenerator.currentDate());

			System.out
					.println("GENERAL_CSV_EXPORT Start generate file for task:"
							+ tas_id);
			// DBConn = new EasyConnection(Starter.PHIS_DB);
			objStatement = DBConn.prepareCall(GetCursor);
			objStatement.registerOutParameter(1, OracleTypes.CURSOR);
			objStatement.setLong(2, tas_id);
			objStatement.registerOutParameter(3, java.sql.Types.VARCHAR);
			objStatement.registerOutParameter(4, java.sql.Types.VARCHAR);
			objStatement.registerOutParameter(5, java.sql.Types.VARCHAR);
			objStatement.registerOutParameter(6, java.sql.Types.VARCHAR);
			objStatement.execute();

			ResultSet result_set = (ResultSet) objStatement.getObject(1);
			task_type = objStatement.getString(3);
			receivers = objStatement.getString(4);
			cc = objStatement.getString(5);
			parameters = objStatement.getString(6);
			//objStatement.close();

			String filename = task_type + "_" + tas_id + "_" + strTimeStamp
					+ ".csv";
			file = new File(tempfilelocation, filename);

			CSVWriter writer = new CSVWriter(new FileWriter(tempfilelocation
					+ "/" + file.getName()));
			writer.writeAll(result_set, true);
			writer.close();

			String gzFileName = task_type + "_" + tas_id + "_" + strTimeStamp
					+ ".csv.gz";
			gzfile = new File(tempfilelocation, gzFileName);

			FileOutputStream fileOutputStream = new FileOutputStream(
					tempfilelocation + "/" + gzFileName);

			GZIPOutputStream outputStream = new GZIPOutputStream(
					fileOutputStream);

			FileInputStream inputStream = new FileInputStream(tempfilelocation
					+ "/" + file.getName());

			byte[] buffer = new byte[1024];
			int length;
			while ((length = inputStream.read(buffer)) > 0) {
				outputStream.write(buffer, 0, length);
			}

			inputStream.close();
			outputStream.close();

			// System.out.println(tempfilelocation+"/"+file.getName());
			// ///////////////////////////
			// //Get Column Names
			// ResultSetMetaData rsmd = result_set.getMetaData();
			// int count = rsmd.getColumnCount();
			// String[] ColumnNameList=new String[count];
			//
			// for (int i = 0; i < count; i++) {
			// String columnname=rsmd.getColumnName(i+1);
			// ColumnNameList[i]=columnname;
			// locheader = locheader+","+columnname;
			// }
			// locheader = locheader.substring(1);
			// FileContent = locheader ;
			//
			// // result_set.setFetchSize(5000);
			// while (result_set.next()) {
			//
			// // HashMap<String, Object> RowValues=new HashMap<String,
			// Object>();
			// String rowStr="";
			// for (int i=0; i<count;i++)
			// {
			// Object value=null;
			//
			// if (rsmd.getColumnClassName(i+1)=="java.sql.Timestamp")
			// {
			// value = result_set.getTimestamp(i+1);
			// }
			// else
			// {
			// value=result_set.getObject(i+1);
			// }
			//
			// String cn=ColumnNameList[i];
			// if(value != null){
			// rowStr = rowStr + ","+"\""+value.toString()+"\"";
			//
			// }
			//
			// }
			// // System.out.println(rowStr);
			// FileContent = FileContent + "\r\n" +rowStr.substring(5);
			// }

			// String FileNameSuffix = ".csv.gz";
			//
			// file = emailConfigGenerator.GenDataFile(task_type,
			// FileNameSuffix, tas_id, FileContent);

		} catch (SQLException e) {
			logger.error(FILE_PREFIX + " Failed to get cursor data."
					+ e.getMessage());
		} catch (IOException e) {
			logger.error(FILE_PREFIX + " Failed to create csv file."
					+ e.getMessage());
		} finally {
			try {
				if (objStatement != null) {
					objStatement.close();
				}
			} catch (SQLException e) {
				logger.error(FILE_PREFIX + " Faild to close objStatement.");
			}
		}
		return gzfile;
	}
}
