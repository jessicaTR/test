package com.thomsonreuters.ce.taskcontroller.task.file.vesselzoneexport;

import java.io.File;
import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTaskProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.emailExport.emailConfigGenerator;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.spatialsdi.SpatialSDI;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class zoneKmlExport extends ExportTaskProcessor {
	public static final String FILE_PREFIX = "SPATIAL_KML_EXPORT";
	// private static final String GetTaskType =
	// "select tad.name from task_queue taq,task_def tad where tas.tad_id = tad.id and tas.id=?";
	private static final String GetParms = "{ call task_data_collector_pkg.collect_task_parm_proc(?,?,?,?,?)}";
	static final String sender = "cne.vessels@thomsonreuters.com";
	public static String SERVICE_NAME = "SPATIAL_KML_EXPORT";
	private static Logger logger = Starter.getLogger(SERVICE_NAME);
	String task_type = null;
	String receivers = null;
	String cc = null;
	String parameters = null;

	@Override
	public void processTask(ArrayList<ExportTask> taskList) {
		Connection DBConn = null;
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			List<Integer> taskIdList = emailConfigGenerator
					.getTaskIDs(taskList);
			// long tmp_id = taskIdList.get(0);

			for (int i = 0; i < taskIdList.size(); i++) {
				if (this.isShuttingDown()) {
					return;
				}

				receivers = null;
				cc = null;
				parameters = null;

				long tas_id = taskIdList.get(i);
				this.UpdateTaskStatus(Long.valueOf(tas_id), "PROCESSING");
				getParms(DBConn, tas_id);
				logger.info(FILE_PREFIX + " Start to process task:" + tas_id);
				System.out.println(FILE_PREFIX + " Start to process task:"
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
								zipFileName, "SPATIAL_KML_EXPORT");
					} else {
						Subject = "[C&E APEX] Attention: new files generated for ["
								+ task_type + "] with id=[" + tas_id + "]";
						String attachedFileNames = File.getName();
						File cfgFile = emailConfigGenerator.GenEmailCfgFile(
								task_type, Subject, sender, receivers, cc,
								parameters, attachedFileNames);
						File[] SourceFiles = new File[] { File, cfgFile };
						String zipFileName = "EMAIL_" + task_type + "_"
								+ tas_id + "_";
						emailConfigGenerator.fileToZip(SourceFiles,
								zipFileName, "SPATIAL_KML_EXPORT");
					}

					this.UpdateTaskStatus(Long.valueOf(tas_id), "COMPLETED");
					logger.info(FILE_PREFIX + " complete task:" + tas_id);
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

	public File getResultFile(Connection DBConn, long tas_id) {
		File file = null;
		String tempfilelocation = Starter.getTempFolder(SERVICE_NAME);
		File gzfile = null;
		ThreadController tc = new ThreadController();
		SpatialSDI zonekmlgenerator = new SpatialSDI(tc);
		try {
			System.out.println(FILE_PREFIX
					+ " Start to call spatial sdi to create file");
			String strTimeStamp = emailConfigGenerator
					.getStringDate(emailConfigGenerator.currentDate());
			String filename = task_type + "_" + tas_id + "_" + strTimeStamp
					+ ".kml.gz";
			zonekmlgenerator.tempfilelocation = tempfilelocation;
			FileInputStream TaskFis = new FileInputStream(Starter.Config_File);
			Properties Prop = new Properties();
			Prop.load(TaskFis);
			String filelocation = Prop.getProperty(SERVICE_NAME
					+ ".filelocation");
			filelocation = FileUtilities.GetAbsolutePathFromEnv(filelocation);
			zonekmlgenerator.filelocation = filelocation;
			zonekmlgenerator
					.SDIProducer("VesselZone", filename, "FULL", DBConn);
			file = new File(filelocation, filename);
			gzfile = new File(tempfilelocation, filename);

			if (gzfile.exists() == true) {
				gzfile.delete();
			}
			zonekmlgenerator.MoveFile(file, gzfile);
			System.out.println(FILE_PREFIX + " sdi file complete.");
		} catch (Exception e) {

			if (e instanceof LogicException) {
				logger.warn(
						"Logic Exception is captured while producing SPATIAL_KML_EXPORT file for task "
								+ tas_id, e);

			} else if (e instanceof SystemException) {
				SystemException se = (SystemException) e;
				logger.warn(
						"System Exception: "
								+ se.getEventID()
								+ " is captured while producing SPATIAL_KML_EXPORT file for task "
								+ tas_id, e);
			} else {
				SystemException se = new SystemException(e);
				logger.warn(
						"System Exception: "
								+ se.getEventID()
								+ " is captured while producing SPATIAL_KML_EXPORT file for task "
								+ tas_id, e);
			}
		}
		return gzfile;
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

	public void getParms(Connection DBConn, long tas_id) {
		try {
			CallableStatement objStatement = null;
			objStatement = DBConn.prepareCall(GetParms);
			objStatement.setLong(1, tas_id);
			objStatement.registerOutParameter(2, java.sql.Types.VARCHAR);
			objStatement.registerOutParameter(3, java.sql.Types.VARCHAR);
			objStatement.registerOutParameter(4, java.sql.Types.VARCHAR);
			objStatement.registerOutParameter(5, java.sql.Types.VARCHAR);
			objStatement.execute();

			task_type = objStatement.getString(2);
			receivers = objStatement.getString(3);
			cc = objStatement.getString(4);
			parameters = objStatement.getString(5);

			objStatement.close();

		} catch (SQLException e) {
			throw new LogicException("Get parameters error when tas_id="
					+ tas_id + "!" + e.getMessage());
		}
	}
}
