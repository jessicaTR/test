package com.thomsonreuters.ce.exportjobcontroller.task.file;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.extract.TaskType;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;

public class TaskScanner extends ControlledThread {

	private TaskType taskRunType;
	private static Logger logger = Starter.getLogger(Starter.SERVICE_NAME);
	private final static String SQL_1 = "select id,tad_id,task_type from cef_cnr.task_queue where (status='WAITING' or status='PROCESSING') and tad_id = ? order by id";
	private ArrayList<ExportTask> taskList = null;

	public TaskScanner(TaskType taskType, ThreadController tc) {
		super(tc);
		this.taskRunType = taskType;
	}

	public void ControlledProcess() {

		taskList = new ArrayList<ExportTask>();

		Connection DBConn = null;
		PreparedStatement objPreStatement = null;
		ResultSet objResult = null;

		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);

			objPreStatement = DBConn.prepareStatement(SQL_1);
			objPreStatement.setInt(1,
					TaskCategory.getInstance(this.taskRunType.getName())
							.getID());
			objResult = objPreStatement.executeQuery();
			int taskDefID = -1;
			while (objResult.next()) {
				int taskId = objResult.getInt(1);
				taskDefID = objResult.getInt(2);

				String taskType = null;
				if (objResult.getString(3) != null) {
					taskType = objResult.getString(3).toUpperCase();
				}
				ExportTask thisTask = new ExportTask(taskId, taskDefID,
						taskType);
				taskList.add(thisTask);

			}

			objResult.close();
			objPreStatement.close();

			// ///////////////////////////////////////////
			// Initialize task processor
			ExportTaskProcessor taskProcessor = null;

			// ///////////////////////////////////////////
			// process task
			try {
				if (!taskList.isEmpty()) {
					try {
						taskProcessor = (ExportTaskProcessor) Class.forName(
								this.taskRunType.getTaskProcessor())
								.newInstance();
						taskProcessor.setScanner(this);

					} catch (Exception e) {
						logger.fatal(this.taskRunType.getName()
								+ ": Can not initialize task processor: "
								+ this.taskRunType.getTaskProcessor());
						return;
					}

					logger.info(this.taskRunType.getName()
							+ ": Task processor starts processing task.");
					taskProcessor.processTask(taskList);
					logger.info(this.taskRunType.getName()
							+ ": Task processor has completed.");
				}
			} catch (Exception e) {
				if (e instanceof LogicException) {
					try {
						logger.warn(this.taskRunType.getName()
								+ ": Logic exception is thrown:", e);
					} catch (Exception innererr) {
						SystemException se = new SystemException(
								"Unknown Exception ", innererr);

						logger.warn(this.taskRunType.getName()
								+ ": System Exception: " + se.getEventID(),
								innererr);
					}
				} else if (e instanceof SystemException) {
					try {

						SystemException se = (SystemException) e;
						logger.warn(this.taskRunType.getName()
								+ ": System Exception: " + se.getEventID(), e);
					} catch (Exception innererr) {

						SystemException se = new SystemException(
								"Unknown Exception ", innererr);
						logger.warn(this.taskRunType.getName()
								+ ": System Exception: " + se.getEventID(),
								innererr);

					}
				} else if (e instanceof NullPointerException) {
					logger.warn(this.taskRunType.getName()
							+ ": NullPointerException is thrown: ", e);
				} else {
					SystemException se = new SystemException(
							"Unknown Exception ", e);
					logger.warn(this.taskRunType.getName()
							+ ": System Exception: " + se.getEventID(), e);
				}// end of if

				// TP.UpdateTaskStatus(ID, "FAILED");

			}
		} catch (SQLException ex) {
			throw new SystemException("Database error", ex);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException ex) {
				// TODO Auto-generated catch block
				throw new SystemException("Database error", ex);
			}
		}

		logger.debug(this.taskRunType.getName() + ": Task scan is done");

		if (IsShuttingDown()) {
			return;
		}

		TaskScanner TS = new TaskScanner(this.taskRunType, this.TC);
		Starter.TimerService.createTimer(this.taskRunType.getInterval(), 0, TS);
	}

	public Date strToDateLong(String strDate) {
		try {
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
					Locale.ENGLISH);
			java.util.Date date = formatter.parse(strDate);
			return (new Date(date.getTime()));
		} catch (Exception e) {
			DateFormat ft = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
			try {
				return (new Date(ft.parse(strDate).getTime()));
			} catch (ParseException e1) {
				logger.error("Failed to parse datastr :" + strDate, e);
				return null;
			}
		}
	}

}