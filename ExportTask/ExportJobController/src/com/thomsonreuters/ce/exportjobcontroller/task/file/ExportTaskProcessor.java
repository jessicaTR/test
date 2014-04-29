package com.thomsonreuters.ce.exportjobcontroller.task.file;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.extract.Task;
import com.thomsonreuters.ce.dbor.extract.TaskProcessor;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exportjobcontroller.Starter;

public abstract class ExportTaskProcessor implements TaskProcessor {
	private static Logger logger = Starter.getLogger(Starter.SERVICE_NAME);

	public boolean isShuttingDown() {
		return TS.IsShuttingDown();
	}

	private TaskScanner TS;

	public void setScanner(TaskScanner ts) {
		TS = ts;
	}

	public void UpdateTaskStatus(Long ID, String Status) {
		Connection DBConn = null;

		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objPreStatement = DBConn
					.prepareCall("{call task_maintain_pkg.update_task_queue_func(?,?,?,?)}");
			objPreStatement.setLong(1, ID);
			objPreStatement.setString(2, Status.toUpperCase());
			objPreStatement.setNull(3, Types.DATE);
			objPreStatement.setNull(4, Types.DATE);
			objPreStatement.execute();
			DBConn.commit();
			objPreStatement.close();
		} catch (SQLException e) {
			logger.warn("SQL exception", e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				logger.warn("SQL exception", e);
			}
		}

	}

	@Override
	public void processTask(Map<Integer, Task> taskMap) {
		// TODO Auto-generated method stub
		
	}
	
	public abstract void processTask(ArrayList<ExportTask> taskList);
}
