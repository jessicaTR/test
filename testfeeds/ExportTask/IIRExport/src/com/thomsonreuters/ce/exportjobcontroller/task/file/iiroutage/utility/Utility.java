package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.IIRTSProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.TaskStatus;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class Utility {
	private static Logger logger = Starter.getLogger(IIRTSProcessor.SERVICE_NAME);
	public void updateStaskStatus(List<Integer> lstIds, TaskStatus status) {		
		
		if (lstIds.size() > 0) {
			
			String sql = null;
			if (status.equals(TaskStatus.PROCESSING))
				sql = "update cef_cnr.task_queue set status=?, start_time=? where id=?";
			else if (status.equals(TaskStatus.COMPLETED) || status.equals(TaskStatus.FAILED))
				sql = "update cef_cnr.task_queue set status=?, end_time=? where id=?";
			else if (status.equals(TaskStatus.WAITING))
				sql = "update cef_cnr.task_queue set status=?, start_time=?, end_time=? where id=?";

			Connection conn = null;
			try {
				conn = new EasyConnection(DBConnNames.CEF_CNR);
				conn.setAutoCommit(false);
				conn.getAutoCommit();
				PreparedStatement ps = conn.prepareStatement(sql);
				for (Integer id : lstIds) {
					ps.setString(1, status.toString());
					if (status.equals(TaskStatus.WAITING)) {
						ps.setTimestamp(2, null);
						ps.setTimestamp(3, null);
						ps.setInt(4, id);
					} else {
						ps.setTimestamp(2, new java.sql.Timestamp((new java.util.Date()).getTime()));
						ps.setInt(3, id);
					}
					ps.addBatch();
				}

				ps.executeBatch();
				conn.commit();
				//conn.setAutoCommit(true);
			} catch (SQLException e) {
				logger.error(Constants.IIR_EMSG_PREFIX + "IIROutageExport: Failed to update task status to "
								+ status + "for SQL Exception");
				e.printStackTrace();
			} finally{
				try {
					if(conn != null && !conn.isClosed()){
							conn.close();				
					}
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error( Constants.IIR_EMSG_PREFIX + "Faild to close db connection in updateStaskStatus");				
				}
			}
		}
	}
	
	public void moveFile(File f1, File f2) {
		try {
			int length = 1048576;
			FileInputStream in = new FileInputStream(f1);
			FileOutputStream out = new FileOutputStream(f2);
			byte[] buffer = new byte[length];

			while (true) {
				int ins = in.read(buffer);
				if (ins == -1) {
					in.close();
					out.flush();
					out.close();
					f1.delete();
					return;
				} else {
					out.write(buffer, 0, ins);
				}
			}
		} catch (FileNotFoundException e) {
			logger.error( "FileNotFoundException", e);
		} catch (IOException e) {
			logger.error( "IOException", e);
		}
	}
	
	public Date addDays(Date date, int days){		
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, days);
		return cal.getTime();
	}
}
