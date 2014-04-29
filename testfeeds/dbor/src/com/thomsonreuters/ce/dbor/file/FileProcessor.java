package com.thomsonreuters.ce.dbor.file;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.sql.DatabaseMetaData;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.ProcessingStatus;
import com.thomsonreuters.ce.dbor.server.DBConnNames;


public abstract class FileProcessor {
	
	protected long FPH_ID;
	
	private static final String InsertFileProcessHistory = "insert into file_process_history (id, file_name,dit_file_category_id,start_time,dit_processing_status) values (fph_seq.nextval,?,?,sysdate,?)";

	private static final String CompleteFileHistory = "update file_process_history set end_time=sysdate, dit_processing_status=? where id=?";
	
	private static final String InsertProcessingDetail = "insert into processing_detail (fph_id,log_time,dit_message_category_id,message) values (?,sysdate,?,?)";
	
	private static final String GetProcessingDetail = "select count(*) from processing_detail where fph_id = ? and dit_message_category_id in (select id from dimension_item where value='WARNING')";
	
	
	public void CreateFileProcessHistory(File FeedFile)
	{
		Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);
		try {
			DatabaseMetaData dmd = DBConn.getMetaData();
			PreparedStatement objPreStatement = DBConn.prepareStatement(InsertFileProcessHistory, new String[]{"ID"});
			objPreStatement.setString(1, FeedFile.getName());
			objPreStatement.setInt(2, getFileCatory(FeedFile).getID());
			objPreStatement.setInt(3, ProcessingStatus.PROCESSING.getID());

			objPreStatement.executeUpdate();
			
			//get ID			
			if(dmd.supportsGetGeneratedKeys()) {   
				ResultSet rs = objPreStatement.getGeneratedKeys();   
			    while(rs.next()) {
			    	this.FPH_ID=rs.getLong(1);
			    }
			}
			
			DBConn.commit();
			objPreStatement.close();
		}
		catch (SQLException e) {
			throw new SystemException("Unknown DB Expection: ", e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				throw new SystemException("Unknown DB Expection: ", e);
			}
		}	
	}
	
	public void UpdateFileHisToFailed()
	{
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

		try {
			PreparedStatement objPreStatement = null;
			objPreStatement = DBConn.prepareStatement(CompleteFileHistory);
			objPreStatement.setInt(1, ProcessingStatus.FAILED.getID());
			objPreStatement.setLong(2, this.FPH_ID);
			objPreStatement.executeUpdate();
			DBConn.commit();
			objPreStatement.close();
		} catch (SQLException e) {
			throw new SystemException("Unknown DB Expection: ", e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				throw new SystemException("Unknown DB Expection: ", e);
			}
		}		
	}
	
	
	public void CompleteFileHis()
	{
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		
		
		try {
			// if processint_detail table has records, then it's
			// COMPLETEDWITHWARNING
			int pdeCount=-1;
			PreparedStatement getPdetPreStatement = DBConn
					.prepareStatement(GetProcessingDetail);
			getPdetPreStatement.setLong(1, this.FPH_ID);
			ResultSet objResultSet = getPdetPreStatement.executeQuery();
			if (objResultSet.next()) {
				pdeCount = objResultSet.getInt(1);
			}
			
			objResultSet.close();
			getPdetPreStatement.close();
			
			PreparedStatement objPreStatement = null;
			objPreStatement = DBConn.prepareStatement(CompleteFileHistory);
			
			if (pdeCount <= 0) {
				objPreStatement.setInt(1, ProcessingStatus.COMPLETED.getID());
			} else {
				objPreStatement.setInt(1, ProcessingStatus.COMPLETEDWITHWARNING
						.getID());
			}			
			
			objPreStatement.setLong(2,this.FPH_ID);
			objPreStatement.executeUpdate();
			DBConn.commit();
			objPreStatement.close();
			
		} catch (SQLException e) {
			throw new SystemException("Unknown DB Expection: ", e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				throw new SystemException("Unknown DB Expection: ", e);
			}
		}		
	}
	
	public abstract void ProcessFile(File FeedFile);
	
	public abstract FileCategory getFileCatory(File FeedFile);
	
	public  void LogDetails(MsgCategory mc, String Msg)
	{
		Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);
		try {		
			if (Msg.length()>2000)
			{
				Msg=Msg.substring(0,2000);
			}
			PreparedStatement objPreStatement = DBConn.prepareStatement(InsertProcessingDetail);
			objPreStatement.setLong(1, this.FPH_ID);
			objPreStatement.setInt(2, mc.getID());
			objPreStatement.setString(3, Msg);

			objPreStatement.executeUpdate();
			
			DBConn.commit();
			objPreStatement.close();
		}
		catch (SQLException e) {
			throw new SystemException("Unknown DB Expection: ", e);
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				throw new SystemException("Unknown DB Expection: ", e);
			}
		}		
		
	}
}
