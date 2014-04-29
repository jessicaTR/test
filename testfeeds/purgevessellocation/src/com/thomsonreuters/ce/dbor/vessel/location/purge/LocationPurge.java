package com.thomsonreuters.ce.dbor.vessel.location.purge;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import oracle.jdbc.OracleTypes;


import com.thomsonreuters.ce.timing.DateConstants;
import com.thomsonreuters.ce.timing.DateFun;
import com.thomsonreuters.ce.timing.Schedule;
import com.thomsonreuters.ce.timing.ScheduleType;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class LocationPurge extends ControlledThread {

	private static final String CheckDBTime = "select to_char(sysdate,'yyyymmddhh24miss') from dual";

	private static final String getVessels = "{call vessel_location_purge_pkg.get_vessel_purge_list_proc(?,?)}";

	private Date CurrentDBPurgeTime=null;

	private Date PreviousPurgeTime=null;

	private int ThreadCounter=0;
	
	private char scheduletype;
	
	private String scheduletime;
	
	private int threadnum;

	public LocationPurge(ThreadController tc, char sty, String sti, int tn)
	{
		super(tc);
		
		this.scheduletype=sty;
		this.scheduletime=sti;
		this.threadnum=tn;
	}

	@Override
	public void ControlledProcess() {
		// TODO Auto-generated method stub


		try {


			if (CurrentDBPurgeTime!=null) {

				Starter.vlplogger.info("[Vessel location purge]:Identifying vessels with previous purge time:"+PreviousPurgeTime);

				Connection DBConn = null;

				ArrayList<Long> VesselLists = new ArrayList<Long>();

				try {
					DBConn = new EasyConnection(DBConnNames.CEF_CNR);
					CallableStatement objStatement=DBConn.prepareCall(getVessels);

					if (PreviousPurgeTime!=null)
					{
						objStatement.setTimestamp("start_time_in", new Timestamp(PreviousPurgeTime.getTime()));
					}
					else
					{
						objStatement.setNull("start_time_in", OracleTypes.TIMESTAMP);
					}

					objStatement.registerOutParameter("cur_out", OracleTypes.CURSOR);
					objStatement.execute();
					ResultSet result_set = (ResultSet) objStatement.getObject("cur_out");
					result_set.setFetchSize(5000);

					while (result_set.next()) {
						Long VesselID = result_set.getLong(1);
						VesselLists.add(VesselID);
					}
					result_set.close();
					objStatement.close();

				} finally
				{
					try {
						DBConn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						Starter.vlplogger.warn(
								"[Vessel location purge]: SQLException:", e);
					}

				}


				Starter.vlplogger.info("[Vessel location purge]:Locations for " + VesselLists.size()
						+ " vessels need to be purged out");

				this.ThreadCounter = threadnum;

				Starter.vlplogger.info("[Vessel location purge]:Starting purging locations");


				for (int i = 0; i < threadnum; i++) {
					new Thread(new PurgeVesselLocation(VesselLists)).start();
				}

				synchronized (VesselLists) {
					if (this.ThreadCounter > 0) {
						VesselLists.wait();
					}
				}

				Starter.vlplogger.info("[Vessel location purge]:Location purge is done");

			}

			if (!this.IsShuttingDown())
			{
				Date DBDate = getDBDate();
				Date nextDBExecuteTime = (new Schedule(DBDate,ScheduleType.getInstance(scheduletype), scheduletime)).GetNextValidTime();

				Date AppDate = new Date();
				long timediff = DBDate.getTime() - AppDate.getTime();

				Date NextRunningTime = new Date(nextDBExecuteTime.getTime() - timediff);

				Starter.vlplogger.info(
						"[Vessel location purge]: Next execute time on SFS:"
								+ DateFun.getStringDate(NextRunningTime));

				LocationPurge LP = new LocationPurge(this.TC,this.scheduletype,this.scheduletime,this.threadnum);
				LP.CurrentDBPurgeTime = nextDBExecuteTime;
				LP.PreviousPurgeTime = this.CurrentDBPurgeTime;
				Starter.TimerService.createTimer(NextRunningTime,0, LP);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			Starter.vlplogger.warn(
					"[Vessel location purge]: Exception:", e);
		} 
	}

	public Date CalculateNextRunningTime(char scheduletype,String scheduletime) throws Exception{
		// Get DB and APP Server Time
		Date DBDate = getDBDate();
		Date AppDate = new Date();

		// Get time diff
		long timediff = DBDate.getTime() - AppDate.getTime();

		// Calculate next validate DB time;
		Date nextDBExecuteTime = (new Schedule(DBDate,ScheduleType.getInstance(scheduletype), scheduletime)).GetNextValidTime();

		// Calculate next validate APP time
		Date nextAppExecuteTime = new Date(nextDBExecuteTime.getTime() - timediff);

		return nextAppExecuteTime;
	}

	private Date getDBDate() throws Exception
	{
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		PreparedStatement objPreStatement = null;
		ResultSet objResult = null;

		try {
			// //////////////////////////////////////////////////////////////////////
			// get attributes
			objPreStatement = DBConn.prepareStatement(CheckDBTime);
			objResult = objPreStatement.executeQuery();

			objResult.next();
			String strDBDate = objResult.getString(1);

			SimpleDateFormat formatter = new SimpleDateFormat(
					DateConstants.FULLTIMEFORMAT);
			ParsePosition pos = new ParsePosition(0);
			Date DBDate = formatter.parse(strDBDate, pos);

			objResult.close();
			objPreStatement.close();

			return DBDate;

		} finally {
			DBConn.close();

		}
	}    

	class PurgeVesselLocation implements Runnable {
		private ArrayList<Long> VesselLists;


		PurgeVesselLocation(ArrayList<Long> v_ids) {
			this.VesselLists = v_ids;
		}

		public void run() {

			Connection ConnectionForPurgingVesselLocation = new EasyConnection(
					DBConnNames.CEF_CNR);
			try {
				String purge_location_event = "{call vessel_location_purge_pkg.purge_location_proc(?)}";
				CallableStatement objStatement = ConnectionForPurgingVesselLocation
						.prepareCall(purge_location_event);

				while (true) {
					Long Vessel_ID=null;

					synchronized (this.VesselLists) {
						if (this.VesselLists.isEmpty() || LocationPurge.this.IsShuttingDown()) {
							break;
						} else {
							Vessel_ID = this.VesselLists.remove(0);
						}
					}


					objStatement.setLong(1, Vessel_ID);
					objStatement.execute();
					ConnectionForPurgingVesselLocation.commit();

				}

				objStatement.close();

			} catch (SQLException e) {
				Starter.vlplogger.warn("[Vessel location purge]: SQLException: ", e);
			} finally {

				try {
					ConnectionForPurgingVesselLocation.close();
				} catch (SQLException e) {
					Starter.vlplogger.warn("[Vessel location purge]: SQLException: ", e);
				}

				synchronized (this.VesselLists) {

					LocationPurge.this.ThreadCounter--;

					if (LocationPurge.this.ThreadCounter == 0) {
						this.VesselLists.notify();
					}
				}
			}
		}
	}

}
