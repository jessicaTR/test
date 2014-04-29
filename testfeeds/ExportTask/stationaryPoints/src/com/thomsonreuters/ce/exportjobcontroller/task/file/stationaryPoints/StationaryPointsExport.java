package com.thomsonreuters.ce.exportjobcontroller.task.file.stationaryPoints;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTaskProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.emailExport.emailConfigGenerator;
import com.thomsonreuters.ce.queue.DiskCache;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.io.BufferedWriter;

import org.apache.log4j.Logger;

import oracle.jdbc.OracleTypes;

public class StationaryPointsExport extends ExportTaskProcessor {

	public static final String STATIONARY_POINTS_PREFIX = "EXTRACT_VES_LOCS";
	public static final String SERVICE_NAME = "EXTRACT_VES_LOCS";
	private static Logger logger = Starter.getLogger(SERVICE_NAME);

	private java.sql.Timestamp currentDate() {

		java.util.Date longdate = new java.util.Date();
		java.sql.Date date = new java.sql.Date(longdate.getTime());
		java.sql.Time time = new java.sql.Time(longdate.getTime());
		java.sql.Timestamp curdate = java.sql.Timestamp.valueOf(date.toString()
				+ " " + time.toString());
		return curdate;
	}

	private static String getStringDate(Date time) {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
		String dateString = formatter.format(time);
		return dateString;
	}

	@Override
	public void processTask(ArrayList<ExportTask> taskList) {

		Connection DBConn = null;
		logger.info(STATIONARY_POINTS_PREFIX + " start!");
		try {
			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			List<Integer> taskIdList = emailConfigGenerator
					.getTaskIDs(taskList);
			CallableStatement getPointsCs = DBConn
					.prepareCall("{? = call task_data_collector_pkg.collect_stationary_points_fn(?,?,?,?,?,?,?,?,?)}");

			String locheader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
					+ "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\r\n"
					+ "<Document>\r\n"
					+ "<StyleMap id=\"msn_ylw-pushpin\">\r\n"
					+ "<Pair><key>normal</key><styleUrl>#sn_ylw-pushpin</styleUrl></Pair>\r\n"
					+ "<Pair><key>highlight</key><styleUrl>#sh_ylw-pushpin</styleUrl></Pair>\r\n"
					+ "</StyleMap>\r\n"
					+ "<Style id=\"sh_ylw-pushpin\"><IconStyle><scale>1.2</scale></IconStyle><PolyStyle><color>7f00aa00</color></PolyStyle></Style>\r\n"
					+ "<Style id=\"sn_ylw-pushpin\"><PolyStyle><color>7f00aa00</color></PolyStyle></Style>\r\n";

			for (int i = 0; i < taskIdList.size(); i++) {
				if (this.isShuttingDown()) {
					return;
				}
				String polygonStr = null;
				Float minLat;
				Float maxLat;
				Float minLon;
				Float maxLon;
				String isStationary;
				String receivers = null;
				String cc = null;
				int tas_id = taskIdList.get(i);
				this.UpdateTaskStatus(Long.valueOf(tas_id), "PROCESSING");

				try {
					getPointsCs.registerOutParameter(1, OracleTypes.CURSOR);
					getPointsCs.setInt(2, tas_id);
					// getPointsCs.registerOutParameter(3,
					// oracle.jdbc.OracleTypes.CLOB);
					getPointsCs.registerOutParameter(3, java.sql.Types.VARCHAR);
					getPointsCs.registerOutParameter(4, java.sql.Types.VARCHAR);
					getPointsCs.registerOutParameter(5, java.sql.Types.VARCHAR);
					getPointsCs.registerOutParameter(6, java.sql.Types.VARCHAR);
					getPointsCs.registerOutParameter(7, java.sql.Types.FLOAT);
					getPointsCs.registerOutParameter(8, java.sql.Types.FLOAT);
					getPointsCs.registerOutParameter(9, java.sql.Types.FLOAT);
					getPointsCs.registerOutParameter(10, java.sql.Types.FLOAT);
					getPointsCs.execute();
					// polygonClob = (oracle.sql.CLOB) getPointsCs.getClob(2);
					ResultSet result_set = (ResultSet) getPointsCs.getObject(1);
					receivers = getPointsCs.getString(3);
					cc = getPointsCs.getString(4);
					String parameters = getPointsCs.getString(5);
					isStationary = getPointsCs.getString(6);
					minLat = getPointsCs.getFloat(7);
					maxLat = getPointsCs.getFloat(8);
					minLon = getPointsCs.getFloat(9);
					maxLon = getPointsCs.getFloat(10);
					// System.out.println(minLat);
					String l_tmp_box = "<Placemark><styleUrl>#msn_ylw-pushpin</styleUrl><Polygon><outerBoundaryIs><LinearRing><coordinates>"
							+ minLon
							+ ","
							+ maxLat
							+ ",0 "
							+ minLon
							+ ","
							+ minLat
							+ ",0 "
							+ maxLon
							+ ","
							+ minLat
							+ ",0 "
							+ maxLon
							+ ","
							+ maxLat
							+ ",0 "
							+ minLon
							+ ","
							+ maxLat
							+ ",0"
							+ "</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>";
					String l_tmp_folder = "<Folder><name> Stationary </name>";
					polygonStr = l_tmp_box + "\r\n" + l_tmp_folder;

					String FileNamePrefix = "EMAIL_StationaryPoints_";
					String FileNameSuffix = ".kml.gz";

					String attachedFileNames = FileNamePrefix + tas_id + "_"
							+ getStringDate(currentDate()) + FileNameSuffix;
					File stationaryFile = new File(
							Starter.getTempFolder(SERVICE_NAME),
							attachedFileNames);

					if (stationaryFile.exists() == true) {
						stationaryFile.delete();
					}

					stationaryFile.createNewFile();

					GZIPOutputStream zfile = new GZIPOutputStream(
							new FileOutputStream(stationaryFile));
					BufferedWriter BW = new BufferedWriter(
							new OutputStreamWriter(zfile, "UTF8"));

					BW.write(locheader);
					BW.write(polygonStr);

					DiskCache<String> CacheforNotStationary = new DiskCache<String>(
							Starter.getTempFolder(SERVICE_NAME));

					while (result_set.next()) {

						String imo = result_set.getString(1);
						String vessel_name = result_set.getString(2);
						// Long vt_ship_id = result_set.getLong(3);
						float speed = result_set.getFloat(4);
						float latitude = result_set.getFloat(5);
						float longitude = result_set.getFloat(6);
						String recordtime_str = result_set.getString(7);
						// String datasource = result_set.getString(8);
						String l_tmp_loc = "<Placemark><name>"
								+ vessel_name
								+ "</name><ExtendedData><Data name=\"Name\"><value>"
								+ vessel_name
								+ "</value></Data><Data name=\"Record Time\"><value>"
								+ recordtime_str
								+ "</value></Data><Data name=\"IMO\"><value>"
								+ imo
								+ "</value></Data><Data name=\"Speed\"><value>"
								+ speed
								+ "</value></Data></ExtendedData><Point><coordinates>"
								+ longitude + "," + latitude
								+ ",0</coordinates></Point></Placemark>";
						if (speed <= 1) {
							BW.write("\r\n");
							BW.write(l_tmp_loc);
						} else {
							CacheforNotStationary.Append("\r\n" + l_tmp_loc);
						}

					}

					BW.write("\r\n" + "</Folder>");

					if (isStationary.equals("N")) {

						BW.write("\r\n" + "<Folder><name>Speed > 1 </name>");
						CacheforNotStationary.Reset();

						while (CacheforNotStationary.HasNext()) {
							String nextNotStationaryFolder = CacheforNotStationary
									.GetNext();
							BW.write(nextNotStationaryFolder);
						}

						CacheforNotStationary.Delete();
						BW.write("\r\n" + "</Folder>");
					}

					BW.write("</Document></kml>");
					BW.flush();
					BW.close();
					long fileSizeInBytes = stationaryFile.length();
					long fileSizeInMB = fileSizeInBytes / 1024 / 1024;
					String subject = null;
					String sender = "cne.vessels@thomsonreuters.com";
					String zipFileName = FileNamePrefix + tas_id + "_";
					if (fileSizeInMB > 15) {
						subject = "[C&E APEX] Attention: new files generated for [Stationary Points]["
								+ tas_id
								+ "]"
								+ ", as file size is bigger than 15M please contact support to download manually.";
						File cfgFile = emailConfigGenerator.GenEmailCfgFile(
								STATIONARY_POINTS_PREFIX, subject, sender,
								receivers, cc, parameters, null);
						File[] SourceFiles = new File[] { cfgFile };

						emailConfigGenerator.fileToZip(SourceFiles,
								zipFileName, STATIONARY_POINTS_PREFIX);
					} else {
						subject = "[C&E APEX] Attention: new files generated for [Stationary Points]["
								+ tas_id + "]";
						File cfgFile = emailConfigGenerator.GenEmailCfgFile(
								STATIONARY_POINTS_PREFIX, subject, sender,
								receivers, cc, parameters, attachedFileNames);
						File[] SourceFiles = new File[] { stationaryFile,
								cfgFile };
						emailConfigGenerator.fileToZip(SourceFiles,
								zipFileName, STATIONARY_POINTS_PREFIX);
					}

					this.UpdateTaskStatus(Long.valueOf(tas_id), "COMPLETED");

				} catch (Exception e) {
					this.UpdateTaskStatus(Long.valueOf(tas_id), "FAILED");
					logger.error("Task with ID:" + tas_id
							+ " failed because of Exception:", e);
				}

			}
			getPointsCs.close();

		} catch (Exception e) {
			logger.error(STATIONARY_POINTS_PREFIX, e);

		} finally {
			try {

				if (DBConn != null && !DBConn.isClosed()) {
					DBConn.close();
				}
			} catch (SQLException e) {
				logger.error(STATIONARY_POINTS_PREFIX
						+ " Faild to close db connection.");
			}
		}

	}

}
