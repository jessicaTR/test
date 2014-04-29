package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;

import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTaskProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.calculation.AGGCalculator;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.AnalyticTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DataUnit;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DateRegion;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.output.IIRWriter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.output.Writable;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.transformation.Transformable;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.transformation.Transformer;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Comparators;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.ConcurrentDateUtil;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Utility;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.AnalyticType;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.DataType;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.TaskStatus;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.DBConnNames;

public class IIRTSProcessor extends ExportTaskProcessor {

	private Utility utility = null;
	private int taskBatchSize = 100;
	private int batchNum = 0;
	public static String SERVICE_NAME = "IIR_TS";
	private static Logger logger = com.thomsonreuters.ce.exportjobcontroller.Starter
			.getLogger(SERVICE_NAME);

	public IIRTSProcessor() {
		init();
	}

	private void init() {
		Properties pros = new Properties();
		try {
			FileInputStream fs = new FileInputStream(Starter.Config_File);
			pros.load(fs);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (pros.getProperty("taskBatchSize") != null) {
			this.setTaskBatchSize(Integer.parseInt(pros
					.getProperty("taskBatchSize")));
		}
		utility = new Utility();
	}

	/**
	 * Collect commodity data for all the related assets
	 * 
	 * @param mergedDataUnitMeta
	 *            : Sorted by fieds: dataType, permId, startDate
	 * @param minDate
	 * @param maxDate
	 * @return mapDataRepository
	 * @throws SQLException
	 */
	private Map<DataType, ArrayList<DataUnit>> collectData4DataUnits(
			List<DataUnit> mergedDataUnitMeta, Date minDate, Date maxDate) {
		logger.debug(Constants.IIR_EMSG_PREFIX
				+ "collect data for data units ...");

		// ArrayList to hold the unit data
		Map<DataType, ArrayList<DataUnit>> allDVRepository = new HashMap<DataType, ArrayList<DataUnit>>();

		HashSet<Long> rawPermIds = new HashSet<Long>();
		// List<Long> rawPermIds = new ArrayList<Long>();
		DataType preDataType = null;
		List<DataUnit> tempDUM = new ArrayList<DataUnit>();
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);
			for (DataUnit du : mergedDataUnitMeta) {
				if (rawPermIds.size() == 0) {
					rawPermIds.add(du.getPermId());
					preDataType = du.getDataType();
					tempDUM.add(du);
				} else if (preDataType.equals(du.getDataType())) {
					rawPermIds.add(du.getPermId());
					tempDUM.add(du);
				} else {
					ResultSet typedRSet = getDataByType(preDataType.toString(),
							rawPermIds, minDate, maxDate, conn);
					// key:PermId; value:
					Map<Long, List<DataUnit>> dbRepo = createTypedDBDataRepository(
							preDataType, typedRSet);
					typedRSet.close();

					List<DataUnit> typedDataRepo = new ArrayList<DataUnit>();
					if (dbRepo.size() > 0) {
						// collect commodity data for data unit in tempDUM list.
						createTypedUnitDataRepository(typedDataRepo, tempDUM,
								dbRepo);
					}

					allDVRepository.put(preDataType,
							(ArrayList<DataUnit>) typedDataRepo);

					preDataType = du.getDataType();
					typedDataRepo = new ArrayList<DataUnit>();
					rawPermIds = new HashSet<Long>();
					rawPermIds.add(du.getPermId());
					tempDUM = new ArrayList<DataUnit>();
					tempDUM.add(du);
				}
			}
			if (rawPermIds.size() > 0) {
				ResultSet typedRSet = getDataByType(preDataType.toString(),
						rawPermIds, minDate, maxDate, conn);
				// key:PermId; value:
				// TODO: add logic to process the case: no related installData.
				Map<Long, List<DataUnit>> dbRepo = createTypedDBDataRepository(
						preDataType, typedRSet);
				List<DataUnit> typedDataRepo = new ArrayList<DataUnit>();
				if (dbRepo.size() > 0) {
					// collect commodity data for data unit in tempDUM list.
					createTypedUnitDataRepository(typedDataRepo, tempDUM,
							dbRepo);
				}

				allDVRepository.put(preDataType,
						(ArrayList<DataUnit>) typedDataRepo);
			}
		} catch (SQLException se) {
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "Faild to collectData4DataUnits for SQL Exception");
			se.printStackTrace();
		} catch (Exception e) {
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "Faild to collectData4DataUnits for " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(Constants.IIR_EMSG_PREFIX
						+ "Faild to close db connection in collectData4DataUnits");
			}
		}

		return allDVRepository;
	}

	/**
	 * Store dv of specified dataType into a hashmap.
	 * 
	 * @param typedRSet
	 * @return Map<Long, List<DataUnit>>
	 * @throws SQLException
	 */
	private Map<Long, List<DataUnit>> createTypedDBDataRepository(
			DataType dataType, ResultSet typedRSet) {
		Map<Long, List<DataUnit>> dbDataRepo = new HashMap<Long, List<DataUnit>>();
		List<DataUnit> singlePermIdDataCol = new ArrayList<DataUnit>();
		Long prePermId = null;
		try {
			while (typedRSet.next()) {
				DataUnit du = new DataUnit();
				du.setPermId(typedRSet.getLong(1));
				du.setDataType(dataType);
				du.setStartDate(typedRSet.getDate(2));
				du.setEndDate(typedRSet.getDate(3));
				du.setValue(typedRSet.getLong(4));
				if (!du.isValidDV(dataType))
					continue;

				if (singlePermIdDataCol.size() == 0) {
					singlePermIdDataCol.add(du);
					prePermId = du.getPermId();
				} else if (prePermId.compareTo(du.getPermId()) == 0) {
					singlePermIdDataCol.add(du);
				} else {
					dbDataRepo.put(prePermId, singlePermIdDataCol);
					singlePermIdDataCol = new ArrayList<DataUnit>();
					singlePermIdDataCol.add(du);
					prePermId = du.getPermId();
				}
			}

			//Close resultset outside
			//typedRSet.close();
			//typedRSet.getStatement().close();

			if (singlePermIdDataCol.size() > 0)
				dbDataRepo.put(prePermId, singlePermIdDataCol);
		} catch (SQLException se) {
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "Hit SQLException when fetch commodity data.\r\n Message:"
					+ se.getMessage() + " ; Cause:" + se.getCause());
			se.printStackTrace();
		} catch (Exception e) {
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "Unknown Exception when fetch commodity data.\r\n Message:"
					+ e.getMessage() + " ; Cause:" + e.getCause());
			e.printStackTrace();
		}
		return dbDataRepo;
	}

	/**
	 * Create unit data repository, which contain the commodity data of all the
	 * related asset. NOTE: both typedDUM and typedRSet were sorted by permId.
	 * 
	 * @param typedDataRepository
	 *            : output parameter
	 * @param typedDUM
	 *            : list of meta of data units, all the DU have the same data
	 *            type , which currently should be INSTALL_CAPACITY or OUTAGE
	 *            eg: [{permId, startDate(calculate_from),
	 *            endDate(calculate_to)}, {permId, startDate(calculate_from),
	 *            endDate(calculate_to)}]
	 * @param dbRepo
	 *            : permId:[{permId, startDate(install/outage from),
	 *            endDate(install/outage to)}, {permId, startDate(install/outage
	 *            from), endDate(install/outage to)}]
	 */
	private void createTypedUnitDataRepository(
			List<DataUnit> typedDataRepository, List<DataUnit> typedDUM,
			Map<Long, List<DataUnit>> dvRepo) {
		DataType dataType = typedDUM.iterator().next().getDataType();

		Iterator<DataUnit> dumIT = typedDUM.iterator();
		if (Constants.ISDEBUG) {
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "Create data repository for " + dataType.toString());
		}
		while (dumIT.hasNext()) {
			DataUnit dum = dumIT.next(); // contains the cal_from and cal_to.
			List<DataUnit> saDVs = dvRepo.get(dum.getPermId()); // single
																// asset's
																// valued data
																// unit list

			// TODO:
			switch (dataType) {
			case INSTALL_CAPACITY:
				if (saDVs == null) {
					if (Constants.ISDEBUG) {
						logger.info(Constants.IIR_EMSG_PREFIX + dum.getPermId()
								+ " doesn't have commodity data! will skip it.");
					}
					dum.setValidDUM(false);
					continue;
				}

				// Sort DataUnitMeta list by fieds: dataType, permId, startDate
				Collections.sort(saDVs, Comparators.getComparator());
				splitDUWithInstallData(typedDataRepository, dum, saDVs);
				break;
			case OUTAGE:
				if (saDVs == null) {
					dum.setValue(0L);
					typedDataRepository.add(dum);
					continue;
				}

				// Sort DataUnitMeta list by fieds: dataType, permId, startDate
				Collections.sort(saDVs, Comparators.getComparator());
				splitDUWithOutageData(typedDataRepository, dum, saDVs);
				break;
			default:
				logger.info(Constants.IIR_EMSG_PREFIX
						+ " Not defined datatype found: " + dataType.toString());
				break;
			}
		}

	}

	/**
	 * Get analytic tasks via task ids and merge them according to the StartData
	 * and EndData of each entity
	 * 
	 * @return Merged analytic task list
	 * @throws SQLException
	 */
	private List<AnalyticTask> getAnalyticTaskList(List<Integer> lstTaskId,
			int taskDefId) {

		logger.info(Constants.IIR_EMSG_PREFIX
				+ " get analytic tasks and merge them ...");

		utility.updateStaskStatus(lstTaskId, TaskStatus.PROCESSING);

		List<AnalyticTask> lstAnaTask = new ArrayList<AnalyticTask>();
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);
			OracleConnection oraConn = conn.unwrap(OracleConnection.class);
			CallableStatement callStmt = conn
					.prepareCall(Constants.SQL_GET_ANALYTIC_QUEUE);
			callStmt.registerOutParameter(1, OracleTypes.CURSOR);

            Array arrTaskId = oraConn.createOracleArray("CE_ID_LST_T", lstTaskId.toArray());
			callStmt.setArray(2, arrTaskId);
			callStmt.setInt(3, taskDefId);// this.taskMap.entrySet().iterator().next().getValue().getTaskDefId());
			callStmt.execute();

			ResultSet rset = (ResultSet) callStmt.getObject(1);
			while (rset.next()) {
				if (rset.getDate("start_date") == null
						|| rset.getDate("end_date") == null
						|| rset.getDate("start_date").after(
								rset.getDate("end_date"))) {
					if (Constants.ISDEBUG) {
						logger.info(Constants.IIR_EMSG_PREFIX
								+ "Invalid task: rreId:"
								+ rset.getLong("rre_id")
								+ "; startDate:"
								+ ConcurrentDateUtil.formatToyMdDash(rset
										.getDate("start_date"))
								+ "; endDate:"
								+ ConcurrentDateUtil.formatToyMdDash(rset
										.getDate("end_date")));
					}
					continue;
				}

				AnalyticTask anaTask = new AnalyticTask();
				anaTask.setRreId(rset.getLong("rre_id"));
				anaTask.setRic(rset.getString("ric"));
				anaTask.setAnalyticTypeId(rset.getInt("and_id"));
				anaTask.setAnalyticTypeName(AnalyticType.valueOf(rset
						.getString("and_name")));
				anaTask.setStartDate(rset.getDate("start_date"));
				anaTask.setEndDate(rset.getDate("end_date"));
				anaTask.setLogTime(rset.getDate("log_time"));

				lstAnaTask.add(anaTask);

				if (Constants.ISDEBUG) {
					logger.info(Constants.IIR_EMSG_PREFIX + " Analytic task: "
							+ anaTask.toString());
				}
			}

			rset.close();
			callStmt.close();

		} catch (SQLException se) {
			se.printStackTrace();
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "Faild to getAnalyticTaskList");
		} finally {
			try {
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(Constants.IIR_EMSG_PREFIX
						+ "Faild to close db connection in getAnalyticTaskList");
			}
		}

		return lstAnaTask;
	}

	/**
	 * Get the dataset for the specified datatype and permId list
	 * 
	 * @param dataType
	 * @param lstPermId
	 * @return ResultSet
	 * @throws SQLException
	 */
	private ResultSet getDataByType(String dataType, HashSet<Long> lstPermId,
			Date minDate, Date maxDate, Connection conn) throws SQLException {

		CallableStatement callStmt = conn
				.prepareCall(Constants.SQL_COLLECT_COMMODITY_DATA);
		callStmt.registerOutParameter(1, OracleTypes.CURSOR);
		callStmt.setInt(2, 1);
		callStmt.setString(3, dataType);
		
		OracleConnection oraConn = conn.unwrap(OracleConnection.class);
		Array arrPermId = oraConn.createOracleArray("CE_ID_LST_T", lstPermId.toArray());
		callStmt.setArray(4, arrPermId);
		callStmt.setDate(5, new java.sql.Date(minDate.getTime()));
		callStmt.setDate(6, new java.sql.Date(maxDate.getTime()));

		callStmt.execute();
		ResultSet rs = (ResultSet) callStmt.getObject(1);

		return rs;
	}

	/**
	 * Get the list of task ids from the task map.
	 * 
	 * @return Integer list of task ids.
	 */
	private List<Integer> getTaskIDs(ArrayList<ExportTask> tasks) {
		logger.info(Constants.IIR_EMSG_PREFIX + "get task ids...");
		List<Integer> lstTaskId = new ArrayList<Integer>();
		Iterator<ExportTask> it = tasks.iterator();
		while (it.hasNext()) {
			lstTaskId.add(it.next().getId());
		}
		logger.info("Got task ids:"+ lstTaskId);
		return lstTaskId;
	}

	private void addPermIdsToTasks(Set<Long> setRREIds, Integer preAnaTypeId,
			Map<Long, ArrayList<AnalyticTask>> subAnaTasks,
			List<AnalyticTask> delAnaTasks) {
		Connection conn = null;
		try {
			conn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement callStmt = conn
					.prepareCall(Constants.SQL_GET_ANALYTIC_PERMIDS);

			OracleConnection oraConn = conn.unwrap(OracleConnection.class);
			Array paramRREIds = oraConn.createOracleArray("CE_ID_LST_T", setRREIds.toArray());
			callStmt.registerOutParameter(1, OracleTypes.CURSOR);
			callStmt.setArray(2, paramRREIds);
			callStmt.setInt(3, preAnaTypeId);
			callStmt.execute();

			ResultSet rset = (ResultSet) callStmt.getObject(1);
			Map<Long, HashSet<Long>> mapDB = new HashMap<Long, HashSet<Long>>();
			while (rset.next()) {
				Long rreId = rset.getLong("rre_id");
				Long permId = rset.getLong("perm_id");

				if (rreId == null || permId == null) {
					continue;
				}
				if (mapDB.containsKey(rreId)) {
					mapDB.get(rreId).add(permId);
				} else {
					HashSet<Long> permIds = new HashSet<Long>();
					permIds.add(permId);
					mapDB.put(rreId, permIds);
				}
			}

			rset.close();
			callStmt.close();

			if (mapDB.isEmpty()) {
				return;
			}

			Iterator<Entry<Long, ArrayList<AnalyticTask>>> it = subAnaTasks
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Long, ArrayList<AnalyticTask>> entry = it.next();
				Long rreId = entry.getKey();
				ArrayList<AnalyticTask> tasks = entry.getValue();
				if (mapDB.containsKey(rreId)) {
					for (AnalyticTask task : tasks) {
						task.setLstPermId(new ArrayList<Long>(mapDB.get(rreId)));
					}
				} else {
					delAnaTasks.addAll(tasks);
					if (Constants.ISDEBUG) {
						logger.info(Constants.IIR_EMSG_PREFIX
								+ "RREID: "
								+ rreId
								+ " doesn't have any related asset(perm_id), will be ignored! ");
					}
				}
			}
		} catch (SQLException se) {
			se.printStackTrace();
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "Failed to initPermId for SQL exception");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "Failed to initPermId for Unkown exception");
		} finally {
			try {
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}

				setRREIds.clear();
				subAnaTasks.clear();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(Constants.IIR_EMSG_PREFIX
						+ "Faild to close db connection in initPermId");
			}
		}
	}

	/**
	 * Get the permIds for each RIC+AnalyticTypeID group and assign them to
	 * analytic task list.
	 * 
	 * @param lstAnaTask
	 * @throws SQLException
	 */
	private void initPermId(List<AnalyticTask> lstAnaTask) {
		logger.info(Constants.IIR_EMSG_PREFIX
				+ "Get the permIds and assign to tasks...");

		List<AnalyticTask> delAnaTasks = new ArrayList<AnalyticTask>();
		Set<Long> setRREIds = new HashSet<Long>();
		Integer preAnaTypeId = lstAnaTask.iterator().next().getAnalyticTypeId();

		Map<Long, ArrayList<AnalyticTask>> subAnaTasks = new HashMap<Long, ArrayList<AnalyticTask>>();
		for (AnalyticTask anaTask : lstAnaTask) {
			if (anaTask.getAnalyticTypeId().equals(preAnaTypeId)) {
				setRREIds.add(anaTask.getRreId());
				if (subAnaTasks.containsKey(anaTask.getRreId())) {
					subAnaTasks.get(anaTask.getRreId()).add(anaTask);
				} else {
					ArrayList<AnalyticTask> arrTasks = new ArrayList<AnalyticTask>();
					arrTasks.add(anaTask);
					subAnaTasks.put(anaTask.getRreId(), arrTasks);
				}
			} else {
				// setRREIds and subAnaTasks would be cleared after related
				// tasks get their permids
				addPermIdsToTasks(setRREIds, preAnaTypeId, subAnaTasks,
						delAnaTasks);

				preAnaTypeId = anaTask.getAnalyticTypeId();
				setRREIds.add(anaTask.getRreId());
				if (subAnaTasks.containsKey(anaTask.getRreId())) {
					subAnaTasks.get(anaTask.getRreId()).add(anaTask);
				} else {
					ArrayList<AnalyticTask> arrTasks = new ArrayList<AnalyticTask>();
					arrTasks.add(anaTask);
					subAnaTasks.put(anaTask.getRreId(), arrTasks);
				}
			}
		}

		if (setRREIds.size() > 0) {
			addPermIdsToTasks(setRREIds, preAnaTypeId, subAnaTasks, delAnaTasks);
		}

		if (!delAnaTasks.isEmpty()) {
			lstAnaTask.removeAll(delAnaTasks);
		}

	}

	/**
	 * Merge the analytic tasks for each RIC+AnalyticTypeID group according to
	 * the StartDate and EndData of them. As by-products, the minDate and
	 * maxDate hold the max time region to be calculated, and would be used when
	 * collected related commodity data.
	 * 
	 * @param lstAnaTask
	 *            : has been sorted by the order rreId, analyticTypeId,
	 *            startDate : task list to be merged
	 * @param minDate
	 * @param maxDate
	 * @return merged task list.
	 */
	private List<AnalyticTask> mergeAnalyticTasks(
			List<AnalyticTask> lstAnaTask, Date minDate, Date maxDate) {
		logger.info(Constants.IIR_EMSG_PREFIX + "merge analytic tasks ...");

		List<AnalyticTask> mergedLstAnaTask = new ArrayList<AnalyticTask>(); // 1
		AnalyticTask preTask = new AnalyticTask();

		minDate.setTime(lstAnaTask.iterator().next().getStartDate().getTime());
		maxDate.setTime(lstAnaTask.iterator().next().getEndDate().getTime());
		try {
			for (AnalyticTask curTask : lstAnaTask) {
				if (minDate.after(curTask.getStartDate()))
					minDate.setTime(curTask.getStartDate().getTime());
				if (maxDate.before(curTask.getEndDate()))
					maxDate.setTime(curTask.getEndDate().getTime());
				// TODO: unit test
				if (mergedLstAnaTask.size() == 0) {
					AnalyticTask newTask = new AnalyticTask(curTask);
					mergedLstAnaTask.add(newTask);
					preTask = newTask;
				} else if (preTask.getRreId().equals(curTask.getRreId())
						&& preTask.getAnalyticTypeId().equals(
								curTask.getAnalyticTypeId())) {
					if (preTask.getEndDate().before(curTask.getStartDate())) {
						AnalyticTask newTask = new AnalyticTask(curTask);
						mergedLstAnaTask.add(newTask);
						preTask = newTask;
					} else if (preTask.getEndDate()
							.before(curTask.getEndDate())) {
						preTask.setEndDate(curTask.getEndDate());
					}
				} else {
					AnalyticTask newTask = new AnalyticTask(curTask);
					mergedLstAnaTask.add(newTask);
					preTask = newTask;
				}
			}
		} catch (Exception e) {
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "IIRTSExport: Failed to merge Analytic Tasks");
			e.printStackTrace();
		}

		return mergedLstAnaTask;

	}

	@Override
	public void processTask(ArrayList<ExportTask> allTasks) {

		if (allTasks == null) {
			logger.error(Constants.IIR_EMSG_PREFIX
					+ "Got null input for taskMap!");
			throw new IllegalArgumentException("taskMap sould not be null!");
		}

		logger.info(Constants.IIR_EMSG_PREFIX + " Total tasks number: "
				+ allTasks.size());

		ArrayList<ExportTask> subTasks = new ArrayList<ExportTask>();
		int count = 0;

		logger.info(Constants.IIR_EMSG_PREFIX + " Batchsize is :"
				+ this.getTaskBatchSize());

		for (ExportTask task : allTasks) {
			subTasks.add(task);
			if (++count == this.getTaskBatchSize()) {
				process(subTasks);
				subTasks = new ArrayList<ExportTask>();
				count = 0;
			}

		}
		if (subTasks.size() > 0)
			process(subTasks);

		logger.info(Constants.IIR_EMSG_PREFIX
				+ " DONE! All current tasks have been processed!!!");
	}

	private void process(ArrayList<ExportTask> tasks) {
		logger.info(Constants.IIR_EMSG_PREFIX + " Batch " + ++this.batchNum
				+ " has been started!");

		List<Integer> lstTaskId = null;
		/**
		 * step 1. get analytic list and merge them.
		 */
		try {
			long start = System.currentTimeMillis();

			lstTaskId = getTaskIDs(tasks);
			int taskDefId = tasks.get(0).getTaskDefId();

			// invoke package to get analytic queue and merge them according to
			// startdate and enddate;

			List<AnalyticTask> lstAnaTask = getAnalyticTaskList(lstTaskId,
					taskDefId);

			// // For Test, will remove in the future
			// List<AnalyticTask> lstAnaTask = new ArrayList<AnalyticTask>();
			// SimpleDateFormat ff = new SimpleDateFormat("yyyy-MM-dd");
			//
			// AnalyticTask task1 = new AnalyticTask();
			// task1.setRreId(27824L);
			// task1.setRic("CARG:7JAD=IIRS");
			// task1.setAnalyticTypeId(2);
			// task1.setAnalyticTypeName(AnalyticType.IIR_AGG_REAL_CAPACITY);
			// task1.setStartDate(ff.parse("2013-10-06"));
			// task1.setEndDate(ff.parse("2013-11-14"));
			// lstAnaTask.add(task1);
			//
			// AnalyticTask task2 = new AnalyticTask();
			// task2.setRreId(27824L);
			// task2.setRic("CARG:7JAD=IIRS");
			// task2.setAnalyticTypeId(2);
			// task2.setAnalyticTypeName(AnalyticType.IIR_AGG_REAL_CAPACITY);
			// task2.setStartDate(ff.parse("2014-03-01"));
			// task2.setEndDate(ff.parse("2014-04-29"));
			// lstAnaTask.add(task2);
			//
			// AnalyticTask task3 = new AnalyticTask();
			// task3.setRreId(27824L);
			// task3.setRic("CARG:7JAD=IIRS");
			// task3.setAnalyticTypeId(3);
			// task3.setAnalyticTypeName(AnalyticType.IIR_AGG_INSTALLED_CAPACITY);
			// task3.setStartDate(ff.parse("2013-10-06"));
			// task3.setEndDate(ff.parse("2013-11-14"));
			// lstAnaTask.add(task3);
			//
			// AnalyticTask task4 = new AnalyticTask();
			// task4.setRreId(27824L);
			// task4.setRic("CARG:7JAD=IIRS");
			// task4.setAnalyticTypeId(3);
			// task4.setAnalyticTypeName(AnalyticType.IIR_AGG_INSTALLED_CAPACITY);
			// task4.setStartDate(ff.parse("2014-03-01"));
			// task4.setEndDate(ff.parse("2014-04-29"));
			// lstAnaTask.add(task4);

			if (lstAnaTask.isEmpty()) {
				logger.warn(Constants.IIR_EMSG_PREFIX
						+ "AnalyticTask was not found with provided tasks!");
				utility.updateStaskStatus(lstTaskId, TaskStatus.COMPLETED);
				return;
			}

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Step 1: get analytic list, sort them and merge them,\r\n "
						+ " cost: " + (System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}

			// sort analytic task list by the order rreId, analyticTypeId,
			// startDate
			Collections.sort(lstAnaTask, Comparators.getComparator());

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Analytic Tasks have been sorted by rreId, analyticTypeId, startDate: as below:");
				for (AnalyticTask task : lstAnaTask) {
					logger.info(Constants.IIR_EMSG_PREFIX + "AnalyticTask: "
							+ task.toString());
				}
			}

			Date minDate = new Date(System.currentTimeMillis());
			Date maxDate = new Date(System.currentTimeMillis());

			// merge analytic tasks by the time periods of them, according the
			// RIC and AnalyticTypeID
			List<AnalyticTask> mergedAnalyticTasks = mergeAnalyticTasks(
					lstAnaTask, minDate, maxDate);

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Analytic Tasks have been Merged by the time periods of them,"
						+ " according the RIC and AnalyticTypeID, as below:");
				for (AnalyticTask task : mergedAnalyticTasks) {
					logger.info(Constants.IIR_EMSG_PREFIX + "AnalyticTask: "
							+ task.toString());
				}
			}

			// get perm_id list by analyticTaskType
			Collections.sort(mergedAnalyticTasks,
					Comparators.getAnaTaskComparatorByType());
			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "merged Analytic Tasks have been sorted by taskTypeId, iir, startTime: ");
				for (AnalyticTask task : mergedAnalyticTasks) {
					logger.info(Constants.IIR_EMSG_PREFIX + "AnalyticTask: "
							+ task.toString());
				}
			}

			initPermId(mergedAnalyticTasks);

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Step 2: get permIds for analytic tasks,\r\n "
						+ " cost: " + (System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Merged analytic tasks with PermIds:");
				for (AnalyticTask task : mergedAnalyticTasks) {
					logger.info(Constants.IIR_EMSG_PREFIX + task.toString());
				}
			}

			/**
			 * 3. Analyze task list and collect data lstDataUnitMeta contains
			 * the dataset as below: permId, datatype,
			 * startdate(calculate_from), enddate(calculate_to) it should be
			 * splited by the date region with commodity data in db
			 */
			Transformable trans = new Transformer(mergedAnalyticTasks);
			List<DataUnit> mergedDataUnitMeta = trans.transform();

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Step 3: transform analytic tasks to dataunit metadata,\r\n "
						+ " cost: " + (System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}

			if (Constants.ISDEBUG) {
				logger.info("mergedDataUnitMeta : ");
				for (DataUnit du : mergedDataUnitMeta) {
					logger.info(du.toString());
				}
			}

			/**
			 * NOTE:DateRegion: INSTALL_CAPACITY: [startDate, endDate); OUTAGE:
			 * [startDate, endDate]
			 */
			Map<DataType, ArrayList<DataUnit>> allDVRepository = collectData4DataUnits(
					mergedDataUnitMeta, minDate, maxDate);

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Step 4: collect commodity data for all the related assets,\r\n "
						+ " cost: " + (System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}

			if (Constants.ISDEBUG) {
				Iterator<Entry<DataType, ArrayList<DataUnit>>> it = allDVRepository
						.entrySet().iterator();
				while (it.hasNext()) {
					Entry<DataType, ArrayList<DataUnit>> entry = it.next();
					logger.info("DataType : " + entry.getKey());
					Iterator<DataUnit> it2 = entry.getValue().iterator();
					while (it2.hasNext()) {
						logger.info(it2.next().toString());
					}
				}
			}

			/**
			 * 4. Calculate
			 */
			if (allDVRepository.size() == 0) {
				utility.updateStaskStatus(lstTaskId, TaskStatus.FAILED);

				logger.error(Constants.IIR_EMSG_PREFIX
						+ " No commodity data was found for all the tasks, will mark them as FAILED!!!");
				throw new LogicException(
						"No commodity data was found for all these tasks!");
			}

			AGGCalculator aggCal = new AGGCalculator();
			aggCal.setAnalyticTasks(mergedAnalyticTasks);
			aggCal.setAllDVRepository(allDVRepository);

			aggCal.calculate();

			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX
						+ "Step 5: Caculate...,\r\n " + " cost: "
						+ (System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}

			/**
			 * 5. Output
			 */
			Map<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> outputContents = aggCal
					.getAggResults();

			if (Constants.ISDEBUG) {
				Iterator<Entry<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>>> it1 = outputContents
						.entrySet().iterator();
				while (it1.hasNext()) {
					Entry<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> entry1 = it1
							.next();
					logger.info("RRE ID: " + entry1.getKey().getRreId()
							+ "; RIC: " + entry1.getKey().getRic());
					for (Long permId : entry1.getKey().getLstPermId()) {
						logger.info(permId.toString());
					}
					Iterator<Entry<DataType, TreeMap<DateRegion, Long>>> it2 = entry1
							.getValue().entrySet().iterator();
					while (it2.hasNext()) {
						Entry<DataType, TreeMap<DateRegion, Long>> entry2 = it2
								.next();
						logger.info("-DataType: " + entry2.getKey().toString());
						Iterator<Entry<DateRegion, Long>> it3 = entry2
								.getValue().entrySet().iterator();
						while (it3.hasNext()) {
							Entry<DateRegion, Long> entry3 = it3.next();
							logger.info("--DateRegion: "
									+ entry3.getKey().toString() + ":"
									+ entry3.getValue());
						}
					}
				}
			}

			try {
				if (outputContents.size() > 0) {
					Writable writer = new IIRWriter(outputContents);
					writer.write();

					if (Constants.ISDEBUG) {
						logger.info(Constants.IIR_EMSG_PREFIX
								+ "Step 6: Write files ...,\r\n " + " cost: "
								+ (System.currentTimeMillis() - start));
						start = System.currentTimeMillis();
					}
				}
			} catch (IOException ioe) {
				utility.updateStaskStatus(lstTaskId, TaskStatus.FAILED);

				logger.warn(Constants.IIR_EMSG_PREFIX
						+ " Failed to generate IIR_TS Files for IO exception! Will mark related task to be FAILED! \r\n Error message: "
						+ ioe.getMessage() + ";\r\n Stack trace: "
						+ ioe.getStackTrace());
			} catch (SQLException se) {
				utility.updateStaskStatus(lstTaskId, TaskStatus.FAILED);

				logger.warn(Constants.IIR_EMSG_PREFIX
						+ " Failed to build filename for SQLException! Will mark related task to be FAILED! \r\n Error message: "
						+ se.getMessage() + ";\r\n Stack trace: "
						+ se.getStackTrace());
			} catch (Exception e) {
				utility.updateStaskStatus(lstTaskId, TaskStatus.FAILED);

				logger.warn(Constants.IIR_EMSG_PREFIX
						+ "Failed to generate IIR_TS files for unkown exception!  Will mark related task to be FAILED! \r\n Error message: "
						+ e.getMessage() + ";\r\n Stack trace: "
						+ e.getStackTrace());

			}

			if (!Constants.ISTEST)
				utility.updateStaskStatus(lstTaskId, TaskStatus.COMPLETED);

			logger.info(Constants.IIR_EMSG_PREFIX + " Batch " + this.batchNum
					+ " has been finished!");

		} catch (Exception ex) {
			utility.updateStaskStatus(lstTaskId, TaskStatus.FAILED);

			logger.warn(Constants.IIR_EMSG_PREFIX
					+ "Failed to finish processing current batch of tasks and will mark them as FAILED!!! \r\n Related taskIds are: "
					+ lstTaskId.toString() + ";\r\n Error message: "
					+ ex.getMessage() + ";\r\n Stack trace: "
					+ ex.getStackTrace());

		}
	}

	/**
	 * Split the specified calculation date region(dum:from->to) date region to
	 * atomic unit according the date region list in lstDV
	 * 
	 * @param dataRepository
	 *            : contains [{permId, dataType, startDate,
	 *            endDate(atomic_region), value}]
	 * 
	 * @param dum
	 *            : contains cal_from and cal_to. : current {permId, dataType,
	 *            startDate(calculate_from), endDate(calculate_to)}
	 * @param singleAssetDVs
	 *            : all the valued DU have the same permId and have sorted by
	 *            startDate. : [{permId, dataType, startDate(install/outage
	 *            from), endDate(install/outage to), value}]
	 */
	private void splitDUWithInstallData(List<DataUnit> dataRepository,
			DataUnit dum, List<DataUnit> singleAssetDVs) {
		if (Constants.ISDEBUG) {
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "Split dataunit for dataunit:" + dum.toString());
		}
		for (DataUnit dv : singleAssetDVs) {
			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX + dv.toString());
			}
			if (dv.getStartDate().before(dum.getStartDate())) {
				if (dv.getEndDate() != null
						&& (dv.getEndDate().before(dum.getStartDate()) || dv
								.getEndDate().equals(dum.getStartDate()))) {
					continue;
				} else if (dv.getEndDate() != null
						&& (dv.getEndDate().before(dum.getEndDate()) || dv
								.getEndDate().equals(dum.getEndDate()))) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(utility.addDays(dv.getEndDate(), -1));
					du.setValue(dv.getValue());
					dataRepository.add(du);

					dum.setStartDate(dv.getEndDate());
					continue;
				} else if (dv.getEndDate() == null
						|| dv.getEndDate().after(dum.getEndDate())) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(dum.getEndDate());
					du.setValue(dv.getValue());
					dataRepository.add(du);
					break;
				}
			} else if (dv.getStartDate().equals(dum.getStartDate())) {
				if (dv.getEndDate() != null
						&& (dv.getEndDate().before(dum.getEndDate()) || dv
								.getEndDate().equals(dum.getEndDate()))) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(utility.addDays(dv.getEndDate(), -1));
					du.setValue(dv.getValue());
					dataRepository.add(du);

					dum.setStartDate(dv.getEndDate());
					continue;
				} else if (dv.getEndDate() == null
						|| dv.getEndDate().after(dum.getEndDate())) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(dum.getEndDate());
					du.setValue(dv.getValue());
					dataRepository.add(du);
					break;
				}
			} else if (dv.getStartDate().after(dum.getStartDate())) {
				if (dv.getEndDate() != null
						&& (dv.getEndDate().before(dum.getEndDate()) || dv
								.getEndDate().equals(dum.getEndDate()))) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dv.getStartDate());
					du.setEndDate(utility.addDays(dv.getEndDate(), -1));
					du.setValue(dv.getValue());
					dataRepository.add(du);

					dum.setStartDate(dv.getEndDate());
					continue;
				} else if (dv.getEndDate() == null
						|| dv.getEndDate().after(dum.getEndDate())) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dv.getStartDate());
					du.setEndDate(dum.getEndDate());
					du.setValue(dv.getValue());
					dataRepository.add(du);
					break;
				}
			} else
				break;
		}
	}

	/**
	 * Split the specified calculation date region(dum:from->to) date region to
	 * atomic unit according the date region list in lstDV
	 * 
	 * @param dataRepository
	 *            : contains [{permId, dataType, startDate, endDate(atomic
	 *            region), value}]
	 * @param dum
	 *            : current {permId, dataType, startDate(calculate_from),
	 *            endDate(calculate_to)}
	 * @param lstDV
	 *            : [{permId, dataType, startDate(install/outage from),
	 *            endDate(install/outage to), value}]
	 */
	private void splitDUWithOutageData(List<DataUnit> dataRepository,
			DataUnit dum, final List<DataUnit> lstDV) {
		if (Constants.ISDEBUG) {
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "Split dataunit for dataunit:" + dum.toString());
		}
		for (DataUnit dv : lstDV) {
			if (Constants.ISDEBUG) {
				logger.info(Constants.IIR_EMSG_PREFIX + dv.toString());
			}
			if (dv.getStartDate().before(dum.getStartDate())) {
				if (dv.getEndDate().before(dum.getStartDate())) {
					continue;
				} else if (dv.getEndDate().before(dum.getEndDate())
						|| dv.getEndDate().equals(dum.getStartDate())) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(dv.getEndDate());
					du.setValue(dv.getValue());
					dataRepository.add(du);

					dum.setStartDate(utility.addDays(dv.getEndDate(), 1));
					continue;
				} else if (dv.getEndDate().equals(dum.getEndDate())
						|| dv.getEndDate().after(dum.getEndDate())) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(dum.getEndDate());
					du.setValue(dv.getValue());
					dataRepository.add(du);
					break;
				}
			} else if (dv.getStartDate().equals(dum.getStartDate())) {
				if (dv.getEndDate().before(dum.getEndDate())) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(dv.getEndDate());
					du.setValue(dv.getValue());
					dataRepository.add(du);

					dum.setStartDate(utility.addDays(dv.getEndDate(), 1));
					continue;
				} else if (dv.getEndDate().equals(dum.getEndDate())
						|| dv.getEndDate().after(dum.getEndDate())) {
					DataUnit du = new DataUnit();
					du.setPermId(dum.getPermId());
					du.setDataType(dv.getDataType());
					du.setStartDate(dum.getStartDate());
					du.setEndDate(dum.getEndDate());
					du.setValue(dv.getValue());
					dataRepository.add(du);
					break;
				}
			} else if (dv.getStartDate().after(dum.getStartDate())) {
				if (dv.getStartDate().after(dum.getEndDate())) {
					dum.setValue(0L);
					break;
				} else if (dv.getEndDate().before(dum.getEndDate())) {
					DataUnit du1 = new DataUnit();
					du1.setPermId(dum.getPermId());
					du1.setDataType(dv.getDataType());
					du1.setStartDate(dum.getStartDate());
					du1.setEndDate(utility.addDays(dv.getStartDate(), -1));
					du1.setValue(0L);
					dataRepository.add(du1);

					DataUnit du2 = new DataUnit();
					du2.setPermId(dum.getPermId());
					du2.setDataType(dv.getDataType());
					du2.setStartDate(dv.getStartDate());
					du2.setEndDate(dv.getEndDate());
					du2.setValue(dv.getValue());
					dataRepository.add(du2);

					dum.setStartDate(utility.addDays(dv.getEndDate(), 1));
					continue;
				} else if (dv.getEndDate().equals(dum.getEndDate())
						|| dv.getEndDate().after(dum.getEndDate())) {
					DataUnit du1 = new DataUnit();
					du1.setPermId(dum.getPermId());
					du1.setDataType(dv.getDataType());
					du1.setStartDate(dum.getStartDate());
					du1.setEndDate(utility.addDays(dv.getStartDate(), -1));
					du1.setValue(0L);
					dataRepository.add(du1);

					DataUnit du2 = new DataUnit();
					du2.setPermId(dum.getPermId());
					du2.setDataType(dv.getDataType());
					du2.setStartDate(dv.getStartDate());
					du2.setEndDate(dum.getEndDate());
					du2.setValue(dv.getValue());
					dataRepository.add(du2);
					break;
				}
			} else
				break;
		}
	}

	/**
	 * @return the taskBatchSize
	 */
	public int getTaskBatchSize() {
		return taskBatchSize;
	}

	/**
	 * @param taskBatchSize
	 *            the taskBatchSize to set
	 */
	public void setTaskBatchSize(int taskBatchSize) {
		this.taskBatchSize = taskBatchSize;
	}
}
