/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.calculation;

import java.util.Collections;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.IIRTSProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.AnalyticTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DataUnit;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DateRegion;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Comparators;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Utility;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.AnalyticType;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.DataType;

/**
 * @author lei.yang
 * 
 */
public class AGGCalculator extends Calculator implements Calculatable {

	private HashMap<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> aggResults = new HashMap<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>>();
	private Utility utility = new Utility();
	private static Logger logger = Starter.getLogger(IIRTSProcessor.SERVICE_NAME);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.calculation
	 * .Calculatable#calculate()
	 */
	@Override
	public void calculate() {
		logger.info(Constants.IIR_EMSG_PREFIX + "Calculate ...");
		try {
			shuffleTasks();
		} catch (Exception ex) {
			logger.error(Constants.IIR_EMSG_PREFIX + "Calculation failed for "
					+ ex.getMessage());
		}
	}

	private void shuffleTasks() throws Exception {
		final String installDataIsNull = Constants.IIR_EMSG_PREFIX
				+ "All the install data is null when caculating ";
		final String outageDataIsNull = Constants.IIR_EMSG_PREFIX
				+ "All the install data is null when caculating ";

		List<DataUnit> installDataUnits = this.allDVRepository
				.get(DataType.INSTALL_CAPACITY);
		List<DataUnit> outageDataUnits = this.allDVRepository
				.get(DataType.OUTAGE);

		if (installDataUnits != null) {
			Collections.sort(installDataUnits, Comparators.getComparator());
		} else {
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "InstallDataUnits is null!! ");
		}

		if (outageDataUnits != null) {
			Collections.sort(outageDataUnits, Comparators.getComparator());
		} else {
			logger.info(Constants.IIR_EMSG_PREFIX
					+ "OutageDataUnits is null!! ");
		}

		for (AnalyticTask task : this.analyticTasks) {
			Date calFrom = task.getStartDate();
			Date calTo = task.getEndDate();

			List<Long> permIds = task.getLstPermId();
			if (Constants.ISDEBUG) {
				logger.info("Caculate task: " + task.getRreId());
				for (Long permId : permIds) {
					logger.info("permId:" + permId);
				}
			}

			if (task.getAnalyticTypeName().equals(
					AnalyticType.IIR_REAL_CAPACITY)) {
				if (installDataUnits == null)
					throw new NullPointerException(installDataIsNull
							+ " IIR_REAL_CAPACITY");
				if (outageDataUnits == null)
					throw new NullPointerException(outageDataIsNull
							+ " IIR_REAL_CAPACITY");

				TreeSet<DateRegion> mixedDateRegions = new TreeSet<DateRegion>();
				List<DataUnit> rawCalInstallDUs = new ArrayList<DataUnit>();
				List<DataUnit> rawCalOutageDUs = new ArrayList<DataUnit>();
				Long permId = permIds.get(0);

				for (DataUnit du : installDataUnits) {
					if (permId.equals(du.getPermId())) {
						DataUnit newUnit = new DataUnit(du);

						if (du.getEndDate().before(calFrom)
								|| du.getStartDate().after(calTo)) {
							continue;
						} else if (du.getEndDate().equals(calFrom)) {
							newUnit.setStartDate(calFrom);
						} else if (du.getStartDate().before(calFrom)) {
							newUnit.setStartDate(calFrom);

							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						} else if (du.getStartDate().equals(calFrom)
								|| du.getStartDate().after(calFrom)) {
							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						}

						rawCalInstallDUs.add(newUnit);
						mixedDateRegions.add(new DateRegion(newUnit
								.getStartDate(), newUnit.getEndDate()));
					}
				}

				for (DataUnit du : outageDataUnits) {
					if (permId.equals(du.getPermId())) {
						DataUnit newUnit = new DataUnit(du);

						if (du.getEndDate().before(calFrom)
								|| du.getStartDate().after(calTo)) {
							continue;
						} else if (du.getEndDate().equals(calFrom)) {
							newUnit.setStartDate(calFrom);
						} else if (du.getStartDate().before(calFrom)) {
							newUnit.setStartDate(calFrom);

							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						} else if (du.getStartDate().equals(calFrom)
								|| du.getStartDate().after(calFrom)) {
							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						}
						rawCalOutageDUs.add(newUnit);
						mixedDateRegions.add(new DateRegion(newUnit
								.getStartDate(), newUnit.getEndDate()));
					}
				}

				TreeSet<DateRegion> atomicRegions = new TreeSet<DateRegion>();
				shuffleDateRegions(mixedDateRegions, atomicRegions);

				if (rawCalInstallDUs.isEmpty()) {
					continue;
				}

				HashMap<DataType, TreeMap<DateRegion, Long>> mapResult = new HashMap<DataType, TreeMap<DateRegion, Long>>();

				TreeMap<DateRegion, Long> installAotiRegionValues = shuffleAGGDataUnits(
						rawCalInstallDUs, atomicRegions);
				mapResult.put(DataType.INSTALL_CAPACITY,
						installAotiRegionValues);

				TreeMap<DateRegion, Long> outageAotiRegionValues = shuffleAGGDataUnits(
						rawCalOutageDUs, atomicRegions);
				mapResult.put(DataType.OUTAGE, outageAotiRegionValues);

				aggResults.put(task, mapResult);
			} else if (task.getAnalyticTypeName().equals(
					AnalyticType.IIR_AGG_REAL_CAPACITY)) {
				// should mix the install region and outage region and crate the
				// atomic regions of mixed collection.
				if (installDataUnits == null)
					throw new NullPointerException(installDataIsNull
							+ " IIR_AGG_REAL_CAPACITY");
				if (outageDataUnits == null)
					throw new NullPointerException(outageDataIsNull
							+ " IIR_AGG_REAL_CAPACITY");

				TreeSet<DateRegion> mixedDateRegions = new TreeSet<DateRegion>();
				List<DataUnit> rawCalInstallDUs = new ArrayList<DataUnit>();
				List<DataUnit> rawCalOutageDUs = new ArrayList<DataUnit>();
				for (DataUnit du : installDataUnits) {
					if (permIds.contains(du.getPermId())) {
						DataUnit newUnit = new DataUnit(du);

						if (du.getEndDate().before(calFrom)
								|| du.getStartDate().after(calTo)) {
							continue;
						} else if (du.getEndDate().equals(calFrom)) {
							newUnit.setStartDate(calFrom);
						} else if (du.getStartDate().before(calFrom)) {
							newUnit.setStartDate(calFrom);

							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						} else if (du.getStartDate().equals(calFrom)
								|| du.getStartDate().after(calFrom)) {
							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						}
						rawCalInstallDUs.add(newUnit);
						mixedDateRegions.add(new DateRegion(newUnit
								.getStartDate(), newUnit.getEndDate()));
					}
				}

				for (DataUnit du : outageDataUnits) {
					if (permIds.contains(du.getPermId())) {
						DataUnit newUnit = new DataUnit(du);

						if (du.getEndDate().before(calFrom)
								|| du.getStartDate().after(calTo)) {
							continue;
						} else if (du.getEndDate().equals(calFrom)) {
							newUnit.setStartDate(calFrom);
						} else if (du.getStartDate().before(calFrom)) {
							newUnit.setStartDate(calFrom);

							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						} else if (du.getStartDate().equals(calFrom)
								|| du.getStartDate().after(calFrom)) {
							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						}

						rawCalOutageDUs.add(newUnit);
						mixedDateRegions.add(new DateRegion(newUnit
								.getStartDate(), newUnit.getEndDate()));
					}
				}

				TreeSet<DateRegion> atomicRegions = new TreeSet<DateRegion>();
				shuffleDateRegions(mixedDateRegions, atomicRegions);

				HashMap<DataType, TreeMap<DateRegion, Long>> mapResult = new HashMap<DataType, TreeMap<DateRegion, Long>>();

				TreeMap<DateRegion, Long> installAotiRegionValues = shuffleAGGDataUnits(
						rawCalInstallDUs, atomicRegions);
				mapResult.put(DataType.INSTALL_CAPACITY,
						installAotiRegionValues);

				TreeMap<DateRegion, Long> outageAotiRegionValues = shuffleAGGDataUnits(
						rawCalOutageDUs, atomicRegions);
				mapResult.put(DataType.OUTAGE, outageAotiRegionValues);

				aggResults.put(task, mapResult);
			} else if (task.getAnalyticTypeName().equals(
					AnalyticType.IIR_AGG_INSTALLED_CAPACITY)) {
				// should mix the install region and outage region and crate the
				// atomic regions of mixed collection.
				if (installDataUnits == null)
					throw new NullPointerException(installDataIsNull
							+ " IIR_AGG_INSTALLED_CAPACITY");

				TreeSet<DateRegion> installDateRegions = new TreeSet<DateRegion>();
				List<DataUnit> rawCalInstallDUs = new ArrayList<DataUnit>();
				for (DataUnit du : installDataUnits) {
					if (permIds.contains(du.getPermId())) {
						DataUnit newUnit = new DataUnit(du);

						if (du.getEndDate().before(calFrom)
								|| du.getStartDate().after(calTo)) {
							continue;
						} else if (du.getEndDate().equals(calFrom)) {
							newUnit.setStartDate(calFrom);
						} else if (du.getStartDate().before(calFrom)) {
							newUnit.setStartDate(calFrom);

							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						} else if (du.getStartDate().equals(calFrom)
								|| du.getStartDate().after(calFrom)) {
							if (du.getEndDate().after(calTo)) {
								newUnit.setEndDate(calTo);
							}
						}

						rawCalInstallDUs.add(newUnit);
						installDateRegions.add(new DateRegion(newUnit
								.getStartDate(), newUnit.getEndDate()));
					}
				}

				TreeSet<DateRegion> atomicRegions = new TreeSet<DateRegion>();
				shuffleDateRegions(installDateRegions, atomicRegions);

				HashMap<DataType, TreeMap<DateRegion, Long>> mapResult = new HashMap<DataType, TreeMap<DateRegion, Long>>();

				TreeMap<DateRegion, Long> installAotiRegionValues = shuffleAGGDataUnits(
						rawCalInstallDUs, atomicRegions);
				mapResult.put(DataType.INSTALL_CAPACITY,
						installAotiRegionValues);

				aggResults.put(task, mapResult);
			}
		}
	}

	public void shuffleDateRegions(TreeSet<DateRegion> regions,
			TreeSet<DateRegion> atomicRegions) {
		while (regions.size() > 0) {
			if (regions.size() == 1) {
				atomicRegions.add(regions.pollFirst());
				break;
			}
			createAtomicDateRegions(regions, atomicRegions);
		}
	}

	private void createAtomicDateRegions(TreeSet<DateRegion> regions,
			TreeSet<DateRegion> atomicRegions) {

		if (Constants.ISTEST) {
			System.out.println("shuttle:");
			for (DateRegion region : regions) {
				System.out.println(region.toString());
			}
		}

		DateRegion first = regions.pollFirst();
		DateRegion second = regions.pollFirst();

		while (first.getEndDate().equals(
				utility.addDays(second.getStartDate(), -1))) {
			atomicRegions.add(first);
			if (regions.size() == 0) {
				atomicRegions.add(second);
				return;
			}
			first = second;
			second = regions.pollFirst();
		}

		/**
		 * |-----------| preRegion |--------------| (1) |------| (2) |--------|
		 * (3) |-------------| (4) |------| (5) skip |---------| (6) skip
		 */

		if (first.getStartDate().equals(second.getStartDate())) {
			// (1)
			DateRegion dr1 = new DateRegion(first.getStartDate(),
					first.getEndDate());
			DateRegion dr2 = new DateRegion(utility.addDays(first.getEndDate(),
					1), second.getEndDate());
			regions.add(dr1);
			regions.add(dr2);
			return;
		} else if (first.getStartDate().before(second.getStartDate())) {
			if (first.getEndDate().after(second.getEndDate())) {
				// (2)
				DateRegion dr1 = new DateRegion(first.getStartDate(),
						utility.addDays(second.getStartDate(), -1));
				DateRegion dr2 = new DateRegion(second.getStartDate(),
						second.getEndDate());
				DateRegion dr3 = new DateRegion(utility.addDays(
						second.getEndDate(), 1), first.getEndDate());
				regions.add(dr1);
				regions.add(dr2);
				regions.add(dr3);
				return;
			} else if (first.getEndDate().equals(second.getEndDate())) {
				// (3)
				DateRegion dr1 = new DateRegion(first.getStartDate(),
						utility.addDays(second.getStartDate(), -1));
				DateRegion dr2 = new DateRegion(second.getStartDate(),
						second.getEndDate());
				regions.add(dr1);
				regions.add(dr2);
				return;
			} else if (first.getEndDate().before(second.getEndDate())) {
				// (4)
				DateRegion dr1 = new DateRegion(first.getStartDate(),
						utility.addDays(second.getStartDate(), -1));
				DateRegion dr2 = new DateRegion(second.getStartDate(),
						first.getEndDate());
				DateRegion dr3 = new DateRegion(utility.addDays(
						first.getEndDate(), 1), second.getEndDate());
				regions.add(dr1);
				regions.add(dr2);
				regions.add(dr3);
				return;
			}
		}
	}

	/**
	 * Calculate the sum of commodity value for the specified task.
	 * 
	 * @param dataUnits
	 *            : [{permId, startDate(calculate_from), endDate(calculate_to),
	 *            value1}, {permId, startDate(calculate_from),
	 *            endDate(calculate_to), value2},...]
	 * @param atomicRegions
	 *            [{DateRegion1(date1, date2)},{DateRegion2(date2, date3)},...]
	 * @return: used for output [{DateRegion1, sumValue},{DateRegion2,
	 *          sumValue},...]
	 */
	private TreeMap<DateRegion, Long> shuffleAGGDataUnits(
			List<DataUnit> dataUnits, TreeSet<DateRegion> atomicRegions) {
		TreeMap<DateRegion, Long> dataValues = new TreeMap<DateRegion, Long>();
		for (DataUnit du : dataUnits) {
			for (DateRegion region : atomicRegions) {
				if (du.getStartDate().equals(region.getStartDate())) {
					if (du.getEndDate().equals(region.getEndDate())) {
						addCalUnit(dataValues, region, du.getValue());
					} else {
						addCalUnit(dataValues, region, du.getValue());
						du.setStartDate(utility.addDays(region.getEndDate(), 1));
					}
				}
			}
		}

		return dataValues;
	}

	private void addCalUnit(Map<DateRegion, Long> dataValues,
			DateRegion region, Long value) {
		if (dataValues.containsKey(region)) {
			dataValues.put(region, dataValues.get(region) + value);
		} else {
			dataValues.put(region, value);
		}
	}

	public Map<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> getAggResults() {
		return aggResults;
	}

	public void setAggResults(
			HashMap<AnalyticTask, HashMap<DataType, TreeMap<DateRegion, Long>>> aggResults) {
		this.aggResults = aggResults;
	}

}
