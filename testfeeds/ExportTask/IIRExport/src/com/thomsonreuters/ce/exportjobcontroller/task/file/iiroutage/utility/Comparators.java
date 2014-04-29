/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility;

import java.util.Comparator;

import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.AnalyticTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DataUnit;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DateRegion;

/**
 * @author lei.yang
 * 
 */
public class Comparators {
	public static Comparator<AnalyticTask> getAnaTaskComparatorByType() {
		return new Comparator<AnalyticTask>() {

			@Override
			public int compare(AnalyticTask anat1, AnalyticTask anat2) {
				int compareTypeId = anat1.getAnalyticTypeId().compareTo(
						anat2.getAnalyticTypeId());
				int compareRreId = anat1.getRreId().compareTo(anat2.getRreId());
				int compareStartDate = anat1.getStartDate().compareTo(
						anat2.getStartDate());

				if (compareTypeId != 0)
					return compareTypeId;

				if (compareRreId != 0)
					return compareRreId;

				if (compareStartDate != 0)
					return compareStartDate;

				return 0;
			}
		};
	}

	public static Comparator<Object> getComparator() {
		return new Comparator<Object>() {

			@Override
			public int compare(Object o1, Object o2) {
				if (o1 instanceof DataUnit) {
					return compare((DataUnit) o1, (DataUnit) o2);
				} else if (o1 instanceof AnalyticTask) {
					return compare((AnalyticTask) o1, (AnalyticTask) o2);
				} else if (o1 instanceof DateRegion) {
					return compare((DateRegion) o1, (DateRegion) o2);
				}
				return 0;
			}

			// Sort DataUnitMeta list by fieds: dataType, permId, startDate
			private int compare(DataUnit du1, DataUnit du2) {
				try {

					int compareDataType = du1.getDataType().compareTo(
							du2.getDataType());
					int comparePermId = du1.getPermId().compareTo(
							du2.getPermId());
					int compareStartDate = du1.getStartDate().compareTo(
							du2.getStartDate());

					if (compareDataType != 0)
						return compareDataType;

					if (comparePermId != 0)
						return comparePermId;

					if (compareStartDate != 0)
						return compareStartDate;

					if (du1.getEndDate() != null && du2.getEndDate() != null) {
						int compareEndDate = du1.getEndDate().compareTo(
								du2.getEndDate());
						if (compareEndDate != 0)
							return compareEndDate;
					}
				} catch (NullPointerException ex) {
					throw new NullPointerException(
									Constants.IIR_EMSG_PREFIX
											+ "IIROutageExport: Failed to compare DataUnit "
											+ "unit1: " + du1.toString()
											+ ";\r\n unit2: " + du2.toString());
				}
				return 0;
			}

			// Sort analytic tasks by fieds: rreid, analyticTypeId, startDate
			private int compare(AnalyticTask anat1, AnalyticTask anat2) {
				try {
					int compareRreId = anat1.getRreId().compareTo(
							anat2.getRreId());
					int compareTypeId = anat1.getAnalyticTypeId().compareTo(
							anat2.getAnalyticTypeId());
					int compareStartDate = anat1.getStartDate().compareTo(
							anat2.getStartDate());

					if (compareRreId != 0)
						return compareRreId;

					if (compareTypeId != 0)
						return compareTypeId;

					if (compareStartDate != 0)
						return compareStartDate;
				} catch (NullPointerException ex) {
					throw new NullPointerException(
									Constants.IIR_EMSG_PREFIX
											+ "IIROutageExport: Failed to compare AnalyticTask "
											+ "task1: " + anat1.toString()
											+ ";\r\n task2: " + anat2.toString());
				}
				return 0;
			}

			private int compare(DateRegion o1, DateRegion o2) {
				return o1.compareTo(o2);
			}
		};
	}

}
