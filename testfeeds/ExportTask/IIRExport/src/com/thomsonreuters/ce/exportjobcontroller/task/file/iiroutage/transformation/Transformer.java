/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.IIRTSProcessor;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.AnalyticTask;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DataUnit;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Comparators;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.ConcurrentDateUtil;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.AnalyticType;
import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility.Constants.DataType;

/**
 * @author lei.yang
 *
 */
public class Transformer implements Transformable{
	private List<AnalyticTask> sourceList;
	private static Logger logger = Starter.getLogger(IIRTSProcessor.SERVICE_NAME);
	public Transformer(List<AnalyticTask> lstAnaTask){
		this.sourceList = lstAnaTask;
	}

	@Override
	public List<DataUnit> transform() {
		logger.info( Constants.IIR_EMSG_PREFIX + 
				" transform...");
		
		/**
		 * 3. Analyze task list and collect data lstDataUnitMeta contains
		 * the dataset as below: permId, datatype, startdate(calculate_from),
		 * enddate(calculate_to) it should be splited by the date region with
		 * commodity data in db
		 */
		
		/**
		 *		permId, dataType, 		  startDate, endDate, value
		 * 		p1	  , INSTALL_CAPACITY, cal_from1, cal_to1, null  : INSTALL_CAPACITY
		 * 		p2    , INSTALL_CAPACITY, cal_from1, cal_to1, null  : REAL_CAPACITY  in task1
		 * 		p2	  , OUTAGE, 		  cal_from1, cal_to1, null  
		 * 		p3    , INSTALL_CAPACITY, cal_from, cal_to, null  : REAL_CAPACITY
		 * 		p3	  , OUTAGE, 		  cal_from, cal_to, null  
		 *  	p2    , INSTALL_CAPACITY, cal_from2, cal_to2, null  : REAL_CAPACITY  in task2
		 * 		p2	  , OUTAGE, 		  cal_from2, cal_to2, null
		 */		
		List<DataUnit> lstDataUnitMeta = transformToDU(this.sourceList);

		/**
		 * sort DataUnitMeta list by the order dataType, perm_id and startDate. eg as below: 
		 *		permId, dataType, 		  startDate, endDate, value
		 * 		p1	  , INSTALL_CAPACITY, cal_from1, cal_to1, null  : INSTALL_CAPACITY
		 * 		p2    , INSTALL_CAPACITY, cal_from1, cal_to1, null  : REAL_CAPACITY  in task1
		 *  	p2    , INSTALL_CAPACITY, cal_from2, cal_to2, null  : REAL_CAPACITY  in task2
		 * 		p3    , INSTALL_CAPACITY, cal_from, cal_to, null  : REAL_CAPACITY
		 * 		p2	  , OUTAGE, 		  cal_from1, cal_to1, null : in task1
		 * 		p2	  , OUTAGE, 		  cal_from2, cal_to2, null : in task2
		 * 		p3	  , OUTAGE, 		  cal_from, cal_to, null			 
		 */
		return sortAndMergeDataUnitMeta(lstDataUnitMeta);
		
	}
	
	/**
	 * Transfor analytic task list to data unit list
	 * NOTE: this method should be modified if new analyticType is added.
	 * @param lstAnaTask
	 * @return DataUnit list:
	 * 		permId, dataType, startDate, endDate, value
	 * 		p1	  , INSTALL_CAPACITY, cal_from1, cal_to1, null  : INSTALL_CAPACITY
	 * 		p2    , INSTALL_CAPACITY, cal_from1, cal_to1, null  : REAL_CAPACITY  in task1
	 * 		p2	  , OUTAGE, 		  cal_from1, cal_to1, null  
	 * 		p3    , INSTALL_CAPACITY, cal_from, cal_to, null  : REAL_CAPACITY
	 * 		p3	  , OUTAGE, 		  cal_from, cal_to, null  
	 *  	p2    , INSTALL_CAPACITY, cal_from2, cal_to2, null  : REAL_CAPACITY  in task2
	 * 		p2	  , OUTAGE, 		  cal_from2, cal_to2, null
	 */
	private List<DataUnit> transformToDU(List<AnalyticTask> lstAnaTask) {
		logger.info( Constants.IIR_EMSG_PREFIX + 
				"transfor analytic task list to dataunit list ...");

		List<DataUnit> lstDataUnit = new ArrayList<DataUnit>();
		for (AnalyticTask curTask : lstAnaTask) {
			List<Long> lstPermId = curTask.getLstPermId();
			for (Long curPermId : lstPermId) {
				DataUnit du = new DataUnit();
				du.setPermId(curPermId);
				du.setStartDate(curTask.getStartDate());
				du.setEndDate(curTask.getEndDate());

				AnalyticType anaTypeName = curTask.getAnalyticTypeName();
				if (anaTypeName.equals(AnalyticType.IIR_AGG_INSTALLED_CAPACITY)) {
					du.setDataType(DataType.INSTALL_CAPACITY);
					lstDataUnit.add(du);
				} else if (anaTypeName.equals(AnalyticType.IIR_AGG_REAL_CAPACITY)
						|| anaTypeName.equals(AnalyticType.IIR_REAL_CAPACITY)) {
					du.setDataType(DataType.INSTALL_CAPACITY);

					DataUnit anDU = new DataUnit();
					anDU.setPermId(du.getPermId());
					anDU.setDataType(DataType.OUTAGE);
					anDU.setStartDate(du.getStartDate());
					anDU.setEndDate(du.getEndDate());
					

					lstDataUnit.add(du);
					lstDataUnit.add(anDU);
				}
			}
		}
		
		// remove duplicate dataunit: permId+dataType+startDate+endDate
		Set<String> uniqueSet = new HashSet<String>();
		String strStartDate = null;
		String strEndDate = null;
		List<DataUnit> delDU = new ArrayList<DataUnit>();
		for (Iterator<DataUnit> it = lstDataUnit.iterator(); it.hasNext();) {
			DataUnit du = it.next();
			if (du.getStartDate() != null)
				strStartDate = ConcurrentDateUtil.formatToyMdDash(du.getStartDate());
			if (du.getEndDate() != null)
				strEndDate = ConcurrentDateUtil.formatToyMdDash(du.getEndDate());
			if (!uniqueSet
					.add(du.getPermId().toString() + du.getDataType().toString() + strStartDate + strEndDate))
//				it.remove();
				delDU.add(du);
		}
		if (!delDU.isEmpty())
			lstDataUnit.removeAll(delDU);
		
		return lstDataUnit;
	}

	/**
	 * Sort DataUnitMeta list by fieds: dataType, permId, startDate
	 * 
	 * @param lstDataUnitMeta
	 * 
	 *		permId, dataType, 		  startDate, endDate, value
	 * 		p1	  , INSTALL_CAPACITY, cal_from1, cal_to1, null  : INSTALL_CAPACITY
	 * 		p2    , INSTALL_CAPACITY, cal_from1, cal_to1, null  : REAL_CAPACITY  in task1
	 *  	p2    , INSTALL_CAPACITY, cal_from2, cal_to2, null  : REAL_CAPACITY  in task2
	 * 		p3    , INSTALL_CAPACITY, cal_from, cal_to, null  : REAL_CAPACITY
	 * 		p2	  , OUTAGE, 		  cal_from1, cal_to1, null : in task1
	 * 		p2	  , OUTAGE, 		  cal_from2, cal_to2, null : in task2
	 * 		p3	  , OUTAGE, 		  cal_from, cal_to, null
	 */
	private List<DataUnit> sortAndMergeDataUnitMeta(List<DataUnit> lstDataUnitMeta) {
//		Comparator<AnalyticTask> cmp = ComparableComparator.getInstance();
//		cmp = ComparatorUtils.nullLowComparator(cmp);
//
//		List<Object> sortFields = new ArrayList<Object>();
//		sortFields.add(new BeanComparator(SortFields.dataType.toString(), cmp));
//		sortFields.add(new BeanComparator(SortFields.permId.toString()));
//		sortFields.add(new BeanComparator(SortFields.startDate.toString()));
//
//		ComparatorChain multiSort = new ComparatorChain(sortFields);
//		Collections.sort(lstDataUnitMeta, multiSort);
		
		//TODO:
		Collections.sort(lstDataUnitMeta, Comparators.getComparator());				
		
		List<DataUnit> mergedDUM = new ArrayList<DataUnit>();
		// merge
		DataUnit preDUM = null;
		for (DataUnit dum : lstDataUnitMeta){
			if (mergedDUM.size() == 0){
				DataUnit newDum = new DataUnit(dum);
				mergedDUM.add(newDum);
				preDUM = newDum;
			} else if (preDUM.getPermId().compareTo(dum.getPermId()) == 0 
					&& preDUM.getDataType().compareTo(dum.getDataType()) == 0){
				if (preDUM.getEndDate().before(dum.getStartDate())){
					DataUnit secDum = new DataUnit(dum);
					mergedDUM.add(secDum);
					preDUM = secDum;
				} else if (preDUM.getEndDate().before(dum.getEndDate())){
					preDUM.setEndDate(dum.getEndDate());
				}
			} else {
				DataUnit newDum = new DataUnit(dum);
				mergedDUM.add(newDum);
				preDUM = newDum;
			}
		}
		return mergedDUM;
	}

	
}
