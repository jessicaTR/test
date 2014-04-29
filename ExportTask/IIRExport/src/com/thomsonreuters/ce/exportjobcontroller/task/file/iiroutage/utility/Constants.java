package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility;

public class Constants {
	
	public enum AnalyticType{
		IIR_AGG_INSTALLED_CAPACITY, IIR_AGG_REAL_CAPACITY, IIR_REAL_CAPACITY
	}	
	
	public enum DataType{
		INSTALL_CAPACITY, OUTAGE
	}
	
	public enum SortFields{
		rreId, analyticTypeId, startDate,
		permId, dataType
	}
	
	public enum TaskStatus{
		WAITING, PROCESSING, COMPLETED, FAILED
	}
	
	public static final String IIR_EMSG_PREFIX = "IIR_TS: ";
	
	public static final String ORIGINAL_SERIALNUM = "000000";
	public static final int FILESIZE = 49999;
		
	public static final String IIR_REAL_FILE_HEADER = "RIC,TRADE_DATE,REFINERY_OPERATION\n";
	public static final String IIR_INSTALL_FILE_HEADER = "RIC,TRADE_DATE,REFINERY_INPUT\n";
	
	//SQLs
	
	/**
	 * function used to get analytic task list
	 * @prameter: 
	 * 	res_cur: OracleTypes.CURSOR, used to hold the return data set;
	 * 	tas_ids_in: oracle.sql.ARRAY, related task id array
	 * 	tad_id_in: task type id  
	 */
	public static final String SQL_GET_ANALYTIC_QUEUE = "{ ? = call cef_cnr.task_maintain_pkg.get_assoc_ana_queue_id_func(?,?) }";
	
	/**
	 * function used to get permId list
	 * @parameters:
	 * 	res_cursor: OracleTypes.CURSOR, used to hold the return data set;
	 * 	rre_ids_in: oracle.sql.ARRAY, related RIC_REPOSITORY_ID
	 * 	and_id_in: number, analytic type id
	 */
	public static final String SQL_GET_ANALYTIC_PERMIDS = "{ ? = call cef_cnr.task_translator_pkg.get_associated_perm_ids_func(?,?) }";
	
	/**
	 * procedure used to collect unit data
	 * @parameters:
	 * 	data_type: varchar, analytic type id; 	
	 * 	perm_ids: oracle.sql.ARRAY, related perm ids; 	
	 * 	data_cursor: OracleTypes.CURSOR, used to hold the return data set;
	 */	
	public static final String SQL_COLLECT_COMMODITY_DATA = "{? = call cef_cnr.task_data_collector_pkg.collect_ts_data(?,?,?,?,?)}";
	
	public static final String SQL_CREATE_PROCESS_HISTORY = "insert into cef_cnr.file_process_history (id, file_name,dit_file_category_id,start_time,dit_processing_status) values (fph_seq.nextval,?,?,sysdate,?)";
	public static final String SQL_COMPLETE_FILE_HISTORY = "update cef_cnr.file_process_history set end_time=sysdate, dit_processing_status=? where id=?";
	public static final String SQL_GET_PRE_FILENAME = "select file_name from cef_cnr.file_process_history where dit_file_category_id = ? and to_char(start_time, 'yyyymmdd') = ? and dit_processing_status = ? and file_name like 'NTSGEN-Refinery_%' order by id desc";
	public static final String SQL_DELETE_EMPTYFILE_RECORD = "delete from cef_cnr.file_process_history where id=?";
	
	public static final boolean ISTEST = false;
	public static final boolean ISDEBUG = false;
}
