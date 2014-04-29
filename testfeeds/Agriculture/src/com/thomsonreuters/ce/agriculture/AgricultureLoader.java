package com.thomsonreuters.ce.agriculture;


import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import oracle.jdbc.OracleTypes;

import com.thomsonreuters.ce.agriculture.Starter;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.ZippedCSVProcessor;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;


public class AgricultureLoader extends ZippedCSVProcessor {

	private static final int DEF_DIV_SCALE = 10;

	private Logger logger  = Starter.thisLogger;
	public void ProcessCSVFile(String file_name, List<String[]> CSVArray) {
		
		Connection conn=null;

		try {
			try{
				conn = new EasyConnection(DBConnNames.CEF_CNR);
			}catch(Exception e){
				logger.error("New connection failed because: ", e);
				System.out.println("New connection failed because: "+e.getMessage());
			}

			CallableStatement cs_facility = conn
			.prepareCall("{call ags_facility_load_pkg.insert_facility_info_proc(?,?,?,?,?,?,?,?,?)}");
			CallableStatement cs_common = conn
			.prepareCall("{call ags_facility_load_pkg.insert_common_info_proc(?,?,?,?,?,?,?,?,?,?,?)}");
			CallableStatement cs_statistic = conn
			.prepareCall("{call ags_facility_load_pkg.update_statistic_data_proc(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			CallableStatement update_refresh_timestamp = conn
			.prepareCall("{call ags_facility_load_pkg.update_refresh_timestamp_proc()}");
			

			// String[] fn_items = fileName.split("_");
//			for (String[] record : CSVArray) {
//			ProcessSingleRecord(record);
//			}
			Map<String, String> colNamesValues = new HashMap<String, String>();			
			String[] arrColNames = CSVArray.get(0);

			for (int j = 1; j < CSVArray.size(); j++) {
				String[] record=CSVArray.get(j);

				String[] c_items = new String[record.length];
				for (int i = 0; i < record.length; i++) {
					if (record[i].indexOf("N/A") >= 0) {
						c_items[i] = record[i].replaceAll("N/A", "").trim();
					} else if (record[i].indexOf("unknown") >= 0) {
						c_items[i] = record[i].replaceAll("unknown", "").trim();
					} else {
						c_items[i] = record[i].trim();
					}
					colNamesValues.put(arrColNames[i].trim().toLowerCase(), c_items[i]);
				}

				try {
					// invoke procedure insert_facility_info_proc
					if (colNamesValues.get("unique_id") != null){
						cs_facility.setString("facility_id_in", colNamesValues.get("unique_id"));
					} else {
						cs_facility.setNull("facility_id_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("phone_numb") != null){
						cs_facility.setString("phone_number_in", colNamesValues.get("phone_numb"));
					} else {
						cs_facility.setNull("phone_number_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("address1") != null){
						cs_facility.setString("address_in", colNamesValues.get("address1"));
					} else {
						cs_facility.setNull("address_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("city") != null){
						cs_facility.setString("city_in", colNamesValues.get("city"));
					} else {
						cs_facility.setNull("city_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("mile_marker") != null){
						cs_facility.setString("mile_marker_in", colNamesValues.get("mile_marker"));
					} else {
						cs_facility.setNull("mile_marker_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("switching_district") != null){
						cs_facility.setString("switch_district_in", colNamesValues.get("switching_district"));
					} else {
						cs_facility.setNull("switch_district_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("nopa_member") != null){
						cs_facility.setString("nopa_member_in", colNamesValues.get("nopa_member"));
					} else {
						cs_facility.setNull("nopa_member_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("bnsf_number") != null){
						cs_facility.setString("bnsf_number_in", colNamesValues.get("bnsf_number"));
					} else {
						cs_facility.setNull("bnsf_number_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("ccl_code") != null){
						cs_facility.setString("ccl_code_in", colNamesValues.get("ccl_code"));
					} else {
						cs_facility.setNull("ccl_code_in", OracleTypes.VARCHAR);
					}
					cs_facility.execute();

					String longitude = colNamesValues.get("longitude");
					String latitude = colNamesValues.get("latitude");			
					String capacity = colNamesValues.get("capacity");
					String planned_capacity = colNamesValues.get("planned_capacity");
					String max_certs = colNamesValues.get("max_certs");
					String daily_loading_rate = colNamesValues.get("daily_loading_rate");
					String max_receipts_deliverable = colNamesValues.get("max_receipts_deliverable");
					String storage_rate = colNamesValues.get("storage_rate");
					String load_out_rate = colNamesValues.get("load_out_rate");
					String location_differential = colNamesValues.get("location_differential");

					// invoke procedure insert_common_info_proc			
					if (colNamesValues.get("unique_id") != null){
						cs_common.setString("facility_id_in", colNamesValues.get("unique_id"));
					} else {
						cs_common.setNull("facility_id_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("name") != null){
						cs_common.setString("name_in", colNamesValues.get("name"));
					} else {
						cs_common.setNull("name_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("owner") != null){
						cs_common.setString("owner_in", colNamesValues.get("owner"));
					} else {
						cs_common.setNull("owner_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("clearing_agent") != null){
						cs_common.setString("clearing_agent_in", colNamesValues.get("clearing_agent"));
					} else {
						cs_common.setNull("clearing_agent_in", OracleTypes.VARCHAR);
					}
					if (colNamesValues.get("data_source") != null){
						cs_common.setString("data_provider_in", colNamesValues.get("data_source"));
					} else {
						cs_common.setNull("data_provider_in", OracleTypes.VARCHAR);
					}			
					if (longitude != null){
						cs_common.setString("longitude_in", isNum(longitude) ? longitude
								: CaculateNum(longitude));
					} else {
						cs_common.setNull("longitude_in", OracleTypes.NUMBER);
					}			
					if (latitude != null){
						cs_common.setString("latitude_in", isNum(latitude) ? latitude
								: CaculateNum(latitude));
					} else {
						cs_common.setNull("latitude_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("state") != null){
						cs_common.setString("state_in", colNamesValues.get("state"));
					} else {
						cs_common.setNull("state_in", OracleTypes.VARCHAR);
					}
					if (colNamesValues.get("country") != null){
						cs_common.setString("country_in", colNamesValues.get("country"));
					} else {
						cs_common.setNull("country_in", OracleTypes.VARCHAR);
					}			
					if (colNamesValues.get("processing") != null){
						cs_common.setString("processing_in", colNamesValues.get("processing"));
					} else {
						cs_common.setNull("processing_in", OracleTypes.VARCHAR);
					}
					if (colNamesValues.get("grain_type") != null){
						cs_common.setString("grain_types_in", colNamesValues.get("grain_type"));
					} else {
						cs_common.setNull("grain_types_in", OracleTypes.VARCHAR);
					}						
					cs_common.execute();

					// invoke procedure update_statistic_data_proc
					if (colNamesValues.get("unique_id") != null){
						cs_statistic.setString("facility_id_in", colNamesValues.get("unique_id"));
					} else {
						cs_statistic.setNull("facility_id_in", OracleTypes.NUMBER);
					}
					if (capacity != null){
						cs_statistic.setString("capacity_in",
								isNum(capacity) ? capacity : CaculateNum(capacity));
					} else {
						cs_statistic.setNull("capacity_in", OracleTypes.NUMBER);
					}
					if (planned_capacity != null){
						cs_statistic.setString("planned_capacity_in",
								isNum(planned_capacity) ? planned_capacity : CaculateNum(planned_capacity));
					} else {
						cs_statistic.setNull("planned_capacity_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("capacity_unit") != null){
						cs_statistic.setString("capacity_unit_in", colNamesValues.get("capacity_unit"));
					} else {
						cs_statistic.setNull("capacity_unit_in", OracleTypes.VARCHAR);
					}
					if (max_certs != null){
						cs_statistic.setString("max_certs_in", isNum(max_certs) ? max_certs
								: CaculateNum(max_certs));
					} else {
						cs_statistic.setNull("max_certs_in", OracleTypes.NUMBER);
					}
					if (daily_loading_rate != null){
						cs_statistic.setString("daily_loading_rate_in",
								isNum(daily_loading_rate) ? daily_loading_rate
										: CaculateNum(daily_loading_rate));
					} else {
						cs_statistic.setNull("daily_loading_rate_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("daily_loading_rate_unit") != null){
						cs_statistic.setString("daily_loading_rate_unit_in", colNamesValues.get("daily_loading_rate_unit"));
					} else {
						cs_statistic.setNull("daily_loading_rate_unit_in", OracleTypes.VARCHAR);
					}			
					if (max_receipts_deliverable != null){
						cs_statistic.setString("max_receipts_deliverable_in",
								isNum(max_receipts_deliverable) ? max_receipts_deliverable
										: CaculateNum(max_receipts_deliverable));
					} else {
						cs_statistic.setNull("max_receipts_deliverable_in", OracleTypes.NUMBER);
					}			
					if (storage_rate != null){
						cs_statistic.setString("storage_rate_in",
								isNum(storage_rate) ? storage_rate : CaculateNum(storage_rate));
					} else {
						cs_statistic.setNull("storage_rate_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("storage_rate_unit") != null){
						cs_statistic.setString("storage_rate_unit_in", colNamesValues.get("storage_rate_unit"));
					} else {
						cs_statistic.setNull("storage_rate_unit_in", OracleTypes.VARCHAR);
					}			
					if (load_out_rate != null){
						cs_statistic.setString("load_out_rate_in",
								isNum(load_out_rate) ? load_out_rate : CaculateNum(load_out_rate));
					} else {
						cs_statistic.setNull("load_out_rate_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("load_out_rate_unit") != null){
						cs_statistic.setString("load_out_rate_unit_in", colNamesValues.get("load_out_rate_unit"));
					} else {
						cs_statistic.setNull("load_out_rate_unit_in", OracleTypes.VARCHAR);
					}			
					if (location_differential != null){
						cs_statistic.setString("location_differential_in",
								isNum(location_differential) ? location_differential : CaculateNum(location_differential));
					} else {
						cs_statistic.setNull("location_differential_in", OracleTypes.NUMBER);
					}			
					if (colNamesValues.get("location_differential_unit") != null){
						cs_statistic.setString("location_differential_unit_in", colNamesValues.get("location_differential_unit"));
					} else {
						cs_statistic.setNull("location_differential_unit_in", OracleTypes.VARCHAR);
					}
					cs_statistic.execute();

					conn.commit();
				} catch (SQLException e) {
					try {
						this.WriteLogs(colNamesValues.get("unique_id"), e.getMessage());
						conn.rollback();
					} catch (SQLException e1) {				
						this.WriteLogs(colNamesValues.get("unique_id"), e1.getMessage());				
					}			
				} catch (Exception e1) {			
					this.WriteLogs(colNamesValues.get("unique_id"), e1.getMessage());
				}			
			}
			
			update_refresh_timestamp.execute();
			conn.commit();			

		} catch (Exception e) 
		{
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + file_name + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + file_name, e);
			throw new SystemException(e.getMessage(), e);
		}
		finally
		{
			try {
				conn.close();
			} catch (SQLException e) {
				throw new com.thomsonreuters.ce.exception.SystemException(e.getMessage(),e);
			}	
		}
	}

	private boolean isNum(final String str) {
		boolean result = true;
		if (str.length() == 0)
			return false;
		for (int i = 0; i < str.length(); i++) {
			if (!Character.isDigit(str.charAt(i))) {
				if (!((i == 0) && (str.charAt(i) == '-'))
						&& !(str.charAt(i) == '.')) {
					result = false;
				}
			}
		}
		return result;
	}

	private String CaculateNum(String item) {
		double result = 0.0;
		if (item.length() == 0)
			return null;
		else if (item.indexOf("/") > 0) {
			double num1 = Double.parseDouble(item.split("/")[0].trim());
			double num2 = Double.valueOf(item.split("/")[1].trim())
			.doubleValue();
			result = CaculateDiv(num1, num2, DEF_DIV_SCALE);
		}
		return String.valueOf(result);
	}

	private double CaculateDiv(double num1, double num2, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException(
			"The scale must be a positive integer or zero");
		}

		BigDecimal b1 = new BigDecimal(Double.toString(num1));
		BigDecimal b2 = new BigDecimal(Double.toString(num2));

		double result = b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP)
		.doubleValue();
		return result;
	}

	private void WriteLogs(String record, String msg) {
		/*
		 * this.LogDetails(MsgCategory.WARN, "agriculture: Failed process " +
		 * type + " due to " + msg);
		 */
		logger.warn("agriculture:  Failed process record with unique_id="
				+ record + " due to " + msg);
		System.out.println("agriculture:  Failed process  record with unique_id=" + record
				+ " due to " + msg);
	}

	public void Finalize() {
		// TODO Auto-generated method stub

	}

	public void Initialize(File FeedFile) {
		// TODO Auto-generated method stub

	}

	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("AGRICULTURE");
	}

}
