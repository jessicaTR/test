package com.thomsonreuters.ce.platts;

import java.io.File;
import java.io.FileReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.Types;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.CSVDataSet;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.file.FileProcessor;

public class PlattsEwindowLoader extends FileProcessor {

	private Logger logger = Starter.thisLogger;
	public void ProcessFile(File FeedFile) {
		// TODO Auto-generated method stub

		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);		
		String file_name=FeedFile.getName();
		
		String window_name="";
		if (file_name.contains("corrections"))
		{
			//for correction files
			window_name=file_name.substring(0,file_name.length()-25);
		}
		else
		{	//for daily files
			window_name=file_name.substring(0,file_name.length()-13);
		}

		try {
			
			CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  PLATTS_EWINDOW_LOAD_PKG.initialize_session_variables()}");
			callableClearSessionSM.execute();
			callableClearSessionSM.close();
			
			FileReader FR=new FileReader(FeedFile);
			CSVDataSet CDS=new CSVDataSet(FR,',','"','\b',0,false);
			
			while(CDS.next())
			{
				//////////////////////////////
				//read values from current row
				//////////////////////////////
				//input parameters for insert_order_fn
				try {
					Long order_id=null;
					String temp_order_id=CDS.getValue("ORDERID");
					if (!temp_order_id.equals(""))
					{
						order_id=Long.valueOf(temp_order_id);
					}

					Integer event_sequence=null;
					String temp_event_sequence=CDS.getValue("EVENTSEQUENCE");
					if (!temp_event_sequence.equals(""))
					{
						event_sequence=Integer.valueOf(temp_event_sequence);
					}

					String window_region=CDS.getValue("WINDOW_REGION");

					Timestamp time_stamp=null;
					String temp_time_stamp=CDS.getValue("TIMESTAMP");
					if (!temp_time_stamp.equals(""))
					{
						time_stamp=strToTimeStamp(CDS.getValue("TIMESTAMP"));
					}


					String order_state=CDS.getValue("ORDERSTATE");
					String product_name=CDS.getValue("PRODUCTNAME");
					String hub_name=CDS.getValue("HUBNAME");
					String strip_name=CDS.getValue("STRIPNAME");


					Date begin_date=null;
					String temp_begin_date=CDS.getValue("BEGINDATE");
					if (!temp_begin_date.equals(""))
					{
						begin_date=strToDate(temp_begin_date);
					}

					Date end_date=null;
					String temp_end_date=CDS.getValue("ENDDATE");
					if (!temp_end_date.equals(""))
					{
						end_date=strToDate(temp_end_date);
					}

					String order_type=CDS.getValue("ORDERTYPE");
					String order_classification=CDS.getValue("ORDERCLASSIFICATION");
					String market=CDS.getValue("MARKET");
					String sender=CDS.getValue("SENDERCOMPANYNAME");
					String buyer=CDS.getValue("BUYERCOMPANYNAME");
					String seller=CDS.getValue("SELLERCOMPANYNAME");
					if(order_state!=null && order_state.equals("consummated"))
					{
						if(buyer==null || buyer.equals("")) buyer="N/A";
						if(seller==null || seller.equals("")) seller="N/A";
					}
					String oco_order_ids=CDS.getValue("OCOORDERIDS");

					String market_state=CDS.getValue("MARKETSTATE");

					Double price=null;
					String temp_price=CDS.getValue("PRICE");
					if (!temp_price.equals(""))
					{
						price=Double.valueOf(temp_price);
					}

					String price_uom=CDS.getValue("PRICE_UOM");

					Double qty_multiple_out=null;
					String temp_qty_multiple_out=CDS.getValue("QTYMULTIPLIEDOUT");
					if (!temp_qty_multiple_out.equals(""))
					{
						qty_multiple_out=Double.valueOf(temp_qty_multiple_out);
					}


					Double quantity=null;
					String temp_quantity=CDS.getValue("QUANTITY");
					if (!temp_quantity.equals(""))
					{
						quantity=Double.valueOf(temp_quantity);
					}

					Double quantity_to=null;
					String temp_quantity_to=CDS.getValue("QUANTITY_TO");
					if (!temp_quantity_to.equals(""))
					{
						quantity_to=Double.valueOf(temp_quantity_to);
					}

					String units=CDS.getValue("UNITS");
					String is_cancelled=CDS.getValue("ISCANCELLED");

					//input parameters for insert_event_c_proc
					String tqc=CDS.getValue("TQC");

					Double c1_percentage=null;
					String temp_c1_percentage=CDS.getValue("C1_PERCENTAGE");
					if (!temp_c1_percentage.equals(""))
					{
						c1_percentage=Double.valueOf(temp_c1_percentage);
					}

					String c1_pricing_basis=CDS.getValue("C1_PRICING_BASIS");
					String c1_pricing_basis_period1=CDS.getValue("C1_PRICING_BASIS_PERIOD1");
					String c1_pricing_basis_period2=CDS.getValue("C1_PRICING_BASIS_PERIOD2");

					Double c1_price=null;
					String temp_c1_price=CDS.getValue("C1_PRICE");
					if (!temp_c1_price.equals(""))
					{
						c1_price=Double.valueOf(temp_c1_price);
					}


					Double c2_percentage=null;
					String temp_c2_percentage=CDS.getValue("C2_PERCENTAGE");
					if (!temp_c2_percentage.equals(""))
					{
						c2_percentage=Double.valueOf(temp_c2_percentage);
					}

					String c2_pricing_basis=CDS.getValue("C2_PRICING_BASIS");
					String c2_pricing_basis_period1=CDS.getValue("C2_PRICING_BASIS_PERIOD1");
					String c2_pricing_basis_period2=CDS.getValue("C2_PRICING_BASIS_PERIOD2");

					Double c2_price=null;
					String temp_c2_price=CDS.getValue("C2_PRICE");
					if (!temp_c2_price.equals(""))
					{
						c2_price=Double.valueOf(temp_c2_price);
					}		    

					Double c3_percentage=null;
					String temp_c3_percentage=CDS.getValue("C3_PERCENTAGE");
					if (!temp_c3_percentage.equals(""))
					{
						c3_percentage=Double.valueOf(temp_c3_percentage);
					}		    

					String c3_pricing_basis=CDS.getValue("C3_PRICING_BASIS");
					String c3_pricing_basis_period1=CDS.getValue("C3_PRICING_BASIS_PERIOD1");
					String c3_pricing_basis_period2=CDS.getValue("C3_PRICING_BASIS_PERIOD2");

					Double c3_price=null;
					String temp_c3_price=CDS.getValue("C3_PRICE");
					if (!temp_c3_price.equals(""))
					{
						c3_price=Double.valueOf(temp_c3_price);
					}	

					//////////////////////////////////////////////////
					//Parsing file done
					//////////////////////////////////////////////////


					CallableStatement callableorderSM = DBConn.prepareCall("{call PLATTS_EWINDOW_LOAD_PKG.insert_order_event_fn(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");

					callableorderSM.setString(1, window_name);
					callableorderSM.setString(2, window_region);
					
					if (time_stamp!=null)
					{
						callableorderSM.setTimestamp(3, time_stamp);
					}
					else
					{
						callableorderSM.setNull(3, Types.TIMESTAMP);		    	
					}
					
					callableorderSM.setString(4, market_state);
					
					
					if (order_id!=null)
					{
						callableorderSM.setLong(5,order_id.longValue());
					}
					else
					{
						callableorderSM.setNull(5, Types.BIGINT);		    	
					}

					if (event_sequence!=null)
					{
						callableorderSM.setInt(6,event_sequence.intValue());
					}
					else
					{
						callableorderSM.setNull(6, Types.INTEGER);		    	
					}
					
					callableorderSM.setString(7, product_name);
					callableorderSM.setString(8, hub_name);
					callableorderSM.setString(9, strip_name);
					
					if (begin_date!=null)
					{
						callableorderSM.setDate(10, begin_date);
					}
					else
					{
						callableorderSM.setNull(10, Types.DATE);		    	
					}
					
					if (end_date!=null)
					{
						callableorderSM.setDate(11, end_date);
					}
					else
					{
						callableorderSM.setNull(11, Types.DATE);		    	
					}	    					
					
					callableorderSM.setString(12, order_type); 
					callableorderSM.setString(13, market);
					callableorderSM.setString(14, sender);
					callableorderSM.setString(15, buyer);
					callableorderSM.setString(16, seller);
					callableorderSM.setString(17, order_state);
					callableorderSM.setString(18, order_classification);
					callableorderSM.setString(19, oco_order_ids);
					
					if (price!=null)
					{
						callableorderSM.setDouble(20, price.doubleValue());
					}
					else
					{
						callableorderSM.setNull(20, Types.DOUBLE);
					}
					
					callableorderSM.setString(21, price_uom);
					
					if (qty_multiple_out!=null)
					{
						callableorderSM.setDouble(22, qty_multiple_out.doubleValue());
					}
					else
					{
						callableorderSM.setNull(22, Types.DOUBLE);
					}

					if (quantity!=null)
					{
						callableorderSM.setDouble(23, quantity.doubleValue());
					}
					else
					{
						callableorderSM.setNull(23, Types.DOUBLE);
					}
					
					if (quantity_to!=null)
					{
						callableorderSM.setDouble(24, quantity_to.doubleValue());
					}
					else
					{
						callableorderSM.setNull(24, Types.DOUBLE);
					}

					callableorderSM.setString(25, units);
					callableorderSM.setString(26, is_cancelled);
					callableorderSM.setString(27, tqc);
					
					if (c1_percentage!=null)
					{
						callableorderSM.setDouble(28, c1_percentage.doubleValue());
					}
					else
					{
						callableorderSM.setNull(28, Types.DOUBLE);
					}

					callableorderSM.setString(29, c1_pricing_basis);
					callableorderSM.setString(30, c1_pricing_basis_period1);
					callableorderSM.setString(31, c1_pricing_basis_period2);

					if (c1_price!=null)
					{
						callableorderSM.setDouble(32, c1_price.doubleValue());
					}
					else
					{
						callableorderSM.setNull(32, Types.DOUBLE);
					}

					if (c2_percentage!=null)
					{
						callableorderSM.setDouble(33, c2_percentage.doubleValue());
					}
					else
					{
						callableorderSM.setNull(33, Types.DOUBLE);
					}

					callableorderSM.setString(34, c2_pricing_basis);
					callableorderSM.setString(35, c2_pricing_basis_period1);
					callableorderSM.setString(36, c2_pricing_basis_period2);

					if (c2_price!=null)
					{
						callableorderSM.setDouble(37, c2_price.doubleValue());
					}
					else
					{
						callableorderSM.setNull(37, Types.DOUBLE);
					}


					if (c3_percentage!=null)
					{
						callableorderSM.setDouble(38, c3_percentage.doubleValue());
					}
					else
					{
						callableorderSM.setNull(38, Types.DOUBLE);
					}

					callableorderSM.setString(39, c3_pricing_basis);
					callableorderSM.setString(40, c3_pricing_basis_period1);
					callableorderSM.setString(41, c3_pricing_basis_period2);

					if (c3_price!=null)
					{
						callableorderSM.setDouble(42, c3_price.doubleValue());
					}
					else
					{
						callableorderSM.setNull(42, Types.DOUBLE);
					}
					

					callableorderSM.execute();
					callableorderSM.close();
					DBConn.commit();

				} catch (Exception e) {
					// TODO Auto-generated catch block
					this.LogDetails(MsgCategory.WARN, "Failed to insert row:" +CDS.toString()+ " because of exception: "+e.getMessage());
					logger.error("Failed to insert row:" +CDS.toString(), e);
				}

			}
			
			FR.close();
			
			try{
				
				CallableStatement callableorderSM = DBConn.prepareCall("{call ce_refresh_maintain_pkg.update_last_upd_date_proc('PLATTS EWINDOW')}");
				callableorderSM.execute();
				callableorderSM.close();
				DBConn.commit();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.warn("SQLException", e);					
			}	
			
		}
		catch (Exception e) {
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + file_name + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + file_name, e);
			throw new SystemException(e.getMessage(), e);
		}
		finally
		{
			try {
				DBConn.close();
			} catch (SQLException e) {
				throw new SystemException(e.getMessage(),e);
			}	
		}
	}

	public FileCategory getFileCatory(File FeedFile) {
		// TODO Auto-generated method stub
		return FileCategory.getInstance("PLATTS EWINDOW");
	}

	private Timestamp strToTimeStamp(String strDate) { 
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"); 
		ParsePosition pos = new ParsePosition(0); 
		Timestamp ts = new Timestamp(formatter.parse(strDate, pos).getTime()); 
		return ts; 
	} 

	private Date strToDate(String strDate) { 
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd"); 
		ParsePosition pos = new ParsePosition(0); 
		Date d = new Date(formatter.parse(strDate, pos).getTime()); 
		return d; 
	} 	


}
