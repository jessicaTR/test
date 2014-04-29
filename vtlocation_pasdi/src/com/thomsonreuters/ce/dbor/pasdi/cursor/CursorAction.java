package com.thomsonreuters.ce.dbor.pasdi.cursor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.util.HashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;

import oracle.jdbc.OracleTypes;
import com.thomsonreuters.ce.database.EasyConnection;

import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.*;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.server.DBConnNames;


public class CursorAction implements Runnable {

	private static final String GetCursor="{call sdi_util_pkg_v2.collect_data(?,?,?)}";
	private HashMap<Long, HashMap<CursorType,FileDataMarker>> PERMID_CURSOR_MAP;
	private CursorType CT;
	private boolean IsFull=true;
	private Counter counter;
	private String TempFileName = null;

	public CursorAction(CursorType ct,HashMap<Long, HashMap<CursorType,FileDataMarker>> pcm, Counter cut)
	{

		this.CT=ct;
		this.PERMID_CURSOR_MAP=pcm;
		this.counter=cut;
		
		try {
			File tempFile = File.createTempFile(this.CT.getCursorName(), null, new File(SDIConstants.tempfolder));
			this.TempFileName=tempFile.getName();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error("IOException",e);
		}		
		finally
		{
			this.counter.Increase();
		}
	}
	
	public CursorAction(CursorType ct,HashMap<Long, HashMap<CursorType,FileDataMarker>> pcm, Counter cut,boolean isfull)
	{
		this(ct,pcm,cut);
		this.IsFull=isfull;
	}	
	
	

	public void run() {
		// TODO Auto-generated method stub

		Connection DBConn = null;

		try {

			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objStatement=DBConn.prepareCall(GetCursor);
			objStatement.setString("cur_name_in", this.CT.getCursorName());
			objStatement.registerOutParameter("cur_out", OracleTypes.CURSOR);
			
			if (this.IsFull)
			{
				objStatement.setString("indicator_in", "F");
			}
			else
			{
				objStatement.setString("indicator_in", "I");
			}
			
			objStatement.execute();
			ResultSet result_set = (ResultSet) objStatement.getObject("cur_out");
			result_set.setFetchSize(10);
			Long temp_perm_id=null;
			SDICursorRow[] SDICR=null;

			while (result_set.next()) {		

				SDICursorRow thisRow=this.CT.GetDataFromRS(result_set);	
				Long perm_id=thisRow.getPerm_ID();

				if (!perm_id.equals(temp_perm_id))
				{
					if (temp_perm_id!=null)
					{
						FileDataMarker FDM=this.saveResultSet(SDICR);

						HashMap<CursorType,FileDataMarker> HM;				

						synchronized(this.PERMID_CURSOR_MAP)
						{							
							HM=this.PERMID_CURSOR_MAP.get(temp_perm_id);

							if (HM==null)
							{
								HM=new HashMap<CursorType,FileDataMarker>();
								this.PERMID_CURSOR_MAP.put(temp_perm_id, HM);						
							}
						}						

						synchronized(HM)
						{				

							HM.put(this.CT, FDM);							
						}						

					}

					SDICR=new SDICursorRow[0];
					temp_perm_id=perm_id;
				}

				SDICursorRow[] tempSDICR=new SDICursorRow[SDICR.length+1];
				System.arraycopy(SDICR, 0, tempSDICR, 0, SDICR.length);
				tempSDICR[SDICR.length]=thisRow;
				SDICR=tempSDICR;
			}
			
			result_set.close();

			objStatement.close();
			
			if (temp_perm_id!=null)
			{

				FileDataMarker FDM=this.saveResultSet(SDICR);

				HashMap<CursorType,FileDataMarker> HM;				

				synchronized(this.PERMID_CURSOR_MAP)
				{							
					HM=this.PERMID_CURSOR_MAP.get(temp_perm_id);

					if (HM==null)
					{
						HM=new HashMap<CursorType,FileDataMarker>();
						this.PERMID_CURSOR_MAP.put(temp_perm_id, HM);						
					}
				}						

				synchronized(HM)
				{				

					HM.put(this.CT, FDM);							
				}						

			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error(e);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error("IOException",e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error("Unknown Exception",e);
		} 		
		finally
		{
			try {		
				DBConn.close();				
			} catch (SQLException e) {
				SDIConstants.SDILogger.error(e);
			}
			SDIConstants.SDILogger.info(this.CT.getCursorName() + "------------------->Done");
			this.counter.Decrease();
		}

		
	}
	
	public SDICursorRow[] getResultSet(FileDataMarker FDM) throws IOException,ClassNotFoundException
	{

		File TempFile=new File(SDIConstants.tempfolder,this.TempFileName);
		RandomAccessFile DiskCache = new RandomAccessFile(TempFile, "rw");
		
		DiskCache.seek(FDM.getPosition());
		byte[] OArray = new byte[FDM.getSize()];
		DiskCache.read(OArray);
		DiskCache.close();
		
		ByteArrayInputStream bOP = new ByteArrayInputStream(OArray);
		ObjectInputStream OIS=new ObjectInputStream(bOP);

		SDICursorRow[] SCR=(SDICursorRow[])OIS.readObject();
		OIS.close();
		return SCR;
	}
	
	
	
	
	public FileDataMarker saveResultSet(SDICursorRow[] SCR) throws IOException
	{
		File TempFile=new File(SDIConstants.tempfolder,this.TempFileName);
		RandomAccessFile DiskCache = new RandomAccessFile(TempFile, "rw");
		
		ByteArrayOutputStream bOP = new ByteArrayOutputStream();
		ObjectOutputStream oO = new ObjectOutputStream( bOP);
		oO.writeObject(SCR);
		oO.flush();
		oO.close();
		
		byte[] OArray = bOP.toByteArray();
		int ObjectSize=OArray.length;
		
		long position=DiskCache.length();
		DiskCache.seek(position);
		DiskCache.write(OArray);
		DiskCache.close();
		
		return new FileDataMarker(this, position,ObjectSize);
		
	}
	
	public void Delete()
	{
		File TempFile=new File(SDIConstants.tempfolder,this.TempFileName);
		TempFile.delete();
		
	}	


}
