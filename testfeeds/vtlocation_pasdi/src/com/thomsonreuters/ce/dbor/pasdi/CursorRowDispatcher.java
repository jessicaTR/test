package com.thomsonreuters.ce.dbor.pasdi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.thomsonreuters.ce.queue.MagicPipe;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.FileDataMarker;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.*;




public class CursorRowDispatcher {
	
	private int thread_num;
	
	private int ActiveNum=0;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_CPA;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_PA;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_VA;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_IDF;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_IDF_v4;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_EDR;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_GA;
	private MagicPipe<HashMap<CursorType, SDICursorRow[]>> MP_ALT;
	
	

	public CursorRowDispatcher(int Thread_Num, 
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_cpa,
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_pa,
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_va,
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_idf,
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_idf_v4,
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_edr,
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_ga,
			MagicPipe<HashMap<CursorType, SDICursorRow[]>> mp_alt)
	{
		this.thread_num=Thread_Num;
		this.MP_CPA=mp_cpa;		
		this.MP_PA=mp_pa;
		this.MP_VA=mp_va;
		this.MP_IDF=mp_idf;
		this.MP_IDF_v4 = mp_idf_v4;
		this.MP_EDR=mp_edr;
		this.MP_GA=mp_ga;
		this.MP_ALT=mp_alt;
	}
	
	public void start(Iterator<Map.Entry<Long, HashMap<CursorType,FileDataMarker>>> iter)
	{
		
		Thread[] ThreadArray=new Thread[this.thread_num];
		for (int i = 0; i < this.thread_num; i++) {
			ThreadArray[i] = new Thread( new Worker(iter));			
		}	
		
		for (int i = 0; i < this.thread_num; i++) {
			ThreadArray[i].start();		
		}	
		
		
	}
	
	private class Worker implements Runnable {
		
		private Iterator<Map.Entry<Long, HashMap<CursorType,FileDataMarker>>> iter;
		
		public Worker(Iterator<Map.Entry<Long, HashMap<CursorType,FileDataMarker>>> ITER)
		{
			this.iter=ITER;
			
			synchronized (CursorRowDispatcher.this) {
				CursorRowDispatcher.this.ActiveNum++;
			}
		}


		public void run() {
			
			while(true)
			{
				try {
					
					HashMap<CursorType,FileDataMarker> CursorData;
					
					synchronized(CursorRowDispatcher.this)
					{
						if (this.iter.hasNext()) {				
							Map.Entry<Long, HashMap<CursorType,FileDataMarker>> element=this.iter.next();
							CursorData=element.getValue();
							this.iter.remove();
						}
						else
						{
							break;
						}
					}
					
					HashMap<CursorType,SDICursorRow[]> All_Cursors=new HashMap<CursorType,SDICursorRow[]>();
					
					for (Iterator<Map.Entry<CursorType,FileDataMarker>> Asset_Iter=CursorData.entrySet().iterator(); Asset_Iter.hasNext();)
					{
						Map.Entry<CursorType,FileDataMarker> element=Asset_Iter.next();
						
						CursorType CT=element.getKey();
						FileDataMarker FDM=element.getValue();
						
						All_Cursors.put(CT, FDM.getResultSet());
						
					}
					
					//check commodity type
					SDICursorRow[] SCR=All_Cursors.get(CursorType.AST_BASE_INFO);
					String CommodityType=((AstBase)SCR[0]).getPas_type();
					
					if (CommodityType!=null)
					{
						if (CommodityType.equals("VESSEL"))
						{
							CursorRowDispatcher.this.MP_CPA.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_VA.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF_v4.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_EDR.putObj(All_Cursors);
							
							if (CursorRowDispatcher.this.MP_ALT!=null)
							{
								CursorRowDispatcher.this.MP_ALT.putObj(All_Cursors);
							}
							
						}
						else if (CommodityType.equals("PLANT")||CommodityType.equals("POWER"))
						{
							CursorRowDispatcher.this.MP_CPA.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_PA.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF_v4.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_EDR.putObj(All_Cursors);						
						}
						else if (CommodityType.equals("AGRICULTURE"))
						{
							CursorRowDispatcher.this.MP_CPA.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF_v4.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_GA.putObj(All_Cursors);
						}
						else if (CommodityType.equals("PORT")||CommodityType.equals("ANCHORAGE")||CommodityType.equals("BERTH"))
						{
							CursorRowDispatcher.this.MP_CPA.putObj(All_Cursors);
							//CursorRowDispatcher.this.MP_VA.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_IDF_v4.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_EDR.putObj(All_Cursors);
							CursorRowDispatcher.this.MP_GA.putObj(All_Cursors);
						}
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					SDIConstants.SDILogger.error("IOException",e);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					SDIConstants.SDILogger.error("ClassNotFoundException",e);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					SDIConstants.SDILogger.error("Unknown Exception",e);
				} 
				
			}
			
			
			synchronized (CursorRowDispatcher.this) {
				CursorRowDispatcher.this.ActiveNum--;
				
				if (CursorRowDispatcher.this.ActiveNum==0)
				{
					CursorRowDispatcher.this.MP_CPA.Shutdown(true);
					CursorRowDispatcher.this.MP_PA.Shutdown(true);
					CursorRowDispatcher.this.MP_VA.Shutdown(true);
					CursorRowDispatcher.this.MP_IDF.Shutdown(true);
					CursorRowDispatcher.this.MP_IDF_v4.Shutdown(true);
					CursorRowDispatcher.this.MP_EDR.Shutdown(true);
					CursorRowDispatcher.this.MP_GA.Shutdown(true);
					if (CursorRowDispatcher.this.MP_ALT!=null)
					{
						CursorRowDispatcher.this.MP_ALT.Shutdown(true);
					}
					
					SDIConstants.SDILogger.info("Dispatcher is done!");
					

				}

			}
			
		}
	}	
	
	
}
