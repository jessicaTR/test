package com.thomsonreuters.ce.IIROutageLoader;

import java.rmi.RMISecurityManager;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.Permission;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.file.FileFolderInfo;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.server.SrvControl;
import com.thomsonreuters.ce.dbor.server.impl.RemoteControlImpl;
import com.thomsonreuters.ce.dbor.tools.FileScanner;
import com.thomsonreuters.ce.timing.TimerPool;
import com.thomsonreuters.ce.thread.ThreadController;

public class Starter implements SrvControl {
	/////////////////////////////////////////////////////////////////////////////////
	// config file path
	/////////////////////////////////////////////////////////////////////////////////
	private static final String Config_File = "../cfg/iiroutage.conf"; 
//	private static final String Config_File = "D:\\CE WORK FOLDER\\gitDBOR\\dbor_nda_applications-master\\Agriculture\\cfg\\agriculture.conf"; 
	public TimerPool TimerService=null;
	
	public static ThreadController TC=null;
	
	public static String SERVICE_NAME="iiroutage";
	
	public static Logger thisLogger;
	
	public void Start(Properties prop)
	{

			/////////////////////////////////////////////////////////////////////////////
			// Initialize logging
			/////////////////////////////////////////////////////////////////////////////
			String loggingCfg = prop.getProperty("logging.configuration");
			PropertyConfigurator.configure(loggingCfg);
			thisLogger=Logger.getLogger(SERVICE_NAME);
			thisLogger.info("Logging is working");
			
			/////////////////////////////////////////////////////////////////////////////
			// start Timer Service
			/////////////////////////////////////////////////////////////////////////////
			TimerService = new TimerPool(1);
			TimerService.Start();
			
			TC=new ThreadController();	
			
			thisLogger.info("Timer service is working");
			/////////////////////////////////////////////////////////////////////////////
			// Read config file into prop object
			/////////////////////////////////////////////////////////////////////////////
			String BaseFolder = System.getenv("FEEDSIN");

			String DataSetName = prop.getProperty("Datasets");
		
			long Interval = Long.parseLong(prop.getProperty(DataSetName + ".checkinterval"));			
			
			String FileExtension = prop.getProperty(DataSetName + ".fileextension");			
							
			String ArrivalFolder = BaseFolder + prop.getProperty(DataSetName + ".arrival");
		
			FileUtilities.CheckFolderExistence(ArrivalFolder);
			
			String WorkFolder = BaseFolder + prop.getProperty(DataSetName + ".work");
		
			FileUtilities.CheckFolderExistence(WorkFolder);
			
			String ArchiveFolder = BaseFolder + prop.getProperty(DataSetName + ".archive");
		
			FileUtilities.CheckFolderExistence(ArchiveFolder);
			
			String UnprocFolder = BaseFolder + prop.getProperty(DataSetName + ".unproc");
			
			FileUtilities.CheckFolderExistence(UnprocFolder);
			
				
			/////////////////////////////////////////////////////////////////////////////
			//Initialize database connection pool
			/////////////////////////////////////////////////////////////////////////////
			try{
				String db_Config_file = prop.getProperty("dbpool.configuration");
				EasyConnection.configPool(db_Config_file);
			}catch(Exception e){
				thisLogger.error("Config connection pool error:", e);
			}


			FileUtilities.cleanup(WorkFolder,ArrivalFolder,FileExtension);
			FileFolderInfo ffi = new FileFolderInfo(FileExtension,ArrivalFolder,WorkFolder,ArchiveFolder,UnprocFolder);
			IIROutageLoader afp =  new IIROutageLoader();
			FileProcessor fp = (FileProcessor) afp;
			FileScanner newScanner=new FileScanner(TC, ffi, fp, thisLogger);

			TimerService.createTimer(0,Interval, newScanner);
	}
	public void Stop()
	{
		//Normal shutdown

		TimerService.Stop();
		thisLogger.info("Timer service is down");

		TC.Shutdown();
		thisLogger.info("Shutdown signal is sent");

		TC.WaitToDone();
		thisLogger.info("All threads are done");

		thisLogger.info("Feed is put down as requested");
		
	}
	public static void main(String[] args) {
		System.setSecurityManager(new RMISecurityManager() {
			public void checkPermission(Permission p) {
			}
		});


		/////////////////////////////////////////////////////////////////////////////
		// start RMI binding
		/////////////////////////////////////////////////////////////////////////////
		String host_name=args[0];
		int rmi_port=Integer.parseInt(args[1]);	
		
		String[] names=null;
		Registry reg=null;
		try {
			reg=LocateRegistry.getRegistry(host_name,rmi_port);
			names = reg.list();
		} catch (Exception e) {
			System.out.println("Failed to access RMI Registry!");
			e.printStackTrace();
			return;
		}

		//Check if service name is bound already		
		for (String name : names)
		{
			if (name.equals(SERVICE_NAME))
			{
				try {
					reg.unbind(SERVICE_NAME);
				} catch (Exception e) {
					System.out.println("Failed to unbind previous service name: "+ SERVICE_NAME);
					return;
				}
			}
		}

		
		Properties prop = new Properties();

		FileInputStream fis;
		try {
			fis = new FileInputStream(Config_File);
			prop.load(fis);
			
			//Start service
			Starter agriculture=new Starter();
			agriculture.Start(prop);

			//Bind service
			try {
				RemoteControlImpl RCI=new RemoteControlImpl(agriculture);		
				reg.bind(SERVICE_NAME, RCI);
			} catch (Exception e) {
				agriculture.thisLogger.error("Failed to bind "+ SERVICE_NAME+" to RMI registry!",e);
				agriculture.Stop();
				return;
			}
			
			agriculture.thisLogger.info(SERVICE_NAME+ " is working now!");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("Agriculture config file can not be found !" + e.getMessage());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error happened when opening task config file !" + e.getMessage());
		} catch (Exception e)
		{
			System.out.println("Error happened!" + e.getMessage());
		}	
		
		
	}
	
	 
}
	