package com.thomsonreuters.ce.oasdi;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.Permission;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.file.FileFolderInfo;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.dbor.tools.FileScanner;
import com.thomsonreuters.ce.timing.TimerPool;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.dbor.server.SrvControl;
import com.thomsonreuters.ce.dbor.server.impl.RemoteControlImpl;

public class Starter implements SrvControl {
	
	/////////////////////////////////////////////////////////////////////////////////
	// config file path
	/////////////////////////////////////////////////////////////////////////////////
	private static final String Config_File = "../cfg/app.conf";

	public TimerPool TimerService=null;
	
	public ThreadController TC=null;
	
	public Logger thisLogger=null;
	
	public static String SERVICE_NAME="oasdi";
	
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
		//Initialize database connection pool
		/////////////////////////////////////////////////////////////////////////////
		String db_Config_file = prop.getProperty("dbpool.configuration");
		EasyConnection.configPool(db_Config_file);
		thisLogger.info("Database connection pool is working");
		
		/////////////////////////////////////////////////////////////////////////////
		// start Timer Service
		/////////////////////////////////////////////////////////////////////////////
		TimerService = new TimerPool(1);
		TimerService.Start();
		
		thisLogger.info("Timer service is working");
		
		long Interval = Long.parseLong(prop.getProperty("filecheckinterval"));
		thisLogger.debug("filecheckinterval: " + Interval);
		
		String FileExtension = prop.getProperty("filepattern");
		thisLogger.debug("filepattern: " + FileExtension);
						
		String ArrivalFolder = FileUtilities.GetAbsolutePathFromEnv(prop.getProperty("arrivalfolder"));
		thisLogger.debug("arrivalfolder: " + ArrivalFolder);
		FileUtilities.CheckFolderExistence(ArrivalFolder);
		
		String WorkFolder = FileUtilities.GetAbsolutePathFromEnv(prop.getProperty("workfolder"));
		thisLogger.debug("workfolder: " + WorkFolder);
		FileUtilities.CheckFolderExistence(WorkFolder);
		
		String ArchiveFolder = FileUtilities.GetAbsolutePathFromEnv(prop.getProperty("archivefolder"));
		thisLogger.debug("archivefolder: " + ArchiveFolder);
		FileUtilities.CheckFolderExistence(ArchiveFolder);
		
		String UnprocFolder = FileUtilities.GetAbsolutePathFromEnv(prop.getProperty("unprocfolder"));
		thisLogger.debug("unprocfolder: " + UnprocFolder);		
		FileUtilities.CheckFolderExistence(UnprocFolder);
		
		FileUtilities.cleanup(WorkFolder,ArrivalFolder,FileExtension);
		thisLogger.info("File clean up is done");
		
		TC=new ThreadController();		
		FileFolderInfo FFI=new FileFolderInfo(FileExtension,ArrivalFolder,WorkFolder,ArchiveFolder,UnprocFolder);
		
		FileScanner FS = new FileScanner(TC, FFI, new OASDILoader(),thisLogger);
		TimerService.createTimer(Interval,Interval, FS);

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

		EasyConnection.CloseAllPool();
		thisLogger.info("DBPool is closed");

		thisLogger.info("Feed is put down as requested");
		
	}
	
	/**
	 * @param args
	 */
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
		
		/////////////////////////////////////////////////////////////////////////////
		// Read config file into prop object
		/////////////////////////////////////////////////////////////////////////////
		Properties prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(Config_File);
			prop.load(fis);
		} catch (Exception e) {
			System.out.println("Can't read configuration file: " + Config_File);
		}
		
		//Start service
		Starter oasdi=new Starter();
		oasdi.Start(prop);

		//Bind service
		try {
			RemoteControlImpl RCI=new RemoteControlImpl(oasdi);		
			reg.bind(SERVICE_NAME, RCI);
		} catch (Exception e) {
			oasdi.thisLogger.error("Failed to bind oasdi to RMI registry!",e);
			oasdi.Stop();
			return;
		}
		
		oasdi.thisLogger.info(SERVICE_NAME+ " is working now!");
	}

}
