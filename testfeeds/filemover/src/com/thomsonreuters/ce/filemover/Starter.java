package com.thomsonreuters.ce.filemover;

import java.util.Properties;
import java.io.FileInputStream;
import java.rmi.RMISecurityManager;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.Permission;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;



import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.timing.TimerPool;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.dbor.server.SrvControl;
import com.thomsonreuters.ce.dbor.server.impl.RemoteControlImpl;

public class Starter implements SrvControl {
	
	/////////////////////////////////////////////////////////////////////////////////
	// config file path
	/////////////////////////////////////////////////////////////////////////////////
	private static final String Config_File = "../cfg/filemover.conf";

	public TimerPool TimerService=null;
	
	public ThreadController TC=null;
	
	public static String SERVICE_NAME="filemover";
	
	private Logger thisLogger;
	
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
		
		thisLogger.info("Timer service is working");
		
		//////////////////////////////////////////////
		
		String DatasetNames = prop.getProperty("filejobs");
		StringTokenizer DataSetNamesList = new StringTokenizer(DatasetNames,
				",", false);
		
		TC=new ThreadController();	
		
		while (DataSetNamesList.hasMoreTokens()) {

			String DataSetName = DataSetNamesList.nextToken().trim();
			String strlogger=prop.getProperty(DataSetName + ".logger");
			Logger fileLogger=Logger.getLogger(strlogger);
			String waiting=FileUtilities.GetAbsolutePathFromEnv(prop.getProperty(DataSetName + ".waiting"));
			String archive=FileUtilities.GetAbsolutePathFromEnv(prop.getProperty(DataSetName + ".archive"));
			String destination=prop.getProperty(DataSetName + ".destination");
			int CheckInterval=Integer.parseInt(prop.getProperty(DataSetName + ".checkinterval"));
			String fileextension=prop.getProperty(DataSetName + ".fileextension");
			
			FileDetector newScanner=new FileDetector(waiting, archive, destination, fileLogger, fileextension,TC);
			TimerService.createTimer(0, CheckInterval, newScanner);
			
			fileLogger.info("Data file name: "+DataSetName);
			fileLogger.info("Logger name: "+strlogger);
			fileLogger.info("Waiting folder : "+waiting);
			fileLogger.info("Archive folder: "+archive);
			fileLogger.info("Destination folder: "+destination);
			fileLogger.info("New file check interval: "+CheckInterval);
			fileLogger.info("Naming convention : "+fileextension);
			
		}
		
		thisLogger.info("All file detectors have been started");
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
		Starter filemover=new Starter();
		filemover.Start(prop);

		//Bind service
		try {
			RemoteControlImpl RCI=new RemoteControlImpl(filemover);		
			reg.bind(SERVICE_NAME, RCI);
		} catch (Exception e) {
			filemover.thisLogger.error("Failed to bind "+ SERVICE_NAME+" to RMI registry!",e);
			filemover.Stop();
			return;
		}
		
		filemover.thisLogger.info(SERVICE_NAME+ " is working now!");
	}

}