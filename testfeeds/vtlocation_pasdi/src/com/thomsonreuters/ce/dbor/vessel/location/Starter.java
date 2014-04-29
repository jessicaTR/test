package com.thomsonreuters.ce.dbor.vessel.location;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.SrvControl;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.timing.TimerPool;
import com.thomsonreuters.ce.dbor.messaging.TRMessageProducer;


public class Starter implements SrvControl {
	
	/////////////////////////////////////////////////////////////////////////////////
	// config file path
	/////////////////////////////////////////////////////////////////////////////////
	private static final String Config_File = "../cfg/app.conf";
	
	public static TimerPool TimerService=null;
	
	public ThreadController TC=null;
	
	public Logger thisLogger=null;
	
	public static final String SERVICE_NAME="vtlocation_pasdi";
	
	public static Starter vtlocation_zone_passdi=null;
	
	
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
		//Initialize TR messaging publisher
		/////////////////////////////////////////////////////////////////////////////
		String messaging_config_file = prop.getProperty("publisher.configuration");
		if (messaging_config_file!=null && !messaging_config_file.equals(""))
		{
			TRMessageProducer.InitialMsgPublishers(messaging_config_file);
		}
		
		/////////////////////////////////////////////////////////////////////////////
		// start Timer Service
		/////////////////////////////////////////////////////////////////////////////
		TimerService = new TimerPool(1);
		TimerService.Start();
		
		thisLogger.info("Timer service is working");
				
		TC=new ThreadController();		

		/////////////////////////////////////////////////////////////////////////////
		//Start location loader
		/////////////////////////////////////////////////////////////////////////////
		VTLocationLoader VTLoader=new VTLocationLoader(TC);
		new Thread(VTLoader).start();
		
		thisLogger.info("VTLocationLoader has been started");
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

		TRMessageProducer.ShutdownMsgPublishers(true);
		
		thisLogger.info("Feed is put down as requested");
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
        if (args.length > 0 && "stop".equals(args[0])) {
        	vtlocation_zone_passdi.Stop();
            System.exit(0);
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
		vtlocation_zone_passdi=new Starter();
		vtlocation_zone_passdi.Start(prop);

		
		
		vtlocation_zone_passdi.thisLogger.info(SERVICE_NAME+ " is working now!");
	}


}
