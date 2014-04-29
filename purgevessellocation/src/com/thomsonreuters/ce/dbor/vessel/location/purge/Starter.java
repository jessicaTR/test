package com.thomsonreuters.ce.dbor.vessel.location.purge;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.server.SrvControl;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.timing.TimerPool;


public class Starter implements SrvControl {
	/////////////////////////////////////////////////////////////////////////////////
	// config file path
	/////////////////////////////////////////////////////////////////////////////////
	private static final String Config_File = "../cfg/app.conf";
	
	public static TimerPool TimerService=null;
	
	public ThreadController TC=null;

	public static final String SERVICE_NAME="vessellocationpurge";
	
	public static Logger vlplogger=null;
	
	public static Starter purgelocation=null;
	
	public void Start(Properties prop)
	{

		/////////////////////////////////////////////////////////////////////////////
		// Initialize logging
		/////////////////////////////////////////////////////////////////////////////
		String loggingCfg = prop.getProperty("logging.configuration");
		PropertyConfigurator.configure(loggingCfg);
		vlplogger=Logger.getLogger(SERVICE_NAME);
		vlplogger.info("Logging is working");
		
		
		/////////////////////////////////////////////////////////////////////////////
		//Initialize database connection pool
		/////////////////////////////////////////////////////////////////////////////
		String db_Config_file = prop.getProperty("dbpool.configuration");
		EasyConnection.configPool(db_Config_file);
		vlplogger.info("Database connection pool is working");
		
		/////////////////////////////////////////////////////////////////////////////
		// start Timer Service
		/////////////////////////////////////////////////////////////////////////////
		TimerService = new TimerPool(1);
		TimerService.Start();
		
		vlplogger.info("Timer service is working");
				
		TC=new ThreadController();		
		
		/////////////////////////////////////////////////////////////////////////////
		//Start Incremental SDI
		/////////////////////////////////////////////////////////////////////////////

		char scheduletype = prop.getProperty("scheduletype").toCharArray()[0];
		String scheduletime = prop.getProperty("time");
		int threadnum = Integer.parseInt(prop.getProperty("threadnum"));

		LocationPurge LP=new LocationPurge(TC,scheduletype,scheduletime,threadnum);
		new Thread(LP).start();
				
	}
	
	public void Stop()
	{
		//Normal shutdown

		TimerService.Stop();
		vlplogger.info("Timer service is down");

		TC.Shutdown();
		vlplogger.info("Shutdown signal is sent");

		TC.WaitToDone();
		vlplogger.info("All threads are done");

		EasyConnection.CloseAllPool();
		vlplogger.info("DBPool is closed");

		vlplogger.info("Feed is put down as requested");
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
        if (args.length > 0 && "stop".equals(args[0])) {
        	purgelocation.Stop();
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
		purgelocation=new Starter();
		purgelocation.Start(prop);
		
		Starter.vlplogger.info(SERVICE_NAME+ " is working now!");
	}



}
