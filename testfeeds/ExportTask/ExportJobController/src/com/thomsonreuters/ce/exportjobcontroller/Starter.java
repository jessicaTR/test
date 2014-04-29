package com.thomsonreuters.ce.exportjobcontroller;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.server.SrvControl;
import com.thomsonreuters.ce.exportjobcontroller.task.file.JobLoader;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.timing.TimerPool;

public class Starter implements SrvControl {

	// public
	public static final String Config_File = "../cfg/export_tasks.conf";
	public static TimerPool TimerService = null;

	// package
	public static final String SERVICE_NAME = "exportjobcontroller";

	// private
	private ThreadController tc = null;
	private static Logger thisLogger = getLogger(SERVICE_NAME);
	private static Starter jobloader = null; 

	@Override
	public void Start(Properties prop) {
		try {
			thisLogger.info("C&E Feed:" + SERVICE_NAME + " is starting up");

			// ///////////////////////////////////////////////////////////////////////////
			// Initialize database connection pool
			// ///////////////////////////////////////////////////////////////////////////
			String db_Config_file = prop.getProperty("dbpool.configuration");
			EasyConnection.configPool(db_Config_file);

			// ///////////////////////////////////////////////////////////////////////////
			// Initialize TR messaging publisher
			// ///////////////////////////////////////////////////////////////////////////
			// String messaging_config_file = prop
			// .getProperty("publisher.configuration");
			// if (messaging_config_file != null
			// && !messaging_config_file.equals("")) {
			// try {
			// TRMessageProducer
			// .InitialMsgPublishers(messaging_config_file);
			// thisLogger
			// .info("Connection to messaging server has been established!");
			// } catch (Exception e) {
			// thisLogger.warn("Failed to connect alerting server: ", e);
			// }
			// }

			// ///////////////////////////////////////////////////////////////////////////
			// start Timer Service
			// ///////////////////////////////////////////////////////////////////////////
			int Timer_Thread_Num = Integer.parseInt(prop
					.getProperty("timer_threadnum"));
			TimerService = new TimerPool(Timer_Thread_Num);
			TimerService.Start();

			thisLogger.info("Timer pool has been started with "
					+ String.valueOf(Timer_Thread_Num) + " thread(s)");

			tc = new ThreadController();
			JobLoader jobLoader = new JobLoader(tc, prop, thisLogger);
			new Thread(jobLoader).start();
			thisLogger.info("Task: " + SERVICE_NAME + " has been started");
		} catch (Exception e) {
			thisLogger.error("Unexpected exception", e);
		}
	}

	@Override
	public void Stop() {
		// Normal shutdown

		TimerService.Stop();
		thisLogger.info("Timer service is down");

		tc.Shutdown();
		thisLogger.info("Shutdown signal is sent");

		tc.WaitToDone();
		thisLogger.info("All threads are done");

		thisLogger.info("Feed is put down as requested");
	}

	public static synchronized Logger getLogger(String loggerName) {
		try {
			Properties prop = new Properties();
			FileInputStream fis = new FileInputStream(Config_File);
			prop.load(fis);
			String loggingCfg = prop.getProperty("logging.configuration");
			PropertyConfigurator.configure(loggingCfg);
			Logger logger = Logger.getLogger(loggerName);
			logger.info("Logger:[" + loggerName + "] is working");
			return logger;
		} catch (Exception e) {
			System.out.println("Can't read configuration file: " + Config_File);
			return null;
		}
	}

	public static synchronized String getTempFolder(String name) {
		try {
			Properties prop = new Properties();
			FileInputStream fis = new FileInputStream(Config_File);
			prop.load(fis);
			String tmp = prop.getProperty(name + ".tempfolder");
			tmp = FileUtilities.GetAbsolutePathFromEnv(tmp);
			return tmp;
		} catch (Exception e) {
			e.printStackTrace();
			return FileUtilities.GetAbsolutePathFromEnv("${APPTEMP}");
		}
	}

	public static void main(String[] args) {
        if (args.length > 0 && "stop".equals(args[0])) {
    		jobloader.Stop();
            System.exit(0);
        }

		// ///////////////////////////////////////////////////////////////////////////
		// Read config file into prop object
		// ///////////////////////////////////////////////////////////////////////////
		Properties prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(Config_File);
			prop.load(fis);
		} catch (Exception e) {
			System.out.println("Can't read configuration file: " + Config_File);
		}

		// Start service
		jobloader = new Starter();
		jobloader.Start(prop);

		thisLogger.info(SERVICE_NAME + " is working now!");
	}

}
