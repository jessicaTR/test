package com.thomsonreuters.ce.dbor.messaging;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Iterator;


import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.queue.MagicPipe;
import com.thomsonreuters.esp.messaging.TrmlConnection;
import com.thomsonreuters.esp.messaging.TrmlMessage;
import com.thomsonreuters.esp.messaging.TrmlMessageManager;
import com.thomsonreuters.esp.messaging.TrmlProducerSession;
import com.thomsonreuters.esp.messaging.log.ITrmlLogger;
import com.thomsonreuters.esp.messaging.settings.TrmlConnectionFactoryResolverSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlConnectionSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlDestinationResolverSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlDestinationSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlDestinationTypeSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlDynamicConnectionFactoryResolverSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlDynamicConnectionFactorySettings;
import com.thomsonreuters.esp.messaging.settings.TrmlDynamicDestinationResolverSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlDynamicDestinationSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlJmsDeliveryModeSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlJmsPropertySettings;
import com.thomsonreuters.esp.messaging.settings.TrmlLoggerSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlLogging;
import com.thomsonreuters.esp.messaging.settings.TrmlMessageAcknowledgeModeSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlMessagePropertySettings;
import com.thomsonreuters.esp.messaging.settings.TrmlMessageTraceTypeSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlProducerSessionSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlPropertySetting;
import com.thomsonreuters.esp.messaging.settings.TrmlServerInfoSettings;
import com.thomsonreuters.esp.messaging.settings.TrmlTibcoEmsPropertySettings;
import com.thomsonreuters.esp.messaging.settings.TrmlByteSerializerSettings;
import com.thomsonreuters.ce.exception.SystemException;


public class TRMessageProducer {
	
	private MagicPipe<String> MQ ; //= new MagicPipe<String>(1024, 2048,1024, 50, Starter.TempFolder);
	
	private static HashMap<String, MagicPipe<String>> MQ_Map=new HashMap<String, MagicPipe<String>>();
	
	public static void InitialMsgPublishers(String cfg)
	{
		try {
			FileInputStream fis = new FileInputStream(cfg);
			Properties prop = new Properties();
			prop.load(fis);

			StringTokenizer ProduceList = new StringTokenizer(prop
					.getProperty("Producer_connections"), ",", false);
			while (ProduceList.hasMoreTokens()) {
				String ProducerName = ProduceList.nextToken().trim();
				
				
		        ////////////////////////////////////
		        // create logging settings
				////////////////////////////////////
				String logging_priority = FileUtilities.GetAbsolutePathFromEnv(prop.getProperty(ProducerName + ".logging.priority"));
				TrmlPropertySetting ls1 = new TrmlPropertySetting();
				ls1.setName("priority");
				ls1.setValue(logging_priority);
				
				String logging_filepath = prop.getProperty(ProducerName + ".logging.filepath");
				TrmlPropertySetting ls2 = new TrmlPropertySetting();
				ls2.setName("filepath");
				ls2.setValue(logging_filepath);				
				
				String logging_maxfilelength = prop.getProperty(ProducerName + ".logging.maxfilelength");
				TrmlPropertySetting ls3 = new TrmlPropertySetting();
				ls3.setName("maxfilelength");
				ls3.setValue(logging_maxfilelength);
				
				ArrayList<TrmlPropertySetting> trmlLogSettingList = 
					new ArrayList<TrmlPropertySetting>();
				trmlLogSettingList.add(ls1);
				trmlLogSettingList.add(ls2);
				trmlLogSettingList.add(ls3);
				
				TrmlLoggerSettings ls = new TrmlLoggerSettings();
				ls.setTypeName("com.thomsonreuters.esp.messaging.log.TrmlFlatFileLogger");	
				ls.setProperties(trmlLogSettingList);
				
				TrmlLogging trmllog = new TrmlLogging();
				trmllog.setLoggerSettings(ls);
				ITrmlLogger logger = TrmlMessageManager.createLogger(trmllog);
		        TrmlMessageManager.setLogger(logger);
		        TrmlMessageManager.setLoggingEnabled(true);
		        
		        ////////////////////////////////////
		        // create connection settings
				////////////////////////////////////
				
				//set and set ConnectionFactoryResolver
		        final TrmlConnectionSettings cnSettings = new TrmlConnectionSettings();
		        TrmlConnectionFactoryResolverSettings cnFactResolverSettings = new TrmlDynamicConnectionFactoryResolverSettings();
		        cnFactResolverSettings.setTypeName("com.thomsonreuters.esp.messaging.TrmlDynamicConnectionFactoryResolver");
		        cnSettings.setConnectionFactoryResolver(cnFactResolverSettings);
		        
		        //set and set DestinationResolver 
		        TrmlDestinationResolverSettings destResolverSettings = new TrmlDynamicDestinationResolverSettings();
		        destResolverSettings.setTypeName("com.thomsonreuters.esp.messaging.TrmlDynamicDestinationResolver");
		        cnSettings.setDestinationResolver(destResolverSettings);
		        
		        //set HeartbeatInterval
		        cnSettings.setHeartbeatInterval(10);
		        
		        //set RecoveryIntervalInSeconds
		        cnSettings.setRecoveryIntervalInSeconds(5);
		        
		        //set MaxRecoveryTimeInSeconds
		        cnSettings.setMaxRecoveryTimeInSeconds(600); 
				
				String ServerUrl=prop.getProperty(ProducerName + ".ServerUrl");
				String UserName=prop.getProperty(ProducerName + ".UserName");
				String Password=prop.getProperty(ProducerName + ".Password");
				
				 //config and set connection factory
		        TrmlDynamicConnectionFactorySettings cfSettings = new TrmlDynamicConnectionFactorySettings();
		        TrmlServerInfoSettings serverInfoSettings1 = new TrmlServerInfoSettings();
		        serverInfoSettings1.setServerUrl(ServerUrl);
		        serverInfoSettings1.setUserName(UserName);
		        serverInfoSettings1.setPassword(Password);
		        serverInfoSettings1.setRelativePriority(1); 
				
		        
				String topics=prop.getProperty(ProducerName + ".topics");
				ArrayList<TrmlServerInfoSettings> TrmlServerInfoSettingsList = 
					new ArrayList<TrmlServerInfoSettings>();
				TrmlServerInfoSettingsList.add(serverInfoSettings1);
		        cfSettings.setServers(TrmlServerInfoSettingsList);
		        cnSettings.setConnectionFactory(cfSettings);
		        
		        //config and set destination
		        TrmlDynamicDestinationSettings dest1Settings = new TrmlDynamicDestinationSettings();
		        dest1Settings.setId("Destination1");
		        dest1Settings.setName(topics);
		        dest1Settings.setDestinationType(TrmlDestinationTypeSettings.TOPIC); //topic

		        /*
		        TrmlDynamicDestinationSettings dest2Settings = new TrmlDynamicDestinationSettings();
		        dest2Settings.setId("Destination2");
		        dest2Settings.setName("TR.OCP01QS.CANDEDATA.1");
		        dest2Settings.setDestinationType(TrmlDestinationTypeSettings.QUEUE); //queue
		        */
				
				
				ArrayList<TrmlDestinationSettings> trmldestinationsettingslist = 
					new ArrayList<TrmlDestinationSettings>();
				trmldestinationsettingslist.add(dest1Settings);
				//trmldestinationsettingslist.add(dest2Settings);
				 
		        cnSettings.setDestinations(trmldestinationsettingslist);
		        //config and set producer sessions
		        TrmlProducerSessionSettings psSettings1 = new TrmlProducerSessionSettings();
		        psSettings1.setName("PS1");
		        psSettings1.setDefaultDestinationId("Destination1");
		        
		        TrmlMessagePropertySettings mpSettings1 = new TrmlMessagePropertySettings();

		        TrmlJmsPropertySettings jmsp1 = new TrmlJmsPropertySettings();
		        jmsp1.setDeliveryMode(TrmlJmsDeliveryModeSettings.PERSISTENT);
		        jmsp1.setTimeToLive(0L);
		        jmsp1.setPriority(4);
		        mpSettings1.setJmsProperties(jmsp1);
		        
		        TrmlTibcoEmsPropertySettings tp1 = new TrmlTibcoEmsPropertySettings();
		        tp1.setTraceType(TrmlMessageTraceTypeSettings.BASIC_TRACE);
		        mpSettings1.setTibcoEmsProperties(tp1);
		        
		        psSettings1.setDefaultMessageProperties(mpSettings1);

		        //set and config byte serializer
		        TrmlByteSerializerSettings TSerializerSettings = new TrmlByteSerializerSettings();
		        TSerializerSettings.setSerializerTypeName("com.thomsonreuters.esp.messaging.TrmlByteSerializer");		        
		        TSerializerSettings.setCompressionEnabled(false);
		        //TSerializerSettings.setCompressionThreshold(2048);
		        
		        psSettings1.setDefaultSerializer(TSerializerSettings);
				
		        //set Message Acknowledge Mode
		        psSettings1.setMessageAcknowledgeMode(TrmlMessageAcknowledgeModeSettings.AUTO_ACKNOWLEDGE);

				ArrayList<TrmlProducerSessionSettings> TProducerSessionSettingsList = 
					new ArrayList<TrmlProducerSessionSettings>();
				TProducerSessionSettingsList.add(psSettings1);

		        cnSettings.setProducerSessions(TProducerSessionSettingsList);
		        
		        TrmlConnection connection = TrmlMessageManager.createConnection(cnSettings);
		        
		        String Queue_Temp=FileUtilities.GetAbsolutePathFromEnv(prop.getProperty(ProducerName + ".queue_temp"));
				int ThreadNum=Integer.parseInt(prop.getProperty(ProducerName + ".ThreadNum"));
				
				MagicPipe<String> MQ = new MagicPipe<String>(1024, 2048,1024, 50, Queue_Temp);
				
				Counter thisCounter=new Counter(ThreadNum);
				
				for (int i=0 ; i<ThreadNum ; i++)
				{
					Publisher pb= new Publisher(connection,MQ, thisCounter);
					new Thread(pb).start();
				}
				
				MQ_Map.put(ProducerName, MQ);
				
			}
		} catch (Exception ex) {
			throw new SystemException("Initialize messaging producer failed!", ex);
		}
	}
	
	public static void ShutdownMsgPublishers(boolean isNormal)
	{
		Iterator<MagicPipe<String>> iterator = MQ_Map.values().iterator();
		
		while(iterator.hasNext()) {
			MagicPipe<String> MQ=iterator.next();
			MQ.Shutdown(isNormal);
			MQ.WaitForClose();
		}
	}
	
	public TRMessageProducer(String name)
	{		
		this.MQ=MQ_Map.get(name);
		
		if (this.MQ==null)
		{
			throw new SystemException("Can not find messaging producer: "+name);
		}
	}
	
	public void Send(String payload)
	{
		MQ.putObj(payload);
	}
	
	static class Publisher implements Runnable
	{
		TrmlConnection connection;
		MagicPipe<String> MQ;
		Counter thisCounter;
		
		Publisher(TrmlConnection conn, MagicPipe<String> mq, Counter counter)
		{
			this.connection=conn;
			this.MQ=mq;
			this.thisCounter=counter;
		}
		
		public void run()
		{

			TrmlProducerSession producerSession=null;
			
			try {
				producerSession = connection.createProducerSession();

				String payload;
				while ((payload=MQ.getObj())!=null)
				{
					TrmlMessage message = producerSession.createMessage(payload);
					producerSession.send(message);					
				}				

			} catch (Exception e) {
				throw new SystemException("Unknown JMS exception!",e);
			} 
			finally
			{
				if (producerSession!=null)
				{
					producerSession.dispose();
				}
				
				synchronized(this.MQ)
				{
					 thisCounter.Value=thisCounter.Value-1;
					    
					    if (thisCounter.Value==0)
					    {
						this.connection.dispose();
						this.MQ.notify();

					    }
				}
			}
		
		}
	}
}

class Counter {
    
    public int Value;
    
    public Counter (int value)
    {
	Value=value;
    }

}

