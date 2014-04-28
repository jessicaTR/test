package com.pointcarbon.esb.app.example;

import java.io.FileInputStream;
import java.util.Properties;

import com.pointcarbon.esb.app.example.service.Runner;
import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.esb.transport.activemq.ActiveMQDestination;
import com.pointcarbon.esb.transport.activemq.ActiveMQService;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by artur on 20/03/14.
 */
public class Main {
    private static ClassPathXmlApplicationContext context;

    public static void main(String[] args) throws Exception {
         
        if (args.length > 0 && "stop".equals(args[0])) {
            final Runner runner = context.getBean(Runner.class);
            runner.shutdown();
            context.close();
            System.exit(0);
        } else {
            context = new ClassPathXmlApplicationContext("example-app-spring.xml");
            final Runner runner = context.getBean(Runner.class);
            context.start();
            runner.start();
            
            //Create a sample message
            String path = Main.class.getClassLoader().getResource("example-app.properties").getPath();
            FileInputStream reader = new FileInputStream(path);
            Properties p = new Properties();
            p.load(reader);
            
            ActiveMQDestination source = new ActiveMQDestination(p.getProperty("jms.source"));
            ActiveMQService activeMQService = context.getBean(ActiveMQService.class);
            
            EsbMessage message = new EsbMessage();
            DateTime ts = new DateTime(DateTimeZone.UTC);
            message.setTimeStamp(ts);
            message.setParameterValue("my_special_value", "my_request_value1");

            activeMQService.writeMessage(source, message).get();
            Thread.sleep(1500);
        }
    }

}
