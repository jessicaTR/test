package com.pointcarbon.esb.app.example.service;

import com.pointcarbon.esb.app.example.exception.ExampleAppServiceException;
import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.esb.transport.activemq.ActiveMQDestination;
import com.pointcarbon.esb.transport.activemq.ActiveMQService;
import com.pointcarbon.esb.transport.activemq.IEsbMessageReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by artur on 20/03/14.
 */
@Service
public class Runner {
    private static Logger log = LoggerFactory.getLogger(Runner.class);

    private @Autowired ActiveMQService activeMQService;

    private @Value("${jms.source}") ActiveMQDestination jmsSource;

    private @Autowired ExampleAppService exampleAppService;

    private @Value("${threads.workers:1}") int numberOfWorkers;
    private ExecutorService exampleWorkerThreadPool;

    public void start() {
        log.info("::: STARTING RUNNER");
        exampleWorkerThreadPool = Executors.newFixedThreadPool(numberOfWorkers);

        for (int i = 0; i < numberOfWorkers; i++) {
            exampleWorkerThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (!Thread.interrupted()) {
                        readEventLoop();
                    }
                }
            });
        }

    }

    private void readEventLoop() {
        activeMQService.read(new IEsbMessageReceiver() {
            @Override
            public void receive(EsbMessage message) {
                log.info("Dequeued esbMessage={}", message);
                try {
                    exampleAppService.onEvent(message);
                } catch (ExampleAppServiceException e) {
                    log.warn("Problems while trying to parse file event for esbMessage=" + message, e);
                }
            }
        }, jmsSource, false);
    }

    public void shutdown() {
        log.info("::: STOPPING RUNNER");
        activeMQService.close();
        exampleWorkerThreadPool.shutdown();
    }
}
