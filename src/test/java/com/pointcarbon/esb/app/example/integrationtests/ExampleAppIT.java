package com.pointcarbon.esb.app.example.integrationtests;

import static junit.framework.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.pointcarbon.esb.app.example.TestsConstants;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.esb.commons.util.StringUtil;
import com.pointcarbon.esb.transport.activemq.ActiveMQDestination;
import com.pointcarbon.esb.transport.activemq.ActiveMQService;
import com.pointcarbon.esb.transport.activemq.IEsbMessageReceiver;

@Test
@ContextConfiguration("classpath:example-app-spring-integrationtest.xml")
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
public class ExampleAppIT extends AbstractTestNGSpringContextTests {
    private static final Logger log = LoggerFactory.getLogger(ExampleAppIT.class);

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    private @Value("${jms.source}") ActiveMQDestination source;
    private @Value("${jms.destination}") ActiveMQDestination destination;
    private @Autowired ActiveMQService activeMQService;

    @Test
    public void shouldForwardMessage() throws Exception {
        log.info("[TEST]: SENDING TEST MSG FWD START");
        final EsbMessage message = TestsConstants.getMessage();
        final int numberOfNotifications = TestsConstants.fetchMyValues().size();
        final CountDownLatch latch = new CountDownLatch(numberOfNotifications);
        final AtomicInteger counter = new AtomicInteger(0);

        threadPool.execute(new Runnable() {
            public void run() {
                readFromActiveMq(destination, latch, counter, StringUtil.toHexString(message.getMsgId()));
            }
        });

        activeMQService.writeMessage(source, message).get();
        log.info("Sleeping in order to let the application consume the message");
        Thread.sleep(1500);
        log.info("Proceed...");
        log.info("[TEST]: SENDING TEST MSG FWD DONE");
        latch.await();
        log.info("Done reading. {}", counter.intValue());
        assertEquals(numberOfNotifications, counter.intValue());
    }

    @Ignore
    private void readFromActiveMq(final ActiveMQDestination destination, final CountDownLatch latch,
            final AtomicInteger counter, final String inboundMessageId) {
        activeMQService.read(new IEsbMessageReceiver() {
            @Override
            public void receive(EsbMessage esbMessage) throws Exception {
                try {
                    log.info("Read from={}. Params={}", destination, esbMessage);
                    assertEquals(inboundMessageId, StringUtil.toHexString(esbMessage.getCorrelatedMsgId()));
                    counter.incrementAndGet();
                    latch.countDown();
                } catch (Exception e) {
                    log.error("Error while reading from destination", e);
                }
            }
        }, destination, null, false);
    }

}
