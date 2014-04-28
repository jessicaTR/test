package com.pointcarbon.esb.app.example.service;

import com.pointcarbon.esb.app.example.beans.MyValue;
import com.pointcarbon.esb.app.example.dao.ExampleDao;
import com.pointcarbon.esb.app.example.exception.ExampleAppServiceException;
import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.esb.transport.activemq.ActiveMQDestination;
import com.pointcarbon.esb.transport.activemq.ActiveMQService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by artur on 20/03/14.
 */
@Service
public class ExampleAppService {
    private static Logger log = LoggerFactory.getLogger(ExampleAppService.class);

    private @Autowired ActiveMQService activeMQService;
    private @Value("${jms.destination}") ActiveMQDestination jmsDestination;

    private @Autowired ExampleDao dao;

    public void onEvent(EsbMessage message) throws ExampleAppServiceException {
        log.info("Start processing message={}", message);
        String myRequestValue = message.getParameterValue("my_special_value");
        if (myRequestValue == null) {
            throw new ExampleAppServiceException("My request value was not found in consumed message");
        }

        List<MyValue> result = dao.fetchValueFromDb(myRequestValue);

        try {
            for (MyValue myValue: result) {
                log.info("Sending notification for myValueId={} and myValueName={}", myValue.getId(), myValue.getName());
                notifyAboutResult(myValue.getName(), message);
            }

        } catch (Exception e) {
            throw new ExampleAppServiceException("Could not publish notification", e);
        }
    }

    void notifyAboutResult(String result, EsbMessage sourceMsg) throws ExecutionException, InterruptedException {
        EsbMessage notification = new EsbMessage();
        notification.setCorrelatedMsgId(sourceMsg.getMsgId());
        notification.setParameterValue("fetched_result", result);

        activeMQService.writeMessage(jmsDestination, notification).get();
    }
}
