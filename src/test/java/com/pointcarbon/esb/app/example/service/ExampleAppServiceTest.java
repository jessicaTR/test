package com.pointcarbon.esb.app.example.service;

import com.pointcarbon.esb.app.example.TestsConstants;
import com.pointcarbon.esb.app.example.dao.ExampleDao;
import com.pointcarbon.esb.app.example.exception.ExampleAppServiceException;
import com.pointcarbon.esb.commons.beans.EsbMessage;
import com.pointcarbon.esb.transport.activemq.ActiveMQDestination;
import com.pointcarbon.esb.transport.activemq.ActiveMQService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.Future;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

/**
 * Created by artur on 21/03/14.
 */
public class ExampleAppServiceTest {

    @InjectMocks
    private ExampleAppService exampleAppService;

    @Mock
    private ExampleDao exampleDao;
    @Mock
    private ActiveMQService activeMQService;

    @Mock
    private Future activeMqServiceResponse;


    @BeforeClass
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        mockResponseForExampleDao();
        mockResponseForActiveMQService();
    }

    private void mockResponseForExampleDao() {
        when(exampleDao.fetchValueFromDb(any(String.class))).thenReturn(TestsConstants.fetchMyValues());
    }

    private void mockResponseForActiveMQService() throws Exception {
        when(activeMqServiceResponse.get()).thenReturn(Void.class);
        when(activeMQService.writeMessage(any(ActiveMQDestination.class), any(EsbMessage.class))).thenReturn(activeMqServiceResponse);
    }

    @Test
    public void send_message_should_produce_notifications() throws Exception {
        EsbMessage inMessage = TestsConstants.getMessage();
        exampleAppService.onEvent(inMessage);

        verify(exampleDao).fetchValueFromDb(any(String.class));
        verify(activeMQService, times(2)).writeMessage(any(ActiveMQDestination.class), any(EsbMessage.class));
    }

    @Test(expectedExceptions = ExampleAppServiceException.class)
    public void send_wrong_message_should_throw_exception() throws Exception {
        EsbMessage inMessage = TestsConstants.getMessage();
        inMessage.removeParameter("my_special_value");
        exampleAppService.onEvent(inMessage);

        verify(exampleDao).fetchValueFromDb(any(String.class));
    }
}
