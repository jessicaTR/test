package com.pointcarbon.esb.app.example.integrationtests;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.pointcarbon.esb.app.example.TestsConstants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.pointcarbon.esb.app.example.service.Runner;
import com.pointcarbon.esb.app.example.beans.MyValue;
import com.pointcarbon.esb.app.example.dao.ExampleDao;
import com.pointcarbon.esb.commons.beans.EsbMessage;

/**
 * Component responsible for providing proper execution environment for test suite.
 * 
 */
@Component
public class IntegrationTestContextManager {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTestContextManager.class);

    private @Autowired Runner runner;
    private @Autowired ExampleDao exampleDao;

    @PostConstruct
    public void initSuiteExecEnv() throws Exception {
        log.info("[TEST]: STARTING THE RUNNER SERVICE");
        System.setProperty("user.timezone", "UTC");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        DateTimeZone.setDefault(DateTimeZone.forID("UTC"));
        mockResponseForExampleDao();
        runner.start();
    }

    private void mockResponseForExampleDao() {
        when(exampleDao.fetchValueFromDb(any(String.class))).thenReturn(TestsConstants.fetchMyValues());
    }

    @PreDestroy
    public void preDestroyOperations() throws Exception {
        log.info("[TEST]: SHUTTING DOWN THE RUNNER SERVICE");
        runner.shutdown();
    }

}
