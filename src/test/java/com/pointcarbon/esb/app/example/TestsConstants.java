package com.pointcarbon.esb.app.example;

import com.pointcarbon.esb.app.example.beans.MyValue;
import com.pointcarbon.esb.commons.beans.EsbMessage;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by artur on 21/03/14.
 */
public class TestsConstants {
    public static List<MyValue> fetchMyValues() {
        List<MyValue> result = new ArrayList<MyValue>();
        MyValue mv1 = new MyValue();
        mv1.setId(1);
        mv1.setName("my_value_1");
        result.add(mv1);
        MyValue mv2 = new MyValue();
        mv2.setId(2);
        mv2.setName("my_value_2");
        result.add(mv2);

        return result;
    }

    public static EsbMessage getMessage() {

        EsbMessage message = new EsbMessage();
        DateTime ts = new DateTime(DateTimeZone.UTC);
        message.setTimeStamp(ts);
        message.setParameterValue("my_special_value", "my_request_value1");

        return message;
    }
}
