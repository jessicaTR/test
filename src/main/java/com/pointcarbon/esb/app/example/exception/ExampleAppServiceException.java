package com.pointcarbon.esb.app.example.exception;

/**
 * Created by artur on 20/03/14.
 */
public class ExampleAppServiceException extends Exception {
    public ExampleAppServiceException (String message) {
        super(message);
    }

    public ExampleAppServiceException (String message, Throwable t) {
        super(message, t);
    }
}
