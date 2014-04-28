package com.pointcarbon.esb.transport.activemq;

/**
 * @author artur This component is required to run in-memory ActiveMQ broker.
 * 
 */
public class FakeProtocolOpenwire extends ProtocolOpenwire {

    @Override
    String createUrl(String hostName) {
        return "vm://localhost?broker.persistent=false";
    }

}

