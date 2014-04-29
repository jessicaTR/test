package com.thomsonreuters.ce.dbor.server;

import java.util.Properties;

public interface SrvControl {
	
	public abstract void Start(Properties prop);
	public abstract void Stop();
	
}
