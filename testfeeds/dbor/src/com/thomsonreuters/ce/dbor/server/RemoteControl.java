package com.thomsonreuters.ce.dbor.server;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteControl extends Remote
{
	public static final String NAME="ServerMgr";
	
	public void Shutdown(boolean isNormal) throws RemoteException;
	
}
