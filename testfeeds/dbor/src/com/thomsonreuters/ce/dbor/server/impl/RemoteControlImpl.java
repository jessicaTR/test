package com.thomsonreuters.ce.dbor.server.impl;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import com.thomsonreuters.ce.dbor.server.RemoteControl;
import com.thomsonreuters.ce.dbor.server.SrvControl;

public class RemoteControlImpl extends UnicastRemoteObject implements RemoteControl {

	private SrvControl SC;
	
	public RemoteControlImpl(SrvControl sc) throws RemoteException
	{
		this.SC=sc;
	}
	
	public void Shutdown(boolean isNormal) throws RemoteException
	{
		if (isNormal)
		{
			this.SC.Stop();
			unexportObject(this,true);
		}
		else
		{
			System.out.println("Service is killed!");
			System.exit(0);
		}	
		
	}

}
