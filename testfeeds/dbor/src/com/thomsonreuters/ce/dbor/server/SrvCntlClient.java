package com.thomsonreuters.ce.dbor.server;

import java.rmi.Naming;

public class SrvCntlClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String host_name=args[0];
		int RMIport=Integer.parseInt(args[1]);
		String ServiceName=args[2];
		String mode=args[3];
		
		boolean isNormal=true;
		
		if (mode.equals("false"))
		{
			isNormal=false;
		}
		
		String RMIurl="//"+host_name+":"+String.valueOf(RMIport)+"/"+ServiceName;
		try {			
			RemoteControl RC=(RemoteControl)Naming.lookup(RMIurl);
			RC.Shutdown(isNormal);
		} catch (Exception e) {
			System.out.println("Failed to send shutdown signal!");
			e.printStackTrace();
		}
	}

}
