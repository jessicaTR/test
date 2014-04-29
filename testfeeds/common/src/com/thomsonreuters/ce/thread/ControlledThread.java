package com.thomsonreuters.ce.thread;

public abstract class ControlledThread implements Runnable {
	
	protected ThreadController TC;
	
	public ControlledThread(ThreadController tc)
	{
		this.TC=tc;
	}
	
	public boolean IsShuttingDown()
	{
		return this.TC.IsShuttingDown();
	}	
	
	public void run()
	{
		if (IsShuttingDown())
		{
			return;
		}	
		
		TC.IncreaseThread();
		
		try {
			ControlledProcess();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			TC.DecreaseThread();
		}
		
	}
	
	public abstract void ControlledProcess();	

}
