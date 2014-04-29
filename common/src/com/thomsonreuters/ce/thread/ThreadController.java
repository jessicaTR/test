package com.thomsonreuters.ce.thread;

public class ThreadController {
	
	private int ThreadCount=0;
	
	private boolean IsShuttingDown=false;
	
	public void Shutdown()
	{
		synchronized(this)
		{
			this.IsShuttingDown=true;
		}
	}
	
	public boolean IsShuttingDown()
	{
		synchronized(this)
		{
			return this.IsShuttingDown;
		}
		
	}
	
	public void IncreaseThread()
	{
		synchronized(this)
		{
			this.ThreadCount++;
		}
	}
	
	public void DecreaseThread()
	{
		synchronized(this)
		{
			this.ThreadCount--;
			
			if (this.ThreadCount==0)
			{
				this.notify();
			}
		}
	}
	
	public void WaitToDone()
	{
		synchronized(this)
		{
			while(this.ThreadCount>0)
			{
				try {
					this.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
