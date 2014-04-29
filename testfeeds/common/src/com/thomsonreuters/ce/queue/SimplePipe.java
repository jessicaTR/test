package com.thomsonreuters.ce.queue;

import java.util.LinkedList;


public class SimplePipe<T> implements Pipe<T>{
	
	private LinkedList<T> Llist;

	private int WaitAdd;

	private int WaitGet;

	private boolean PendingClose;

	private boolean Close;
	
	private long InNum=0L;
	
	private long OutNum=0L;

	// max size of action queue
	private int max_Size;

	/**
	 * Constructor
	 * 
	 * @param maxCapacity:
	 *            max size of action queue
	 */
	public SimplePipe(int maxCapacity) {
		this.max_Size = maxCapacity;
		this.WaitAdd = 0;
		this.WaitGet = 0;
		this.Llist = new LinkedList<T>();
	}
	
	public long getInputCount()
	{
		synchronized (this) {
			return this.InNum;
		}
	}
	
	public long getOutputCount()
	{
		synchronized (this) {
			return this.OutNum;
		}		
	}

	/**
	 * get first action from action queue
	 * 
	 * @return first action object
	 */

	public T getObj(){

		T first_obj = null;

		synchronized (this) {
			if (this.Close) {
				return null;
			}
			
			while (this.Llist.isEmpty()) {
				
				this.WaitGet++;
				try {
					this.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					throw new MQException(e);
				}
				this.WaitGet--;
				
				if (this.Close ) {
					//weaked by shutdown or other get
					return null;
				}					
			}

			first_obj = this.Llist.remove(0);
			
			if ((this.PendingClose) && (this.Llist.size()==0))
			{
				this.Close=true;
				// add finish listener
				//System.out.println("Queue done");
				
				if (this.WaitGet>0)
				{
					this.notifyAll();
				}
			}
			
			if (this.WaitAdd > 0 ) {
				this.notify();
			}
			
			this.OutNum++;
		}
		
		return first_obj;
	}

	public T putObj(T objNew){
		
		if (objNew == null) {
			throw new MQException("Object can not be NULL!");
		}		

		synchronized (this) {
			
			if (this.PendingClose)
			{
				return null;
			}			

			while (this.Llist.size() == max_Size) {
				if (this.PendingClose || this.Close)
				{
					break;
				}
				
				this.WaitAdd++;
				try {
					this.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					throw new MQException(e);
				}
				this.WaitAdd--;		
			}

			if (!this.Close)
			{
				this.InNum++;
				this.Llist.add(objNew);
			}
			else
			{
				//for shutdown only
				return null;
			}

			if (this.WaitGet > 0) {
				this.notify();
			}
		}
		
		return objNew;
	}

	public void WaitForClose()
	{
		synchronized (this) {
			while(!this.Close)
			{
				try {
					this.WaitGet++;
					this.wait();
					this.WaitGet--;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	public void Shutdown(boolean isNormal) {

		if (isNormal) {
			synchronized (this) {

				this.PendingClose = true;

				if (this.Llist.isEmpty())
				{
					this.Close = true;
					//add finish listener
//					System.out.println("shutdown true: Queue done");				
				}
				
				if (this.WaitGet > 0 || this.WaitAdd>0) {
					this.notifyAll();
				}
			}
		}
		else
		{
			synchronized (this) {
				
				this.Llist.clear();
				
				this.PendingClose = true;
				this.Close=true;
				
				//add finish listener
//				System.out.println("shutdown false: Queue done");
				
				if (this.WaitGet>0 || this.WaitAdd >0)
				{
					this.notifyAll();
				}
			}			
		}		
	}	

}
