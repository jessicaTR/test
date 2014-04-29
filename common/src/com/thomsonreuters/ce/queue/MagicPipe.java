package com.thomsonreuters.ce.queue;


import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.io.Serializable;
import java.io.RandomAccessFile;

import java.util.LinkedList;
import java.util.List;

import com.thomsonreuters.ce.io.AdvancedBIS;
import com.thomsonreuters.ce.io.ObjectReadStream;
import com.thomsonreuters.ce.io.ObjectWriteStream;


public class MagicPipe<T extends Serializable> implements Runnable,Pipe<T> {

	private LinkedList<T> InputCache;

	private LinkedList<T> OutputCache;

	private boolean Mthread;

	private String tempDir;

	private File tempFile = null;
	
	private RandomAccessFile DiskCache = null;
	
	private FileInputStream Fin =null;
	
	private long DiskCacheSize = 0L;

	private int CacheMinSize;

	private int CacheMaxSize;

	private int UpdateBufferSize;

	private float DiskCacheDirtyRatio;

	private boolean PendingClose;

	private boolean Close;

	private long InputNum = 0L;

	private long OutputNum = 0L;

	private int getWaitNum = 0;
	
	private long CacheReadPosition = 0L;

	public MagicPipe(int CacheMin_Size, int CacheMax_Size,
			int UpdateBuffer_Size, int DiskCacheDirty_Ratio, String TempDir) {

		this.Close = false;
		this.PendingClose = false;

		this.CacheMinSize = CacheMin_Size;
		this.CacheMaxSize = CacheMax_Size;
		this.UpdateBufferSize = UpdateBuffer_Size * 1024;
		this.DiskCacheDirtyRatio = DiskCacheDirty_Ratio / 100F;
		this.tempDir = TempDir;

		this.InputCache = new LinkedList<T>();
		this.OutputCache = new LinkedList<T>();
		this.Mthread = false;

	}
	
	public MagicPipe() {

		this.Close = false;
		this.PendingClose = false;

		this.CacheMinSize = 1000;
		this.CacheMaxSize = 2000;
		this.UpdateBufferSize = 1048576;
		this.DiskCacheDirtyRatio = 0.5F;
		this.tempDir = System.getProperty("java.io.tmpdir");

		this.InputCache = new LinkedList<T>();
		this.OutputCache = new LinkedList<T>();
		this.Mthread = false;

	}	

	public long getInputCount()
	{
		synchronized (this) {
			return this.InputNum;
		}
	}
	
	public long getOutputCount()
	{
		synchronized (this) {
			return this.OutputNum;
		}		
	}
	
	public T putObj(T objNew) {
		
		if (objNew == null) {
			throw new MQException("Message can not be NULL!");
		}
		
		synchronized (this) {

			if (this.PendingClose) {
				return null;
			}

			// wake to maintain
			if ((this.InputCache.size() > this.CacheMaxSize)
					|| (this.getWaitNum > 0)) {
				if (!this.Mthread) {
					this.Mthread = true;
					new Thread(this).start();
				}
			}

			this.InputCache.add(objNew);
			InputNum++;
		}
		
		return objNew;
	}

	public T getObj() {

		T FirstObject = null;

		synchronized (this) {

			if (this.Close) {
				return null;
			}

			try {
				// need to be filled back
				if ((this.OutputCache.size() < this.CacheMinSize)
						&& (this.InputCache.size() > 0 || this.DiskCacheSize > 0)) {
					if (!this.Mthread) {
						this.Mthread = true;
						new Thread(this).start();
					}
				}

				while (this.OutputCache.isEmpty())
				{
					if (this.InputCache.size() > 0 || this.DiskCacheSize > 0) {
						if (!this.Mthread) {
							this.Mthread = true;
							new Thread(this).start();
						}
					} 						
					
					getWaitNum++;
					this.wait();
					getWaitNum--;
					
					if (this.Close) {
						return null;
					}
				}

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				throw new MQException(e);
			}

			FirstObject = this.OutputCache.remove(0);

			// close
			if (this.PendingClose
					&& (this.InputCache.isEmpty()
							&& this.OutputCache.isEmpty() && this.DiskCacheSize == 0)) {
				this.Close = true;
				//add finish listener
				//System.out.println("Queue done");
				
				if (this.getWaitNum > 0)
				{
					this.notifyAll();
				}
			}

			OutputNum++;

		}

		return FirstObject;
	}
	
	public void WaitForClose()
	{
		synchronized (this) {
			while(!this.Close)
			{
				try {
					getWaitNum++;
					this.wait();
					getWaitNum--;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void run() {

		synchronized (this) {

			try {
				// week up by add
				if (this.InputCache.size() > 0) {

					if (this.DiskCacheSize > 0) {

						// move inputqueue to disk
						this.writeObjectsToDiskCache(this.InputCache);
						this.InputCache.clear();

					} else {
						// move inputqueue to outputqueue
						int fillsize = this.CacheMaxSize
								- this.OutputCache.size();
						if (this.InputCache.size() <= fillsize) {
							this.OutputCache.addAll(this.InputCache);
							this.InputCache.clear();
						} else {
							List<T> FillList = this.InputCache
									.subList(0, fillsize);
							this.OutputCache.addAll(FillList);
							FillList.clear();

							this.writeObjectsToDiskCache(this.InputCache);
							this.InputCache.clear();
						}
					}
				}			

				
				if (this.OutputCache.size() < this.CacheMinSize) {					

					if (this.DiskCacheSize > 0) {
						// move disk to outputqueue
						int fillsize = this.CacheMaxSize
								- this.OutputCache.size();

						if (this.DiskCacheSize <= fillsize) {
							List<T> FillList = this
									.getObjectsFromDiskCache(this.DiskCacheSize);
							this.OutputCache.addAll(FillList);
							FillList.clear();
														
						} else {

							List<T> FillList = this
									.getObjectsFromDiskCache(fillsize);
							this.OutputCache.addAll(FillList);
							FillList.clear();
						}

					}

				}

				if (this.DiskCacheSize != 0) {
					if (this.CacheReadPosition > (this.tempFile.length() * this.DiskCacheDirtyRatio))
					{
						updateDiskCache();
					}
				}
				
				if (this.getWaitNum > 0	&& this.OutputCache.size()>0) {
					this.notifyAll();
				}				

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				throw new MQException("Temporary file is not found!", e);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				throw new MQException("Temporary file IO Exception!", e);
			} catch (ClassNotFoundException e) {
				throw new MQException("Object definition class is not found!",
						e);
			}

			this.Mthread = false;
		}
	}

	
	public void Shutdown(boolean isNormal)
	{
		if (isNormal)
		{
			synchronized (this) {
				this.PendingClose = true;
				
				if (this.OutputCache.isEmpty() && this.InputCache.isEmpty() && this.DiskCacheSize == 0)
				{
					this.Close=true;
					//add finish listener
					//System.out.println("Queue done");
				}

				if (this.getWaitNum > 0) {
					this.notifyAll();
				}
			}			
		}
		else
		{
			synchronized (this) {

				this.InputCache.clear();
				this.OutputCache.clear();			
				
				if (this.DiskCacheSize > 0) {
					try {
						this.Fin.close();
						this.DiskCache.close();
					} catch (IOException e) {
						throw new MQException(e);
					}
					
					this.tempFile.delete();
					this.DiskCache = null;
					this.tempFile = null;	
					this.DiskCacheSize=0;
				}	
				
				this.Close = true;
				this.PendingClose=true;
				//add finish listener
				//System.out.println("Queue done");				
				
				if (this.getWaitNum > 0) {
					this.notifyAll();
				}
			}			
		}
		
	}	

	private void updateDiskCache() throws FileNotFoundException, IOException {

			
		byte[] buf = new byte[this.UpdateBufferSize];

		long TempWritePos = 0L;
		long TempReadPos =this.CacheReadPosition;
		int i = 0;
		DiskCache.seek(TempReadPos);

		while ((i = DiskCache.read(buf)) != -1) {
			DiskCache.seek(TempWritePos);
			DiskCache.write(buf, 0, i);
			TempWritePos = TempWritePos + i;
			TempReadPos = TempReadPos + i;
			DiskCache.seek(TempReadPos);
		}

		DiskCache.setLength(TempWritePos);
		this.Fin.getChannel().position(0L);
		this.CacheReadPosition=0L;
	}

	private LinkedList<T> getObjectsFromDiskCache(long objNum) throws IOException,
			ClassNotFoundException {
		
		LinkedList<T> result = new LinkedList<T>();	
		this.Fin.getChannel().position(this.CacheReadPosition);
		AdvancedBIS ABIS=new AdvancedBIS(this.Fin,this.UpdateBufferSize);
		ObjectReadStream oI=new ObjectReadStream(ABIS);
		
		long i = 0;
		while (i < objNum) {
			result.add((T)oI.readObject());
			this.DiskCacheSize--;
			i++;
		}		
		
		this.CacheReadPosition=this.Fin.getChannel().position()-ABIS.getCount()+ABIS.getPos();
		
		//oI.close();
		
		if (this.DiskCacheSize == 0) {
			this.Fin.close();
			this.DiskCache.close();
			
			this.tempFile.delete();
			this.DiskCache = null;
			this.tempFile = null;			
		}
		
		return result;
	}

	private void writeObjectsToDiskCache(LinkedList<T> ObjList) throws IOException {

		if (this.DiskCacheSize == 0) 
		{
			this.tempFile = File.createTempFile("queue", null, new File(this.tempDir));	
			
			this.DiskCache = new RandomAccessFile(this.tempFile, "rw");
			this.Fin = new FileInputStream(this.tempFile);	
			this.Fin.getChannel().position(0L);	
			this.CacheReadPosition=0L;
		}
			
		this.DiskCache.seek(this.DiskCache.length());
		
		byte[] buf = new byte[this.UpdateBufferSize];
		int bufSize=0;
		
				
		while (!ObjList.isEmpty()) {	
			
			ByteArrayOutputStream bOP = new ByteArrayOutputStream();
			ObjectWriteStream oO = new ObjectWriteStream(bOP);

			oO.writeObject(ObjList.remove(0));

			byte[] OArray = bOP.toByteArray();
			int ObjectSize=OArray.length;
			int OArray_ReadPos=0;
			
			while ((bufSize+ObjectSize)>=this.UpdateBufferSize)
			{
				int diff=this.UpdateBufferSize-bufSize;
				System.arraycopy(OArray, OArray_ReadPos, buf, bufSize, diff);
				this.DiskCache.write(buf,0,this.UpdateBufferSize);
				buf=new byte[this.UpdateBufferSize];
				bufSize=0;			
				OArray_ReadPos=OArray_ReadPos+diff;
				ObjectSize=ObjectSize-diff;
			}
			
			System.arraycopy(OArray, OArray_ReadPos, buf, bufSize, ObjectSize);
			bufSize=bufSize+ObjectSize;
			
			this.DiskCacheSize = this.DiskCacheSize + 1;
		}
		
		this.DiskCache.write(buf,0,bufSize);
		
	}
}
