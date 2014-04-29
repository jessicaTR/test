package com.thomsonreuters.ce.queue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

import com.thomsonreuters.ce.io.AdvancedBIS;
import com.thomsonreuters.ce.io.ObjectReadStream;
import com.thomsonreuters.ce.io.ObjectWriteStream;

public class DiskCache<T extends Serializable> {

	private int Size;

	private String tempDir;

	private String tempFileName=null;
	
	private long CacheReadPosition = 0L;
	
	private int Pointer=1;
		
	
	public DiskCache(String TempDir)
	{
		this.tempDir = TempDir;
		this.Size=0;
		this.CacheReadPosition=0L;
		
		try {
			
			File tempFile = File.createTempFile("DiskCache", null, new File(this.tempDir));	
			this.tempFileName=tempFile.getName();
			
		} catch (Exception e) {
			throw new MQException("Initialize DiskQueue failed: ",e);
		}				
	}

	public boolean isEmpty()
	{
		if (this.Size==0)
		{
			return true;
		}
				
		return false;
	}

	public int GetSize()
	{
		return this.Size;
	}

	public void Delete()
	{
		File TempFile=new File(this.tempDir,this.tempFileName);
		TempFile.delete();
	}

	public T GetNext() 
	{
		if (this.Pointer<=this.Size)
		{
			try {
				File TempFile=new File(this.tempDir,this.tempFileName);
				FileInputStream Fin=new FileInputStream(TempFile);
				Fin.getChannel().position(this.CacheReadPosition);
				AdvancedBIS ABIS=new AdvancedBIS(Fin);
				ObjectReadStream oI=new ObjectReadStream(ABIS);

				T NextObj=(T)oI.readObject();

				this.CacheReadPosition=Fin.getChannel().position()-ABIS.getCount()+ABIS.getPos();
				this.Pointer++;

				Fin.close();
				return NextObj;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		else
		{		
			return null;
		}
		
		
	}
	
	public void Reset()
	{
		this.CacheReadPosition=0L;		
		this.Pointer=1;
	}
	
	public boolean HasNext()
	{
		if (this.Pointer<=this.Size)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	

	public void Append(T Obj){
		
		try {
			File TempFile=new File(this.tempDir,this.tempFileName);
			
			RandomAccessFile DiskCache=new RandomAccessFile(TempFile, "rw");;
			DiskCache.seek(DiskCache.length());

			ByteArrayOutputStream bOP = new ByteArrayOutputStream();
			ObjectWriteStream oO = new ObjectWriteStream(bOP);
			oO.writeObject(Obj);

			byte[] OArray = bOP.toByteArray();
			int ObjectSize=OArray.length;

			DiskCache.write(OArray,0,ObjectSize);
			DiskCache.close();
			
			this.Size++;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}	

}
