package com.thomsonreuters.ce.queue;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.DataInputStream;
import java.util.zip.GZIPInputStream;

import com.thomsonreuters.ce.io.ObjectReadStream;

public class DiskCacheReader <T extends Serializable> {

    private File DiskCache=null;

    private ObjectReadStream ORS=null;
    
    private long length=0;
    
    private long pointer=0;

    public DiskCacheReader(String Dir, String FileName) throws IOException
    {
	//this.CacheReadPosition=0L;
	DiskCache = new File(Dir, FileName);
	//CacheReadPosition = 0L;
	DataInputStream FIS=new DataInputStream(new FileInputStream(DiskCache));
	
	length=FIS.readLong();

	ORS=new ObjectReadStream(new GZIPInputStream(FIS));
	
    }

    public void Delete() throws Exception
    {
	synchronized (this)
	{
	    ORS.close();
	    DiskCache.delete();
	    DiskCache=null;
	}
    }

    public T GetNext() throws Exception
    {
	synchronized (this)
	{
	    if (this.pointer<this.length)
	    {
		T NextObj=(T)ORS.readObject();
		this.pointer++;
		return NextObj;
	    }
	    else
	    {		
		return null;
	    }
	}
    }

    public void Reset() throws IOException
    {
	synchronized (this)
	{
		DataInputStream FIS=new DataInputStream(new FileInputStream(DiskCache));
		length=FIS.readLong();
		ORS=new ObjectReadStream(new GZIPInputStream(FIS));
	}
    }
    
}
