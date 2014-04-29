package com.thomsonreuters.ce.queue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.RandomAccessFile;
import java.util.zip.GZIPOutputStream;


import com.thomsonreuters.ce.io.ObjectWriteStream;

public class DiskCacheWriter<T extends Serializable> {
    private File DiskCache = null;

    private GZIPOutputStream GOS = null;
    
    private long length=0;

    public DiskCacheWriter(String Dir, String FileName) throws IOException {
	// this.CacheReadPosition=0L;

	DiskCache = new File(Dir, FileName);

	DiskCache.createNewFile();
	
	FileOutputStream FOS= new FileOutputStream(DiskCache);
	
	FOS.getChannel().position(8);	
	
	GOS = new GZIPOutputStream(FOS);

    }

    public void Delete() throws Exception {

	synchronized (this) {
	    GOS.close();
	    DiskCache.delete();
	    DiskCache = null;
	}
    }

    public void Append(T Obj) throws IOException {

	synchronized (this) {
	    ByteArrayOutputStream bOP = new ByteArrayOutputStream();
	    ObjectWriteStream oO = new ObjectWriteStream(bOP);
	    oO.writeObject(Obj);

	    byte[] OArray = bOP.toByteArray();

	    GOS.write(OArray);
	    
	    length++;
	}
    }

    public void Finish() throws IOException {
	synchronized (this) {
	    GOS.flush();
	    GOS.finish();
	    GOS.close();

	    
	    RandomAccessFile RF=new RandomAccessFile(this.DiskCache, "rw");
	    RF.seek(0);

	    ByteArrayOutputStream BOS=new ByteArrayOutputStream();
	    DataOutputStream DOS=new DataOutputStream(BOS);
	    DOS.writeLong(length);
	    DOS.flush();
	    byte[] LengthByte = BOS.toByteArray();
	    DOS.close();
	    
	    RF.write(LengthByte);
	    RF.close();
	    
	}
    }
    
}
