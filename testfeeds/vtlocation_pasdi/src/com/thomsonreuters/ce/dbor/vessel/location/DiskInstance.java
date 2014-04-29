package com.thomsonreuters.ce.dbor.vessel.location;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;



public class DiskInstance<T extends Serializable> {
	
	private String tempFileName=null;
	private boolean IsEmpty=true;
	
	public DiskInstance()
	{
		try {
			File tempFile = File.createTempFile("DIS", null, new File(VTLocationLoader.tempfolder));	
			this.tempFileName=tempFile.getName();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public DiskInstance(String fn)
	{
		this.tempFileName=fn;
	}
	
	public boolean Exists()
	{
		File TempFile=new File(VTLocationLoader.tempfolder,this.tempFileName);
		return TempFile.exists();
	}
	
	public boolean IsEmpty()
	{
		return this.IsEmpty;
	}
	
	public void SaveInstance(T obj) throws IOException
	{
		File TempFile=new File(VTLocationLoader.tempfolder,this.tempFileName);
		ObjectOutputStream OOS= new ObjectOutputStream(new FileOutputStream(TempFile));
		OOS.writeObject(obj);
		OOS.close();
		this.IsEmpty=false;
	}
	
	public T GetInstance() throws IOException, FileNotFoundException, ClassNotFoundException
	{

		File TempFile=new File(VTLocationLoader.tempfolder,this.tempFileName);
		ObjectInputStream OIS= new ObjectInputStream(new FileInputStream(TempFile));
		T thisInstance = (T)OIS.readObject();
		OIS.close();
		return thisInstance;
		
	}
	
	public void Delete()
	{
		File TempFile=new File(VTLocationLoader.tempfolder,this.tempFileName);
		TempFile.delete();
	}

}
