package com.thomsonreuters.ce.dbor.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;


import com.thomsonreuters.ce.exception.SystemException;

public abstract class ZippedFileProcessor extends FileProcessor {
	
	public void ProcessFile(File FeedFile) {	
		
			ZipFile ZippedFile=null;
			InputStream IS = null;
			try {
				ZippedFile=new ZipFile(FeedFile);
				Enumeration ZippedFiles=ZippedFile.entries();
				
				Initialize(FeedFile);
				
				while(ZippedFiles.hasMoreElements())
				{
					ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();
					String FileName = entry.getName();
					IS = ZippedFile.getInputStream(entry);
					ProcessFile(FileName, IS);
				}
				
				Finalize();
				IS.close();
				ZippedFile.close();				
			} catch (ZipException e) {
				throw new SystemException("Zipped File is corrupted", e);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				throw new SystemException("IO Exception", e);
			}
	}
	
	public abstract void Initialize(File FeedFile);
	public abstract void ProcessFile(String filename, InputStream IS);
	public abstract void Finalize();
}
