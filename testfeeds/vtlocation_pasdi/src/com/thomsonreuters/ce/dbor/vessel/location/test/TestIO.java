package com.thomsonreuters.ce.dbor.vessel.location.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import com.thomsonreuters.ce.dbor.file.ExtensionFilter;
import com.thomsonreuters.ce.exception.SystemException;

public class TestIO {

	private static String ArchiveFolder="/dsdata/feedsin/vtlocation/archive/";
	private static String WorkFolder="/dsdata/feedsin/vtlocation/work/";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ExtensionFilter EF = new ExtensionFilter("^(VTCurrentLocation_)(.)*zip$");

		while(true)
		{

			System.out.println("##########################################");
			System.out.println("#Moving files from archive to work folder#");
			System.out.println("##########################################");

			File Archive= new File(ArchiveFolder);

			File[] ArchiveFileList = Archive.listFiles(EF);


			File[] WorkFiles=new File[ArchiveFileList.length];

			for (int i=0;i<ArchiveFileList.length;i++)
			{
				WorkFiles[i]=new File(WorkFolder + ArchiveFileList[i].getName());

				System.out.println("starts moving file "+ArchiveFileList[i].getName() + " from archive to work folder");
				MoveFile(ArchiveFileList[i], WorkFiles[i]);
				System.out.println("File "+ArchiveFileList[i].getName()+ " has been moved to work folder");

			}


			System.out.println("###############################################");
			System.out.println("#Moving files from work back to archive folder#");
			System.out.println("###############################################");

			File Work= new File(WorkFolder);

			File[] WorkFileList = Work.listFiles(EF);

			File[] ArchiveFiles=new File[ArchiveFileList.length];

			for (int i=0;i<WorkFileList.length;i++)
			{
				ArchiveFiles[i]=new File(ArchiveFolder + WorkFileList[i].getName());

				System.out.println("starts moving file "+WorkFileList[i].getName() + " from work back to archive folder");
				MoveFile(WorkFileList[i], ArchiveFiles[i]);
				System.out.println("File "+WorkFileList[i].getName()+ " has been moved back to archive folder");
			}
		}


	}

	
	
    public static void MoveFile(File f1, File f2) {
	try {
	    int length = 1048576;
	    FileInputStream in = new FileInputStream(f1);
	    FileOutputStream out = new FileOutputStream(f2);
	    byte[] buffer = new byte[length];

	    while (true) {
		int ins = in.read(buffer);
		if (ins == -1) {
		    in.close();
		    out.flush();
		    out.close();
		    f1.delete();
		    return;
		} else
		{
		    out.write(buffer, 0, ins);
		}
	    }

	} catch (Exception e) {

	    throw new SystemException("File: " + f1.getName()
		    + " can not be moved to "
		    + f2.getAbsolutePath(),e);			
	} 
    }	
}
