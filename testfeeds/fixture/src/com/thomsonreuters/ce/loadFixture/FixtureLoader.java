package com.thomsonreuters.ce.loadFixture;

import java.io.File;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.loadFixture.Starter;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.file.FileProcessor;

public class FixtureLoader extends FileProcessor{
	private Logger logger  = Starter.thisLogger;
	public void ProcessFile(File FeedFile) {
		 
		String filename = FeedFile.getName();
		String prefix=null;

		try {			
			  prefix=filename.substring(filename.lastIndexOf(".")+1);
			  if(prefix.equals("xls")) {
				  ICAPFixtureLoader icap = new ICAPFixtureLoader();
				  icap.ProcessFile(FeedFile);
			  }else if(prefix.equals("DBF")){
				  MRIFixtureLoader mri = new MRIFixtureLoader();
				  mri.ProcessFile(FeedFile);
			  }else if(prefix.equals("zip")){
				  TRFixtureLoader tr = new TRFixtureLoader();
				  tr.ProcessFile(FeedFile);
			  }
			
		}catch(Exception e){
			this.LogDetails(MsgCategory.ERROR, "Failed to process file:" + filename + " because of exception: "+e.getMessage());
			logger.error("Failed to process file:" + filename, e);
		}
		
	}
	
	public FileCategory getFileCatory(File FeedFile) {

		return FileCategory.getInstance("FIXTURE");
	}
}
