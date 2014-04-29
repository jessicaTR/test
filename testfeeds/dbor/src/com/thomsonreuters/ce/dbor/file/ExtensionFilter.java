package com.thomsonreuters.ce.dbor.file;

import java.io.File;
import java.io.FileFilter;

public class ExtensionFilter implements FileFilter {

	private String Extension;
	
	public ExtensionFilter(String Ext)
	{
		this.Extension=Ext;

	}
	
	public boolean accept(File file) {
		// TODO Auto-generated method stub
		// "tmp_" prefix is added by crontab between FSS to SFS 
//		return (!file.getName().startsWith(Extension))&&(file.isFile());
		return file.getName().matches(Extension);

	}
}
