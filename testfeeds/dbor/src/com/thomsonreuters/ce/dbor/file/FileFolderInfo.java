package com.thomsonreuters.ce.dbor.file;


public class FileFolderInfo {
	
	protected String FileExtension;
	protected String ArrivalFolder;
	protected String WorkFolder;
	protected String ArchiveFolder;
	protected String UnprocFolder;

	
	
	public FileFolderInfo(String fileextension,String arrivalfolder,String workfolder,String archivefolder,String unprocfolder)
	{
		this.FileExtension=fileextension;
		this.ArrivalFolder=arrivalfolder;
		this.WorkFolder=workfolder;
		this.ArchiveFolder=archivefolder;
		this.UnprocFolder=unprocfolder;
		
	}


	public String getArchiveFolder() {
		return ArchiveFolder;
	}


	public String getArrivalFolder() {
		return ArrivalFolder;
	}


	public String getFileExtension() {
		return FileExtension;
	}

	public String getUnprocFolder() {
		return UnprocFolder;
	}

	public String getWorkFolder() {
		return WorkFolder;
	}
	
}
