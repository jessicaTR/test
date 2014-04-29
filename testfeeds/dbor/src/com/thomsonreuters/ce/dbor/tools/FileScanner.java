package com.thomsonreuters.ce.dbor.tools;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.file.FileFolderInfo;
import com.thomsonreuters.ce.dbor.file.DateOrderedFileWrapper;
import com.thomsonreuters.ce.dbor.file.ExtensionFilter;
import com.thomsonreuters.ce.dbor.file.FileProcessor;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;


public class FileScanner extends ControlledThread{
	

	private FileFolderInfo thisFFI;
	private FileProcessor FP;
	private Logger thisLogger;
	
	public FileScanner(ThreadController tc,FileFolderInfo ffi,FileProcessor fp, Logger logg)
	{
		super(tc);
		this.thisFFI=ffi;
		this.FP=fp;
		this.thisLogger=logg;
	}
	
	public void ControlledProcess()
	{
		File ArrivalFolder= new File(thisFFI.getArrivalFolder());   

		ExtensionFilter EF = new ExtensionFilter(thisFFI.getFileExtension());
		
		File[] FileList = ArrivalFolder.listFiles(EF);

		thisLogger.debug(FileList.length
				+ " file(s) patterned with "+thisFFI.getFileExtension()+" has been found in arrival folder: "
				+ thisFFI.getArrivalFolder());

		DateOrderedFileWrapper[] fileWrappers = new DateOrderedFileWrapper[FileList.length];
		for (int i = 0; i < FileList.length; i++) {
			fileWrappers[i] = new DateOrderedFileWrapper(FileList[i]);
		}

		// Sorting
		Arrays.sort(fileWrappers);

		// processing the first file
		if (fileWrappers.length>0)
		{
			File thisFile = fileWrappers[0].getFile();

			/////////////////////////////////////////////
			// move to work
			File WorkingFile = null;
			try {
				thisLogger.debug( "Starts moving feed file: "
						+ thisFile.getName() + " to work folder: "
						+ thisFFI.getWorkFolder());
				WorkingFile = new File(thisFFI.getWorkFolder()
						+ thisFile.getName());
				
				FileUtilities.MoveFile(thisFile, WorkingFile);

				thisLogger.debug("Feed file: "
						+ WorkingFile.getName()
						+ " has been moved to work folder: "
						+ thisFFI.getWorkFolder());
			} catch (Exception e) {
				thisLogger.warn( "Can not move file to work folder, please investigate."
							, e);
				return;
			}
			
			
			/////////////////////////////////////////////
			//process file
			
			try {
				
				thisLogger.info("File processor starts processing file: "
								+ WorkingFile.getName());	
				/////////////////////////////////////////////
				//Create file processing history			
				FP.CreateFileProcessHistory(WorkingFile);			
				FP.ProcessFile(WorkingFile);
				FP.CompleteFileHis();
				
				thisLogger.info("File processor has completed file: "
								+ WorkingFile.getName());
				
				/////////////////////////////////////////////
				// Move to archive
				try {
					thisLogger.debug("Starts moving it to archive folder: "
									+ thisFFI.getArchiveFolder());

					File ArchiveFile = new File(thisFFI.getArchiveFolder()
							+ WorkingFile.getName());

					FileUtilities.MoveFile(WorkingFile, ArchiveFile);

					thisLogger.debug("File: "+ ArchiveFile.getName()
							+ " has been moved to archive folder: "
							+ thisFFI.getArchiveFolder());
				} catch (Exception e) {
					thisLogger.warn( "Can not move file to archive folder, please investigate."
							, e);		
					return;
				}

				
			} catch (Exception e) {
				
				FP.UpdateFileHisToFailed();
				
				if (e instanceof LogicException)
				{
					//log exception details to details table
					FP.LogDetails(MsgCategory.WARN, e.getMessage());
					thisLogger.warn("Logic exception is thrown while processing file: "
							+ WorkingFile.getName(), e);

				}
				else if (e instanceof SystemException) 
				{
					SystemException se = (SystemException) e;
					FP.LogDetails(MsgCategory.WARN, "System issue: "
							+ se.getEventID()
							+ ", Please ask support to investigate");
					thisLogger.warn("System Exception: "
							+ se.getEventID()
							+ " is thrown while processing file: "
							+ WorkingFile.getName(), e);

				}
				else
				{
					String EventID = UUID.randomUUID().toString();
					FP.LogDetails(MsgCategory.WARN, "Unknown issue: "
							+ EventID
							+ ", Please ask support to investigate");
					thisLogger.warn ("Unknown issue: "
							+ EventID
							+ " is thrown while processing file: "
							+ WorkingFile.getName(), e);
				}// end of if
				
				////////////////////////////////////////
				// move to unproc
				try {
					thisLogger.debug("Starts moving feed file: " + WorkingFile.getName()
									+ " to upproc folder: "
									+ thisFFI.getUnprocFolder());
					File UnprocFile = new File(thisFFI.getUnprocFolder()
							+ WorkingFile.getName());
					FileUtilities.MoveFile(WorkingFile, UnprocFile);

					thisLogger.debug("File: "
							+ UnprocFile.getName()
							+ " has been moved to unproc folder: "
							+ thisFFI.getUnprocFolder());

				} catch (Exception e1) {
					thisLogger.warn("Can not move file to unproc folder, please investigate.",
									e1);
					return;
				}	
							
			}// end of catch exception
		}
		
	}
			
}
