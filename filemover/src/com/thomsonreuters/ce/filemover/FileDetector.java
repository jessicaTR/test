package com.thomsonreuters.ce.filemover;

import java.io.File;
import org.apache.log4j.Logger;
import java.util.Arrays;
import com.jcraft.jsch.Session;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.dbor.file.ExtensionFilter;
import com.thomsonreuters.ce.dbor.file.DateOrderedFileWrapper;
import com.thomsonreuters.ce.ssh.SshUtils;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.exception.SystemException;

public class FileDetector extends ControlledThread {
	
	private ExtensionFilter EF; 
	
	private String waiting;
	
	private String archive;

	private String user;
	
	private String passwd;
	
	private String host;
	
	private int port;
	
	private String rfolder;
	
	private Logger logger;

	public FileDetector(String Waiting, String Archive, String Destination, Logger fileLogger, String Fileextension,ThreadController tc) {
		super(tc);
		EF=new ExtensionFilter(Fileextension);
		
		String remotefilefolder=Destination;
		
		String account=remotefilefolder.substring(0, remotefilefolder.indexOf('@'));
		this.user=account.substring(0,account.indexOf('/'));
		this.passwd=account.substring(account.indexOf('/')+1);
		remotefilefolder=remotefilefolder.substring(remotefilefolder.indexOf('@')+1);
		this.host=remotefilefolder.substring(0, remotefilefolder.indexOf(':'));
		String portrfile=remotefilefolder.substring(remotefilefolder.indexOf(':')+1);
		this.port=Integer.parseInt(portrfile.substring(0,portrfile.indexOf(':')));
		
		this.rfolder=portrfile.substring(portrfile.indexOf(':')+1);
		
		if (!this.rfolder.endsWith("/"))
		{
			this.rfolder = this.rfolder+"/";
		}
		
		this.logger=fileLogger;
		
		this.waiting = Waiting;
		
		if (!this.waiting.endsWith("/"))
		{
			this.waiting = this.waiting+"/";
		}

		this.archive = Archive;
		if (!this.archive.endsWith("/"))
		{
			this.archive = this.archive+"/";
		}

		
	}
	
	@Override
	public void ControlledProcess() {
		// TODO Auto-generated method stub
		File WaitingFolder=new File(waiting);
		
		File[] FileList = WaitingFolder.listFiles(EF);

		
		logger.debug(FileList.length
				+ " file(s) has been found in arrival folder: "
				+ waiting);		
		
		if (FileList.length==0)
		{
			return;
		}
		
		DateOrderedFileWrapper[] fileWrappers = new DateOrderedFileWrapper[FileList.length];
		
		for (int i = 0; i < FileList.length; i++) {
			fileWrappers[i] = new DateOrderedFileWrapper(FileList[i]);
		}

		// Sorting
		Arrays.sort(fileWrappers);
		

		Session session=null;
		try {
			logger.debug("Logging in remote server");	
			
			session=SshUtils.connect(user,passwd,host,port);
			
			logger.debug("Server login successfully");		
			
			for (int i = 0; i < fileWrappers.length; i++) {
				
				if (IsShuttingDown())
				{
					break;
				}	
				
				File thisFile = fileWrappers[i].getFile();
				logger.info("Delivering file: " + thisFile.getName() +" to "+ rfolder + "@" + host );
				
				
				try {
					SshUtils.ScpTo(thisFile.getAbsolutePath(), rfolder,session);
					logger.info("File: " + thisFile.getName()+" has been delivered to "+ rfolder + "@" + host );
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error("Failed to deliver file: "+thisFile.getName(), e);
					continue;
				}
				
				//Moving file to archive			
				File ArchiveFile = new File(archive	+ thisFile.getName());
				FileUtilities.MoveFile(thisFile, ArchiveFile);
				logger.debug("File: " + thisFile.getName()+" has been archived to folder "+archive);
				
			}
		} 
		catch(Throwable e)
		{
			logger.error("Unexpected exception: ", e);
		}
		finally
		{
			logger.debug("disconnecting session");
			if (session != null) {
				session.disconnect();
			}
		}
		
	}

}
