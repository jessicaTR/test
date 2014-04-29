package com.thomsonreuters.ce.ssh;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.thomsonreuters.ce.exception.SystemException;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;


public class SshUtils {
	
	
	
	public static void ScpTo(String lfile, String rfolder, Session session) throws Exception
	{
		boolean ptimestamp = true;
		
		char FILESEPARATOR=System.getProperty("file.separator").charAt(0);
		
		String lfilename=lfile.substring(lfile.lastIndexOf(FILESEPARATOR)+1);
		
		String rtempfile;
		
		if (!rfolder.endsWith("/"))
		{
			rfolder=rfolder+"/";
		}
		
		rtempfile=rfolder+"tmp_"+lfilename;
	
		try {
			// exec 'scp -t rfile' remotely
			String command="scp " + (ptimestamp ? "-p" :"") +" -t "+rtempfile;
			Channel channel=session.openChannel("exec");
			((ChannelExec)channel).setCommand(command);

			// get I/O streams for remote scp
			OutputStream out=channel.getOutputStream();
			InputStream in=channel.getInputStream();

			channel.connect();

			//////////////////////////////////////////////////
			//Check result
			//////////////////////////////////////////////////
			int b=in.read();
			// b may be 0 for success,
			//          1 for error,
			//          2 for fatal error,
			//          -1
			
			if (b!=0)
			{
				StringBuffer sb=new StringBuffer();
				int c;
				do {
					c=in.read();
					sb.append((char)c);
				}
				while(c!='\n');
				
				in.close();
				out.close();
				channel.disconnect();
				throw new SystemException(command+" error: "+sb.toString());
			}
			//////////////////////////////////////////////////

			File _lfile = new File(lfile);

			if(ptimestamp){
				command="T "+(_lfile.lastModified()/1000)+" 0";
				// The access time should be sent here,
				// but it is not accessible with JavaAPI ;-<
				command+=(" "+(_lfile.lastModified()/1000)+" 0\n"); 
				out.write(command.getBytes()); 
				out.flush();
				
				//////////////////////////////////////////////////
				//Check result
				//////////////////////////////////////////////////
				b=in.read();
				// b may be 0 for success,
				//          1 for error,
				//          2 for fatal error,
				//          -1
				
				if (b!=0)
				{
					StringBuffer sb=new StringBuffer();
					int c;
					do {
						c=in.read();
						sb.append((char)c);
					}
					while(c!='\n');
					
					in.close();
					out.close();
					channel.disconnect();
					throw new SystemException(command+ " error: "+sb.toString());
				}
				//////////////////////////////////////////////////
			}

			// send "C0644 filesize filename", where filename should not include '/'
			long filesize=_lfile.length();
			command="C0644 "+filesize+" ";
			command+=lfilename;
			command+="\n";
			out.write(command.getBytes()); out.flush();
			
			//////////////////////////////////////////////////
			//Check result
			//////////////////////////////////////////////////
			b=in.read();
			// b may be 0 for success,
			//          1 for error,
			//          2 for fatal error,
			//          -1
			
			if (b!=0)
			{
				StringBuffer sb=new StringBuffer();
				int c;
				do {
					c=in.read();
					sb.append((char)c);
				}
				while(c!='\n');
				
				in.close();
				out.close();
				channel.disconnect();
				throw new SystemException(command+" error: "+sb.toString());
			}
			//////////////////////////////////////////////////

			// send a content of lfile
			FileInputStream fis=new FileInputStream(lfile);
			byte[] buf=new byte[1024];
			while(true){
				int len=fis.read(buf, 0, buf.length);
				if(len<=0) break;
				out.write(buf, 0, len); //out.flush();
			}
			fis.close();
			
			// send '\0'
			buf[0]=0; out.write(buf, 0, 1); out.flush();

			//////////////////////////////////////////////////
			//Check result
			//////////////////////////////////////////////////
			b=in.read();
			// b may be 0 for success,
			//          1 for error,
			//          2 for fatal error,
			//          -1
			
			if (b!=0)
			{
				StringBuffer sb=new StringBuffer();
				int c;
				do {
					c=in.read();
					sb.append((char)c);
				}
				while(c!='\n');
				
				in.close();
				out.close();
				channel.disconnect();
				throw new SystemException("scp error: "+sb.toString());
			}
			//////////////////////////////////////////////////
			
			in.close();
			out.close();
			channel.disconnect();
			
			////////////////////////////////////////////////
			//rename temp file to normal;
			String rfile;
			rfile=rfolder+lfilename;
			
			command="mv "+rtempfile+" "+rfile;
			execCmd(command, session);
			
		} catch (Exception e) {
			throw e;
		}
	}
	
	/** 
     * 连接到指定的IP 
     *  
     * @throws JSchException 
     */  
    public static Session connect(String user, String passwd, String host, int port) throws Exception{  
    	JSch jsch = new JSch();  
    	Session session = jsch.getSession(user, host, port);  
        session.setPassword(passwd);  
          
        java.util.Properties config = new java.util.Properties();  
        config.put("StrictHostKeyChecking", "no");  
        session.setConfig(config);  
          
        session.connect();  
        
        return session;
    }  
    
    
    /** 
     * 执行相关的命令 
     * @throws JSchException  
     */  
    public static void execCmd(String command, Session session) throws Exception
    {  
    	Channel channel=session.openChannel("exec");
        ((ChannelExec)channel).setCommand(command);
    	
    	channel.setInputStream(null);  
    	((ChannelExec) channel).setErrStream(System.err);  
    	InputStream in = channel.getInputStream();  
    	channel.connect();  
    	
    	try {
			byte[] tmp=new byte[1024];
			while(true){
				while(in.available()>0){
					int i=in.read(tmp, 0, 1024);
					if(i<0)
					{
						break;
					}
					System.out.print(new String(tmp, 0, i));
				}
				if(channel.isClosed()){
					break;
				}
				try{Thread.sleep(1000);}catch(Exception ee){}
			}
		} finally{
			channel.disconnect();
		}
    }  
   
}
