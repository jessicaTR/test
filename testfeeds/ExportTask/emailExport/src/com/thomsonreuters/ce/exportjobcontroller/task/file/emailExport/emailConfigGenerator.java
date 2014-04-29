package com.thomsonreuters.ce.exportjobcontroller.task.file.emailExport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;

import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.exportjobcontroller.Starter;
import com.thomsonreuters.ce.exportjobcontroller.task.file.ExportTask;

public class emailConfigGenerator {
	public static final String SERVICE_NAME = "emailExport";
	public static String cfgHeader = "dataset,timestamp,subject,sender,receivers,cc,attachments,body";
	public static String cfgFileName = "EMAIL_Config.csv";
	public static String strTimeStamp = getStringDate(currentDate());
	private static Logger logger = Starter.getLogger(SERVICE_NAME);

	public static boolean fileToZip(File[] SourceFiles, String fileName,
			String dataset) {

		boolean flag = false;

		FileInputStream fis = null;
		BufferedInputStream bis = null;
		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		File zipFile = null;
		try {
			String zipFilePath = ReadConfiguration(Starter.Config_File, dataset);
			fileName = fileName + strTimeStamp;
			zipFile = new File(zipFilePath, fileName + ".zip");
			File[] sourceFiles = SourceFiles;
			if (null == sourceFiles || sourceFiles.length < 1) {
				logger.warn("Source files do not exist.");
			} else {
				fos = new FileOutputStream(zipFile);
				zos = new ZipOutputStream(new BufferedOutputStream(fos));
				byte[] bufs = new byte[1024 * 10];
				for (int i = 0; i < sourceFiles.length; i++) {
					ZipEntry zipEntry = new ZipEntry(sourceFiles[i].getName());
					zos.putNextEntry(zipEntry);
					fis = new FileInputStream(sourceFiles[i]);
					bis = new BufferedInputStream(fis, 1024 * 10);
					int read = 0;
					while ((read = bis.read(bufs, 0, 1024 * 10)) != -1) {
						zos.write(bufs, 0, read);
					}
					sourceFiles[i].delete();
				}
				flag = true;
				logger.info(dataset + zipFile.getName()
						+ " generated successfully.");
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			logger.warn(dataset + zipFile.getName() + " generated failed!");
			throw new RuntimeException(e);

		} catch (IOException e) {
			e.printStackTrace();
			logger.warn(dataset + zipFile.getName() + " generated failed!");
			throw new RuntimeException(e);
		} finally {
			try {
				if (null != bis)
					bis.close();
				if (null != zos)
					zos.close();
				if (null != fis)
					fis.close();
				if (null != fos)
					fos.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}

		}
		return flag;
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
				} else {
					out.write(buffer, 0, ins);
				}
			}

		} catch (Exception e) {

			throw new SystemException("File: " + f1.getName()
					+ " can not be moved to " + f2.getAbsolutePath(), e);
		}
	}

	//
	// public static void GenFile(String FileNamePrefix, String FileNameSuffix,
	// String dataset, long tas_id, String header, String content,
	// String footer, String subject, String sender, String receivers,
	// String cc, String body) {
	// try {
	//
	// File[] SourceFiles = { thisCsvFile, thisFile };
	// String zipFileName = FileNamePrefix + tas_id + "_" + strTimeStamp;
	// String filelocation = ReadConfiguration(configfile, dataset);
	// Boolean fileSuccessFlag = fileToZip(SourceFiles, filelocation,
	// zipFileName);
	// if (fileSuccessFlag)
	// LoggerHelper.log(LogLevel.INFO, dataset + zipFileName
	// + " generated successfully.");
	// else
	// LoggerHelper.log(LogLevel.WARN, dataset + zipFileName
	// + " generated failed!");
	//
	// if (thisFile.delete()) {
	//
	// } else {
	// LoggerHelper.log(LogLevel.WARN,
	// dataset + " Delete " + thisFile.getName()
	// + " is failed.");
	// }
	// if (thisCsvFile.delete()) {
	//
	// } else {
	// LoggerHelper.log(LogLevel.WARN, dataset + " Delete "
	// + thisCsvFile.getName() + " is failed.");
	// }
	//
	// } catch (FileNotFoundException e) {
	// throw new SystemException(e);
	// } catch (UnsupportedEncodingException e) {
	// throw new SystemException(e);
	// } catch (IOException e) {
	// throw new SystemException(e);
	// } catch (Exception e) {
	// throw new SystemException(e);
	// }
	// }

	public static File GenDataFile(String FileNamePrefix,
			String FileNameSuffix, long tas_id, String data) {
		try {
			BufferedWriter bw = null;
			String tempfilelocation = Starter.getTempFolder(SERVICE_NAME);
			// strTimeStamp = getStringDate(currentDate());
			String filename = FileNamePrefix + tas_id + "_" + strTimeStamp
					+ FileNameSuffix;
			String Tempfilename = filename + ".temp";
			File thisTempFile = new File(tempfilelocation, Tempfilename);
			File thisFile = null;

			if (thisTempFile.exists() == true) {
				thisTempFile.delete();
			}

			thisTempFile.createNewFile();

			GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(
					thisTempFile));
			bw = new BufferedWriter(new OutputStreamWriter(zfile, "UTF8"));
			if (data != null) {
				bw.write(data);
				bw.flush();
			}
			bw.close();
			thisFile = new File(tempfilelocation, filename);

			if (thisFile.exists() == true) {
				thisFile.delete();
			}

			MoveFile(thisTempFile, thisFile);
			return thisFile;
		} catch (FileNotFoundException e) {
			throw new SystemException(e);
		} catch (UnsupportedEncodingException e) {
			throw new SystemException(e);
		} catch (IOException e) {
			throw new SystemException(e);
		} catch (Exception e) {
			throw new SystemException(e);
		}
	}

	public static File GenEmailCfgFile(String dataset, String subject,
			String sender, String receivers, String cc, String body,
			String filename) {
		try {
			String TempCsvFilename = cfgFileName + ".temp";
			File thisTempCsvFile = new File(Starter.getTempFolder(SERVICE_NAME), TempCsvFilename);
			if (thisTempCsvFile.exists() == true) {
				thisTempCsvFile.delete();
			}
			BufferedWriter csvBw = null;
			FileOutputStream out = new FileOutputStream(thisTempCsvFile);
			OutputStreamWriter osw = new OutputStreamWriter(out, "UTF8");
			csvBw = new BufferedWriter(osw);
			csvBw.write(cfgHeader);
			csvBw.flush();
			csvBw.newLine();
			csvBw.write(dataset + "," + strTimeStamp + "," + subject + ","
					+ sender + "," + receivers + "," + cc + "," + filename
					+ "," + body);
			csvBw.flush();
			csvBw.close();
			osw.close();
			out.close();

			File thisCsvFile = new File(Starter.getTempFolder(SERVICE_NAME), cfgFileName);
			if (thisCsvFile.exists() == true) {
				thisCsvFile.delete();
			}
			MoveFile(thisTempCsvFile, thisCsvFile);
			return thisCsvFile;
		} catch (FileNotFoundException e) {
			throw new SystemException(e);
		} catch (UnsupportedEncodingException e) {
			throw new SystemException(e);
		} catch (IOException e) {
			throw new SystemException(e);
		} catch (Exception e) {
			throw new SystemException(e);
		}
	}

	public static String ReadConfiguration(String configfile, String dataset) {
		try {
			FileInputStream TaskFis = new FileInputStream(configfile);
			Properties Prop = new Properties();
			Prop.load(TaskFis);
			String filelocation = Prop.getProperty(dataset + ".filelocation");
			filelocation = FileUtilities.GetAbsolutePathFromEnv(filelocation);
			return filelocation;

		} catch (Exception e) {
			throw new SystemException(
					"Error occurs while reading configuration file "
							+ configfile);
		}
	}

	public static List<Integer> getTaskIDs(ArrayList<ExportTask> taskList) {

		List<Integer> lstTaskId = new ArrayList<Integer>();
		Iterator<ExportTask> iterator = taskList.iterator();
		ExportTask task = null;
		while (iterator.hasNext()) {
			task = (ExportTask) iterator.next();
			lstTaskId.add(task.getId());
		}

		return lstTaskId;
	}

	public static java.sql.Timestamp currentDate() {

		java.util.Date longdate = new java.util.Date();
		java.sql.Date date = new java.sql.Date(longdate.getTime());
		java.sql.Time time = new java.sql.Time(longdate.getTime());
		java.sql.Timestamp curdate = java.sql.Timestamp.valueOf(date.toString()
				+ " " + time.toString());

		return curdate;
	}

	public static String getStringDate(Date time) {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
		String dateString = formatter.format(time);
		return dateString;
	}

}
