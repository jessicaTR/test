package com.thomsonreuters.ce.dbor.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.BiffException;

import com.thomsonreuters.ce.exception.SystemException;

public abstract class ZippedXLSProcessor extends ZippedFileProcessor{

	public void ProcessFile(String filename, InputStream IS)
	{
		try {
			
			Initialize(filename);			
			BufferedInputStream bis = new BufferedInputStream(IS);
			WorkbookSettings ws = new WorkbookSettings();
			ws.setLocale(Locale.US);
//			ws.setEncoding("GBK");
			Workbook rwb = Workbook.getWorkbook(bis, ws);
			
			for (int i=0; i <rwb.getNumberOfSheets();i++)
			{
				Sheet rs = rwb.getSheet(i);
				ProcessSheet(rs);
			}
			
			Finalize();
			
			rwb.close();
			bis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			throw new SystemException("Can't find file", e);
		} catch (BiffException e) {
			// TODO Auto-generated catch block
			throw new SystemException("Can't read spreadsheet", e);
		} catch (IndexOutOfBoundsException e) {
			// TODO Auto-generated catch block
			throw new SystemException("Can't find particular sheet", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SystemException("IO Exception", e);
		}

	}
	public void Initialize(File feedFile){};
	
	public abstract void Initialize(String filename);
	public abstract void ProcessSheet(Sheet sheet);
	public abstract void Finalize();
	
}
