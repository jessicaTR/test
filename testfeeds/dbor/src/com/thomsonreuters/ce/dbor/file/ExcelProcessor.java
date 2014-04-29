package com.thomsonreuters.ce.dbor.file;

import java.io.File;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Locale;

import com.thomsonreuters.ce.exception.SystemException;

import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.BiffException;


public abstract class ExcelProcessor extends FileProcessor {

	@Override
	public void ProcessFile(File FeedFile) {
		try {
			
			Initialize(FeedFile);
			
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(FeedFile));
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

	public abstract void Initialize(File FeedFile);
	public abstract void ProcessSheet(Sheet sheet);
	public abstract void Finalize();

}
