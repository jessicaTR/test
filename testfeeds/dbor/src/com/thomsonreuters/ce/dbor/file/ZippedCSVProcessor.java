package com.thomsonreuters.ce.dbor.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.thomsonreuters.ce.exception.SystemException;

import au.com.bytecode.opencsv.CSVReader;

public abstract class ZippedCSVProcessor extends ZippedFileProcessor {

	
	public void ProcessFile(String filename, InputStream IS)
	{
		CSVReader CSVR = new CSVReader(new InputStreamReader(IS));
		List<String[]> allElementsInCSV = null;
		
		try {
			allElementsInCSV = CSVR.readAll();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SystemException("IOException is thrown out while parsing CSV file: "+filename ,e);
		}		
		
		ProcessCSVFile(filename, allElementsInCSV);
		
	}
	
	public abstract void ProcessCSVFile(String FileName, List<String[]> CSVArray);

}
