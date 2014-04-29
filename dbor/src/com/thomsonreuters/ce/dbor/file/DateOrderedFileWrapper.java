package com.thomsonreuters.ce.dbor.file;

import java.io.File;

public class DateOrderedFileWrapper implements Comparable {

    private File file;
    
    public DateOrderedFileWrapper(File file) {
        this.file = file;
    }

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
        if (this.file.getName().compareTo(((DateOrderedFileWrapper)o).getFile().getName()) > 0) {
            return 1;
        } else if (this.file.getName().compareTo(((DateOrderedFileWrapper)o).getFile().getName()) < 0) {
            return -1;
        } else {
            return 0;
        }
	}
	
    public File getFile() {
        return this.file;
    }

}
