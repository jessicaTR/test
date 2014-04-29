package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.output;

import java.io.IOException;
import java.sql.SQLException;

public interface Writable {	
	public void write() throws IOException, SQLException;
}
