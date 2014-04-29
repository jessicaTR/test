package com.thomsonreuters.ce.dbor.extract;

import java.util.Map;

public interface TaskProcessor {
			
	public void processTask(Map<Integer, Task> taskMap);	
}
