/**
 * 
 */
package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.transformation;

import java.util.List;

import com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.entity.DataUnit;

/**
 * @author lei.yang
 *
 */
public interface Transformable {
	public List<DataUnit> transform();
}
