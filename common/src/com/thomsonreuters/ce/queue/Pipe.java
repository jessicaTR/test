package com.thomsonreuters.ce.queue;



public interface Pipe<T> {
	public T getObj();
	public T putObj(T newObj);
	public void Shutdown(boolean isNormal);
	public long getInputCount();
	public long getOutputCount();
	public void WaitForClose();
}
