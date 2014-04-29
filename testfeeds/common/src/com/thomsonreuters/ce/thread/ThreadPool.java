package com.thomsonreuters.ce.thread;


import com.thomsonreuters.ce.queue.Pipe;

public class ThreadPool {

	private int ActiveNum;

	private Pipe<Runnable> MainQueue;

	private ThreadPool(int Thread_Num, Pipe<Runnable> Task_Queue) {
		
		this.MainQueue = Task_Queue;
		
		for (int i = 0; i < Thread_Num; i++) {
			Thread WorkerThread = new Thread( new Worker());
			WorkerThread.start();
		}
	}

	public static ThreadPool NewPool(int Thread_Num,
			Pipe<Runnable> Task_Queue) {
		ThreadPool thisPool = new ThreadPool(Thread_Num, Task_Queue);
		return thisPool;
	}

	public void Shutdown(boolean isNormal) {
		synchronized (this) {			
//			 System.out.println("start shutdown in thread pool shutdown");
			this.MainQueue.Shutdown(isNormal);
		}
	}

	public boolean IsRunning() {
		synchronized (this) {
			if (this.ActiveNum > 0) {
				return true;
			}
			return false;
		}
	}

	private class Worker implements Runnable {

		public void run() {

			synchronized (ThreadPool.this) {
				ThreadPool.this.ActiveNum++;
			}

			while (true) {

				Runnable first_Action = null;

				first_Action = ThreadPool.this.MainQueue
							.getObj();

				if (first_Action == null) {
					// ThreadPool has been shutdown
					break;
				}

				try {
					first_Action.run();
				} catch (Throwable ex) {
					ex.printStackTrace();
				}
			}

			synchronized (ThreadPool.this) {
				ThreadPool.this.ActiveNum--;
/*				if (ThreadPool.this.ActiveNum == 0) {
					
					String Msg = "Thread Pool has been shutdown";
					System.out.println(Msg);
					
				}*/
			}

		}
	}

}
