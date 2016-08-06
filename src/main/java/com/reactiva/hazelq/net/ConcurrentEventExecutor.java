package com.reactiva.hazelq.net;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;
/**
 * @author esutdal
 *
 */
class ConcurrentEventExecutor extends SingleThreadEventExecutor {

	private static final Logger log = LoggerFactory.getLogger(ConcurrentEventExecutor.class);
	private ExecutorService executors;
	
	/**
	 * 
	 * @param parent
	 * @param executor
	 * @param maxPendingTasks
	 * @param rejectedExecutionHandler
	 * @param execThreads
	 */
	ConcurrentEventExecutor(EventExecutorGroup parent, Executor executor, int maxPendingTasks,
            RejectedExecutionHandler rejectedExecutionHandler, int execThreads) {
		super(parent, executor, true, maxPendingTasks, rejectedExecutionHandler);
		executors = Executors.newFixedThreadPool(execThreads, new ThreadFactory() {
			int n = 1;
			@Override
			public Thread newThread(Runnable arg0) {
				Thread t = new Thread(arg0, "xcomm-exec-"+(n++));
				return t;
			}
		});
		
		addShutdownHook(new Runnable() {
			
			@Override
			public void run() {
				executors.shutdown();
				try {
					executors.awaitTermination(30, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		});
	}
	
	private void run0(Runnable r)
	{
		if (r != null) {
            r.run();
            updateTime();
        }
	}
	private synchronized void updateTime() {
        updateLastExecutionTime();
    }	
	@Override
	protected void run() {
		
		for (;;) 
		{
            final Runnable task = takeTask();
            if (task != null) {
            	executors.submit(new Runnable() {
					
					@Override
					public void run() {
						try {
							run0(task);
						}
						catch(Exception e)
						{
							log.error("-- Execution thread exception --", e);
						}
						finally {
						}
					}
				});
                
            }

            if (confirmShutdown()) {
                break;
            }
        }
		
	}

}
