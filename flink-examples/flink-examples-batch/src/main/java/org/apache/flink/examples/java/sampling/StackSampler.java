package org.apache.flink.examples.java.sampling;

import org.apache.flink.runtime.util.JvmUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StackSampler {
	public static void main(String[] args) {
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
		for (StackTraceElement stackTraceElement : stackTrace) {
			System.out.println("\n" + stackTraceElement);
		}

		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(() -> {
			String threadName = Thread.currentThread().getName();
			System.out.println("Hello " + threadName);
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});


//		System.out.println("--------------");
//		ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
//		System.out.println(	threadMxBean.getThreadInfo(2, Integer.MAX_VALUE));

		Collection<ThreadInfo> threadDump = JvmUtils.createThreadDump();
		System.out.println(threadDump);

		for (ThreadInfo threadInfo : threadDump) {
			if(!threadInfo.isDaemon()){
				System.out.println(">>" + threadInfo.getThreadId());
				System.out.println(threadInfo);
			}
		}
	}
}
