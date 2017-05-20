package org.apache.spark.watchdogs;

public interface Watchdog {
	WatchdogResult check();

	default String name() {
		return this.getClass().getSimpleName();
	}
}
