package org.apache.spark.watchdogs;

interface Watchdog {
	WatchdogResult check();

	default String name() {
		return this.getClass().getSimpleName();
	}
}
