package org.apache.spark.watchdogs;

import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class CoresWaitingWatchdog implements Watchdog {
	static final Logger log = LoggerFactory.getLogger(SparkStageHangingWatchdog.class);

	final String waitingStatusPrefix = "wait";
	final int waitingStatusCores = 0;

	private final JavaSparkContext ctx;
	private final ResourceGetter resourceGetter;
	private final Gson gson;
	private final String statusEndpoint;
	private final long maxWaitingTimeForCores;

	public CoresWaitingWatchdog(JavaSparkContext ctx, ResourceGetter resourceGetter, Gson gson, String statusEndpoint, long maxWaitingTimeForCores) {
		this.ctx = ctx;
		this.resourceGetter = resourceGetter;
		this.gson = gson;
		this.statusEndpoint = statusEndpoint;
		this.maxWaitingTimeForCores = maxWaitingTimeForCores;
	}

	public WatchdogResult check() {
		try {
			log.debug("Selecting status from {} ", statusEndpoint);
			String masterStatusAsStr = resourceGetter.get(statusEndpoint);
			SparkMasterStatus clusterStatus = gson.fromJson(masterStatusAsStr, SparkMasterStatus.class);
			final String appId = ctx.getConf().getAppId();
			for (SparkMasterStatus.ActiveApp activeApp : clusterStatus.getActiveapps()) {
				log.debug("Checking app for waiting status: {}", activeApp);
				if (appId.equalsIgnoreCase(activeApp.getId()) &&
						Strings.nullToEmpty(activeApp.getState()).toLowerCase().contains(waitingStatusPrefix) &&
						activeApp.getCores() <= waitingStatusCores &&
						System.currentTimeMillis() - activeApp.getStarttime() > maxWaitingTimeForCores) {

					Runnable fix = new Runnable() {
						@Override
						public void run() {
							log.error("Application {} not passed 'waiting for cores' check, stopping context ", appId);
							ctx.stop();
						}
					};
					return new WatchdogResult(appId + " has tasks more than threshold " + maxWaitingTimeForCores, false, Optional.of(fix));
				}
			}
			return new WatchdogResult(appId + " ok", true, Optional.empty());
		} catch (Exception e) {
			log.warn("Can't parse master status url", e);
			return new WatchdogResult("Watchdog check failed, can't decide status for appId " + ctx.getConf().getAppId() + " due to error: " + e.getMessage(), true, Optional.empty());
		}
	}
}
