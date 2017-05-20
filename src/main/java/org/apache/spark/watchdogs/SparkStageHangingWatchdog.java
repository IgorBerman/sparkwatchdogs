package org.apache.spark.watchdogs;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sometimes some stage hangs. Due to other reasons(writing to s3) we can't use speculation mode, so we prefer just to kill some stage
 */
class SparkStageHangingWatchdog implements Watchdog {
	static final Logger log = LoggerFactory.getLogger(SparkStageHangingWatchdog.class);

	private final JavaSparkContext ctx;
	private final String appId;
	private final int thresholdInHours;

	public SparkStageHangingWatchdog(JavaSparkContext ctx, String appId, int thresholdInHours) {
		this.ctx = ctx;
		this.appId = appId;
		this.thresholdInHours = thresholdInHours;
	}

	public WatchdogResult check() {
		final Set<Integer> stagesThatTakesLongTime = new HashSet<>();
		int[] activeStageIds = ctx.statusTracker().getActiveStageIds();

		if (activeStageIds == null) {
			return new WatchdogResult("ok", true, Optional.<Runnable>empty());
		}

		for (int activeStageId : activeStageIds) {
			SparkStageInfo stageInfo = ctx.statusTracker().getStageInfo(activeStageId);
			if (stageInfo == null) {
				continue;
			}
			long submissionTime = stageInfo.submissionTime();

			if (System.currentTimeMillis() - submissionTime > TimeUnit.HOURS.toMillis(thresholdInHours)) {
				stagesThatTakesLongTime.add(stageInfo.stageId());
			}
		}

		if (stagesThatTakesLongTime.isEmpty()) {
			return new WatchdogResult("ok", true, Optional.<Runnable>empty());
		}
		Runnable fix = () -> {
			for (int stageId : stagesThatTakesLongTime) {
				log.warn("Canceling state {} of app {} ", stageId, appId);
				try {
					ctx.sc().dagScheduler().cancelStage(stageId);
				} catch (Exception e) {
					log.error("Unable to cancel stage {} of app {}", stageId, appId, e);
				}
			}
		};
		return new WatchdogResult(
				"AppId " + appId + "stages : " + Joiner.on(",").join(stagesThatTakesLongTime) + " take more than configured threshold of " + thresholdInHours + " hrs",
				false,
				Optional.of(fix)
		);
	}
}
