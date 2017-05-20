package org.apache.spark.watchdogs;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by igor on 5/20/17.
 */
public class Watchdogs {
	static final Logger log = LoggerFactory.getLogger(SparkStageHangingWatchdog.class);

	public static void install(final String statusEndpoint, final JavaSparkContext ctx) {
		ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
				.setNameFormat("AppStatusChecker").build();
		final long sleepTimeBetweenChecks = TimeUnit.MINUTES.toMillis(10);
		final long maxWaitingTimeForResources = TimeUnit.MINUTES.toMillis(10);
		final int maxTaskExecutionTimeInHours = 3;
		ExecutorService checkerThread = Executors.newFixedThreadPool(1, threadFactory);
		checkerThread.execute(new Runnable() {
			@Override
			public void run() {
				Gson gson = new Gson();
				String appId = ctx.getConf().getAppId();
				ResourceGetter resourceGetter = new DefaultResourceGetter();
				Watchdog stagesWatchdog = new SparkStageHangingWatchdog(ctx, appId, maxTaskExecutionTimeInHours);
				Watchdog coresWatchdog = new CoresWaitingWatchdog(ctx, resourceGetter, gson, statusEndpoint, maxWaitingTimeForResources);
				List<Watchdog> watchdogs = ImmutableList.of(coresWatchdog, stagesWatchdog);
				while (true) {
					try {
						Thread.sleep(sleepTimeBetweenChecks);
					} catch(Exception e) {
						log.warn("Sleep exception", e);
					}

					log.info("Starting watchdogs...");
					for(Watchdog watchdog : watchdogs) {
						try {
							log.info("Watchdog: " + watchdog.name());
							WatchdogResult watchdogResult = watchdog.check();
							if (!watchdogResult.isOk()) {
								watchdogResult.getFix().get().run();
							}
						} catch (Exception e) {
							log.warn("Can't check " + watchdog.name(), e);
						}
					}
					log.info("Watchdogs done");
					try {
						if (ctx.sc().stopped().get()) {
							return;
						}
					} catch(Exception e) {
						log.warn("Check if ctx stopped exception", e);
					}
				}
			}
		});
	}
}
