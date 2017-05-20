package org.apache.spark.watchdogs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkStageInfoImpl;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.scheduler.DAGScheduler;
import org.junit.Test;

public class SparkStageHangingWatchdogTest {

	JavaSparkContext ctx = mock(JavaSparkContext.class);
	JavaSparkStatusTracker tracker = mock(JavaSparkStatusTracker.class);

	@Test
	public void testNoStages() throws Exception {
		SparkStageHangingWatchdog checker = new SparkStageHangingWatchdog(ctx, "appId", 3);

		when(ctx.statusTracker()).thenReturn(tracker);

		assertTrue(checker.check().isOk());
	}

	@Test
	public void testNoStages2() throws Exception {
		SparkStageHangingWatchdog checker = new SparkStageHangingWatchdog(ctx, "appId", 3);

		when(ctx.statusTracker()).thenReturn(tracker);

		when(tracker.getActiveStageIds()).thenReturn(new int[]{});

		assertTrue(checker.check().isOk());
	}

	@Test
	public void testStageOk() throws Exception {
		SparkStageHangingWatchdog checker = new SparkStageHangingWatchdog(ctx, "appId", 3);

		when(ctx.statusTracker()).thenReturn(tracker);

		when(tracker.getActiveStageIds()).thenReturn(new int[]{1});

		when(tracker.getStageInfo(1)).thenReturn(new SparkStageInfoImpl(1, 0, System.currentTimeMillis(), null, 0, 0, 0, 0));

		assertTrue(checker.check().isOk());
	}


	@Test
	public void testStageNotOk() throws Exception {
		SparkStageHangingWatchdog checker = new SparkStageHangingWatchdog(ctx, "appId", 3);

		when(ctx.statusTracker()).thenReturn(tracker);

		when(tracker.getActiveStageIds()).thenReturn(new int[]{1});

		when(tracker.getStageInfo(1)).thenReturn(new SparkStageInfoImpl(1, 0, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4), null, 0, 0, 0, 0));

		assertFalse(checker.check().isOk());
	}


	@Test
	public void testTwoStageNotOk() throws Exception {
		SparkStageHangingWatchdog checker = new SparkStageHangingWatchdog(ctx, "appId", 3);

		when(ctx.statusTracker()).thenReturn(tracker);
		SparkContext scalaContext = mock(SparkContext.class);
		when(ctx.sc()).thenReturn(scalaContext);

		DAGScheduler dagScheduler = mock(DAGScheduler.class);
		when(scalaContext.dagScheduler()).thenReturn(dagScheduler);

		when(tracker.getActiveStageIds()).thenReturn(new int[]{1, 2});

		when(tracker.getStageInfo(1)).thenReturn(new SparkStageInfoImpl(1, 0, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4), null, 0, 0, 0, 0));
		when(tracker.getStageInfo(2)).thenReturn(new SparkStageInfoImpl(2, 0, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4), null, 0, 0, 0, 0));

		WatchdogResult watchdogResult = checker.check();
		assertFalse(watchdogResult.isOk());

		watchdogResult.getFix().get().run();

		verify(dagScheduler).cancelStage(1);
		verify(dagScheduler).cancelStage(2);

	}

	@Test
	public void testStageAbsent() throws Exception {
		SparkStageHangingWatchdog checker = new SparkStageHangingWatchdog(ctx, "appId", 3);

		when(ctx.statusTracker()).thenReturn(tracker);

		when(tracker.getActiveStageIds()).thenReturn(new int[]{1});

		when(tracker.getStageInfo(1)).thenReturn(null);

		assertTrue(checker.check().isOk());
	}
}
