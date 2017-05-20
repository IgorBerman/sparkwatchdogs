package org.apache.spark.watchdogs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.gson.Gson;

public class CoresWaitingWatchdogTest {

	ResourceGetter resourceGetter = mock(ResourceGetter.class);
	JavaSparkContext ctx = mock(JavaSparkContext.class);
	SparkConf conf = mock(SparkConf.class);

	@Before
	public void init() {
		when(ctx.getConf()).thenReturn(conf);
	}

	@Test
	public void testEmptyResponse() throws Exception {
		CoresWaitingWatchdog watchdog = new CoresWaitingWatchdog(ctx, resourceGetter, new Gson(), "url",  1);
		String json = "[]";

		when(resourceGetter.get("url")).thenReturn(json);

		assertTrue(watchdog.check().isOk());
	}

	@Test
	public void testExceptionWhenGettingStatus() throws Exception {
		CoresWaitingWatchdog watchdog = new CoresWaitingWatchdog(ctx, resourceGetter, new Gson(), "url", 1);

		when(resourceGetter.get("url")).thenThrow(new IllegalArgumentException("something happened"));

		assertTrue(watchdog.check().isOk());
	}

	@Test
	public void testWhenOk() throws Exception {
		when(conf.getAppId()).thenReturn("app-20170516074321-0041");
		CoresWaitingWatchdog watchdog = new CoresWaitingWatchdog(ctx, resourceGetter, new Gson(), "url",  1);

		List<String> allLines = Files.readAllLines(Paths.get(this.getClass().getResource("ok.json").toURI()), Charset.defaultCharset());
		String json = Joiner.on("\n").join(allLines);

		when(resourceGetter.get("url")).thenReturn(json);

		assertTrue(watchdog.check().isOk());
	}


	@Test
	public void testWhenWaitingNegativeCores() throws Exception {
		when(conf.getAppId()).thenReturn("app-20170516074321-0041");
		CoresWaitingWatchdog watchdog = new CoresWaitingWatchdog(ctx, resourceGetter, new Gson(), "url", 1);

		List<String> allLines = Files.readAllLines(Paths.get(this.getClass().getResource("waitingNegativeCores.json").toURI()), Charset.defaultCharset());
		String json = Joiner.on("\n").join(allLines);

		when(resourceGetter.get("url")).thenReturn(json);

		assertFalse(watchdog.check().isOk());
	}

	@Test
	public void testWhenWaitingZeroCores() throws Exception {
		when(conf.getAppId()).thenReturn("app-20170516074321-0041");
		CoresWaitingWatchdog watchdog = new CoresWaitingWatchdog(ctx, resourceGetter, new Gson(), "url", 1);

		List<String> allLines = Files.readAllLines(Paths.get(this.getClass().getResource("waitingZeroCores.json").toURI()), Charset.defaultCharset());
		String json = Joiner.on("\n").join(allLines);

		when(resourceGetter.get("url")).thenReturn(json);

		assertFalse(watchdog.check().isOk());
	}

}
