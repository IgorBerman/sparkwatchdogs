package org.apache.spark.watchdogs;

import java.net.URL;

import org.apache.commons.io.IOUtils;

public class DefaultResourceGetter implements ResourceGetter {

	@Override
	public String get(String url) throws Exception {
		return IOUtils.toString(new URL(url));
	}

}
