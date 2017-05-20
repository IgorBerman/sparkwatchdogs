package org.apache.spark.watchdogs;

interface ResourceGetter {
	String get(String url) throws Exception;
}
