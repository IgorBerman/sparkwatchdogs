package org.apache.spark.watchdogs;

public interface ResourceGetter {
	String get(String url) throws Exception;
}
