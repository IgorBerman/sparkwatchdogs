package org.apache.spark.watchdogs;


import java.util.Optional;

/**
 * Created by igor on 5/20/17.
 */
public class WatchdogResult {
	private final String msg;
	private final boolean ok;
	private final Optional<Runnable> fix;


	public WatchdogResult(String msg, boolean isOk, Optional<Runnable> fix) {
		this.msg = msg;
		this.ok = isOk;
		this.fix = fix;
	}

	public String getMsg() {
		return msg;
	}

	public boolean isOk() {
		return ok;
	}

	public Optional<Runnable> getFix() {
		return fix;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		WatchdogResult that = (WatchdogResult) o;

		if (ok != that.ok) return false;
		if (msg != null ? !msg.equals(that.msg) : that.msg != null) return false;
		return fix != null ? fix.equals(that.fix) : that.fix == null;
	}

	@Override
	public int hashCode() {
		int result = msg != null ? msg.hashCode() : 0;
		result = 31 * result + (ok ? 1 : 0);
		result = 31 * result + (fix != null ? fix.hashCode() : 0);
		return result;
	}
}
