package org.apache.spark.watchdogs;

import java.util.List;

public class SparkMasterStatus {
	private List<SparkWorkerStatus> workers;
	private List<ActiveApp> activeapps;

	public SparkMasterStatus(List<SparkWorkerStatus> workers, List<ActiveApp> activeapps) {
		this.workers = workers;
		this.activeapps = activeapps;
	}

	public SparkMasterStatus() {
	}

	public List<SparkWorkerStatus> getWorkers() {
		return workers;
	}

	public void setWorkers(List<SparkWorkerStatus> workers) {
		this.workers = workers;
	}

	public List<ActiveApp> getActiveapps() {
		return activeapps;
	}

	public void setActiveapps(List<ActiveApp> activeapps) {
		this.activeapps = activeapps;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SparkMasterStatus that = (SparkMasterStatus) o;

		if (workers != null ? !workers.equals(that.workers) : that.workers != null) return false;
		return activeapps != null ? activeapps.equals(that.activeapps) : that.activeapps == null;
	}

	@Override
	public int hashCode() {
		int result = workers != null ? workers.hashCode() : 0;
		result = 31 * result + (activeapps != null ? activeapps.hashCode() : 0);
		return result;
	}

	public static class SparkWorkerStatus {
		private String id;
		private String host;
		private String webuiaddress;
		private String state;
		private int cores;
		private int coresused;
		private int coresfree;

		public SparkWorkerStatus(String id, String host, String webuiaddress, String state, int cores, int coresused, int coresfree) {
			this.id = id;
			this.host = host;
			this.webuiaddress = webuiaddress;
			this.state = state;
			this.cores = cores;
			this.coresused = coresused;
			this.coresfree = coresfree;
		}

		public SparkWorkerStatus() {
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public String getWebuiaddress() {
			return webuiaddress;
		}

		public void setWebuiaddress(String webuiaddress) {
			this.webuiaddress = webuiaddress;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public int getCores() {
			return cores;
		}

		public void setCores(int cores) {
			this.cores = cores;
		}

		public int getCoresused() {
			return coresused;
		}

		public void setCoresused(int coresused) {
			this.coresused = coresused;
		}

		public int getCoresfree() {
			return coresfree;
		}

		public void setCoresfree(int coresfree) {
			this.coresfree = coresfree;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			SparkWorkerStatus that = (SparkWorkerStatus) o;

			if (cores != that.cores) return false;
			if (coresused != that.coresused) return false;
			if (coresfree != that.coresfree) return false;
			if (id != null ? !id.equals(that.id) : that.id != null) return false;
			if (host != null ? !host.equals(that.host) : that.host != null) return false;
			if (webuiaddress != null ? !webuiaddress.equals(that.webuiaddress) : that.webuiaddress != null)
				return false;
			return state != null ? state.equals(that.state) : that.state == null;
		}

		@Override
		public int hashCode() {
			int result = id != null ? id.hashCode() : 0;
			result = 31 * result + (host != null ? host.hashCode() : 0);
			result = 31 * result + (webuiaddress != null ? webuiaddress.hashCode() : 0);
			result = 31 * result + (state != null ? state.hashCode() : 0);
			result = 31 * result + cores;
			result = 31 * result + coresused;
			result = 31 * result + coresfree;
			return result;
		}
	}


	public static class ActiveApp {
		private long starttime;
		private String id;
		private String name;
		private int cores;
		private String state;
		private long duration;

		public ActiveApp(long starttime, String id, String name, int cores, String state, long duration) {
			this.starttime = starttime;
			this.id = id;
			this.name = name;
			this.cores = cores;
			this.state = state;
			this.duration = duration;
		}

		public ActiveApp() {
		}

		public long getStarttime() {
			return starttime;
		}

		public void setStarttime(long starttime) {
			this.starttime = starttime;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getCores() {
			return cores;
		}

		public void setCores(int cores) {
			this.cores = cores;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public long getDuration() {
			return duration;
		}

		public void setDuration(long duration) {
			this.duration = duration;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ActiveApp activeApp = (ActiveApp) o;

			if (starttime != activeApp.starttime) return false;
			if (cores != activeApp.cores) return false;
			if (duration != activeApp.duration) return false;
			if (id != null ? !id.equals(activeApp.id) : activeApp.id != null) return false;
			if (name != null ? !name.equals(activeApp.name) : activeApp.name != null) return false;
			return state != null ? state.equals(activeApp.state) : activeApp.state == null;
		}

		@Override
		public int hashCode() {
			int result = (int) (starttime ^ (starttime >>> 32));
			result = 31 * result + (id != null ? id.hashCode() : 0);
			result = 31 * result + (name != null ? name.hashCode() : 0);
			result = 31 * result + cores;
			result = 31 * result + (state != null ? state.hashCode() : 0);
			result = 31 * result + (int) (duration ^ (duration >>> 32));
			return result;
		}
	}
}
