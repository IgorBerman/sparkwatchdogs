# sparkwatchdogs
simple implementation for spark watchdogs for etl processing

see [org.apache.spark.watchdogs.Watchdogs](src/main/java/org/apache/spark/watchdogs/Watchdogs.java) as usage example

Currently there are two watchodgs:
- CoresWaitingWatchdog - 
Probably due to some bug in spark core, sometime there is a situation when the cores are free and available, but application can't get them.
In this scenario it waiting for cores without terminating.
When such application is killed, next retry to submit exactly same application gets all necessary cores and proceedes without a problem.
You can deploy some cron based solution to retry the submission of same application.
- SparkStageHangingWatchdog -
Sometimes some stage hangs. Due to other reasons([see spark-gotchas](https://github.com/IgorBerman/spark-gotchas)) we can't use speculation mode, so we prefer just to kill some stage/job

