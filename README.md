# sparkwatchdogs
simple implementation for spark watchdogs for etl processing.
## Motivation
We have production etl jobs that run all day long. The system should be as automatic as possible, without human intervention.
We still trying to get this level, so we constantly building additional self-checks or as I call them "watchdogs". Many times it
 means that just killing some stage/job/application is better than just finding out that due to one stage the whole
 pipeline did nothing in last few hours and the whole pipeline is in huge delay.

## Example
see [org.apache.spark.watchdogs.Watchdogs](src/main/java/org/apache/spark/watchdogs/Watchdogs.java) as usage example

## What we have currently
Currently there are two watchodgs:
- CoresWaitingWatchdog - 
Probably due to some bug in spark core, sometime there is a situation when the cores are free and available, but 
application can't get them.
In this scenario it waiting for cores without terminating.
When such application is killed, next retry to submit exactly same application gets all necessary cores and proceedes 
without a problem.
You can deploy some cron based solution to retry the submission of same application.
Pay attention that there are all kind of different problems why this situation is "normal", e.g. you may have firewall
 problem and driver can't connect to master and get resources(there are plenty of stackoverflow questions regarding this problem).
Make sure to use this watchdog if you 100% sure that your cluster setup is ok.
- SparkStageHangingWatchdog -
Sometimes some stage hangs. Due to other reasons([see spark-gotchas](https://github.com/IgorBerman/spark-gotchas)) 
we can't use speculation mode, so we prefer just to cancel some stage(so probably the whole job will be canelled)

