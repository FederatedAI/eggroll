package com.webank.eggroll.clustermanager.schedule;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.quartz.*;

import java.util.concurrent.ConcurrentHashMap;


@Singleton
public class Quartz {

	private final Scheduler scheduler;
	static public   ConcurrentHashMap<String ,ScheduleInfo>  sheduleInfoMap = new ConcurrentHashMap<>();



	@Inject
	public Quartz(final SchedulerFactory factory, final GuiceJobFactory jobFactory) throws SchedulerException {
		this.scheduler = factory.getScheduler();
		this.scheduler.setJobFactory(jobFactory);
//		this.scheduler.s

	}

	public  void  start(){
		sheduleInfoMap.forEach((key, scheduleInfo) -> {

			ScheduleJob scheduleJob= new ScheduleJob();
			JobDetail  jobDetail = JobBuilder.newJob(ScheduleJob.class).usingJobData("job_key",key).build();
			CronTrigger  cronTrigger = new CronTrigger() {
			}
			scheduler.scheduleJob(jobDetail,)
		});
	}




	public final Scheduler getScheduler() {
		return this.scheduler;
	}
}
