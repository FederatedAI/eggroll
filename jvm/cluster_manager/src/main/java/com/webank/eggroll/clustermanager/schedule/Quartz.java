package com.webank.eggroll.clustermanager.schedule;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.eggroll.core.postprocessor.ApplicationStartedListener;
import com.webank.eggroll.clustermanager.processor.ApplicationStartedRunner;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.concurrent.ConcurrentHashMap;

import static com.eggroll.core.config.Dict.SCHEDULE_KEY;


@Singleton
public class Quartz extends ApplicationStartedListener {

	Logger logger = LoggerFactory.getLogger(Quartz.class);
	private  Scheduler scheduler;

	static public   ConcurrentHashMap<String ,ScheduleInfo>  sheduleInfoMap = new ConcurrentHashMap<>();

//	new StdSchedulerFactory(getProperties("quartz.properties"))
	SchedulerFactory schedulerFactory = new StdSchedulerFactory();



	public  void  start(){
		try {
			scheduler = schedulerFactory.getScheduler();
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
		sheduleInfoMap.forEach((key, scheduleInfo) -> {
			try {
				ScheduleJob scheduleJob = new ScheduleJob();
				JobDetail jobDetail = JobBuilder.newJob(ScheduleJob.class).usingJobData(SCHEDULE_KEY, key).build();
				CronTriggerImpl cronTrigger = new CronTriggerImpl(scheduleInfo.getMethod().getName(), "t_tgroup1");
				try {
					cronTrigger.setCronExpression(scheduleInfo.cron);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				logger.info("register cron schedule {}",scheduleInfo);
				scheduler.scheduleJob(jobDetail,cronTrigger );
			}catch (Exception e){
				e.printStackTrace();
			}
		});
		try {
			scheduler.start();
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	public final Scheduler getScheduler() {
		return this.scheduler;
	}



	public static void main(String[] args) throws Exception {
		//第一步：创建一个JobDetail实例
//		JobDetail  jobDetail = JobBuilder.newJob(ScheduleJob.class).build();
//		JobDetail  jobDetail2 = JobBuilder.newJob(ScheduleJob2.class).build();
//		//第二步：创建CronTrigger，指定组及名称,并设置Cron表达式
//		CronTriggerImpl cronTrigger = new CronTriggerImpl("t_trigger1", "t_tgroup1");
//		//CronExpression cexp = new CronExpression("0/5 * * * * ?");//创建表达式
//		cronTrigger.setCronExpression("0/5 * * * * ?");
//
//
//		CronTriggerImpl cronTrigger2 = new CronTriggerImpl("t_trigger2", "t_tgroup1");
//		cronTrigger2.setCronExpression("0/1 * * * * ?");
//
//		//第三步：通过SchedulerFactory获取一个调度器实例
//
//
//		//第四步：将job跟trigger注册到scheduler中进行调度
//		scheduler.scheduleJob(jobDetail, cronTrigger);
//		scheduler.scheduleJob(jobDetail2,cronTrigger2);
//		//第五步：调度启动
//		scheduler.start();
	}

	@Override
	public void onApplicationStarted(String[] args) throws Exception {
		start();
	}
}
