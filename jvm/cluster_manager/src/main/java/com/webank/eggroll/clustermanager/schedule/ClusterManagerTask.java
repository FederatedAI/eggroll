package com.webank.eggroll.clustermanager.schedule;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.cluster.ClusterManagerService;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

@Singleton
public class ClusterManagerTask {

    @Inject
    Quartz  quartz;


    public static void runTask(Thread thread) {
        thread.start();
    }


//    public static  class QuartzJob implements Job {
//
//
//        public void execute(JobExecutionContext jobExecutionContext)
//                throws JobExecutionException {
//            // 获取JobDetail中的JobDataMap
//            JobDataMap jobDetailDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
//            // 获取Trigger中的JobDataMap
//            JobDataMap triggerDataMap = jobExecutionContext.getTrigger().getJobDataMap();
//            log.info(jobDetailDataMap.get("message"));
//            log.info(triggerDataMap.get("number"));
//        }
//    }

    public static void main(String[] args)  {
//        // 构建一个JobDetail实例...
//
//        // 构建一个Trigger，指定Trigger名称和组，规定该Job立即执行，且两秒钟重复执行一次
////        SimpleTrigger trigger = TriggerBuilder.newTrigger()
////                .startNow() // 执行的时机，立即执行
////                .withIdentity("trigger1", "group1") // 不是必须的
////                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
////                        .withIntervalInSeconds(2).repeatForever()).build();  // build之后返回的类型是SimpleTrigger，具体的类型是SimpleTriggerImpl
//
//        JobDetail jobDetail = JobBuilder.newJob(QuartzJob.class)
//                // 指定JobDetail的名称和组名称
//                .withIdentity("job1", "group1")
////                // 使用JobDataMap存储用户数据
////                .usingJobData("message", "JobDetail传递的文本数据").build();
//
//        // 创建一个SimpleTrigger，规定该Job立即执行，且两秒钟重复执行一次
//        SimpleTrigger trigger = TriggerBuilder.newTrigger()
//                // 设置立即执行，并指定Trigger名称和组名称
//                .startNow().withIdentity("trigger1", "group1")
//                // 使用JobDataMap存储用户数据
//                .usingJobData("number", 128)
//                // 设置运行规则，每隔两秒执行一次，一直重复下去
//                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
//                        .withIntervalInSeconds(2).repeatForever()).build();
//
//        // 得到Scheduler调度器实例
//        Scheduler scheduler = null;
//        try {
//            scheduler = new StdSchedulerFactory().getScheduler();
//        } catch (SchedulerException e) {
//            e.printStackTrace();
//        }
//
//        scheduler.scheduleJob(jobDetail, trigger); // 绑定JobDetail和Trigger
//        scheduler.start();


    }

}
