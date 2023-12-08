package org.fedai.eggroll.clustermanager.schedule;

import com.google.inject.Key;
import org.fedai.eggroll.clustermanager.Bootstrap;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.lang.reflect.InvocationTargetException;

import static org.fedai.eggroll.core.config.Dict.SCHEDULE_KEY;

public class ScheduleJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String scheduleKey = (String) jobExecutionContext.getJobDetail().getJobDataMap().get(SCHEDULE_KEY);
        ScheduleInfo scheduleInfo = Quartz.sheduleInfoMap.get(scheduleKey);
        Key key = scheduleInfo.getKey();
        Object instance = Bootstrap.injector.getInstance(key);
        try {
            scheduleInfo.getMethod().invoke(instance);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

    }
}
