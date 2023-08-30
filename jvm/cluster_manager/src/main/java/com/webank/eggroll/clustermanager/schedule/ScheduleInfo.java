package com.webank.eggroll.clustermanager.schedule;

import com.google.inject.Key;
import lombok.Data;

import java.lang.reflect.Method;

@Data
public class ScheduleInfo {
    String  cron;
    Method method;
    Key key;
}
