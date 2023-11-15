package org.fedai.eggroll.clustermanager.schedule;

import com.google.inject.Key;
import lombok.Data;

import java.lang.reflect.Method;

@Data
public class ScheduleInfo {
    String cron;
    Method method;
    Key key;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(key.getTypeLiteral().getRawType()).append(":").append(method.getName()).append(" ").append(cron);
        return sb.toString();
    }
}
