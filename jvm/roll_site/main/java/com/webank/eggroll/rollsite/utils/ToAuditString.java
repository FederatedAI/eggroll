package com.webank.eggroll.rollsite.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.webank.eggroll.core.constant.RollSiteConfKeys;
import com.webank.eggroll.rollsite.model.ProxyServerConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component
public class ToAuditString {
    @Autowired
    private ProxyServerConf proxyServerConf;

    public static String toOneLineString(Message m, String delim) {
        LinkedList<String> result = new LinkedList<>();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : m.getAllFields().entrySet()) {
            Object value = "";
            if (entry.getValue() instanceof Message) {
                value = "{" + toOneLineString((Message) entry.getValue(), ",") + "}";
            } else if (entry.getValue() instanceof List) {
                value += "[";
                for (Object o : ((List) entry.getValue())) {
                    value += "{" + toOneLineString((Message) o, ",") + "}.";
                }
                value += "]";
            } else {
                value = entry.getValue();
            }

            result.add(entry.getKey().getName() + "=" + value.toString());
        }

        return String.join(delim, result);
    }

    public void refreshAuditTopics() { ;
        String auditTopics = RollSiteConfKeys.EGGROLL_ROLLSITE_AUDIT_TOPICS().get();
        if (auditTopics != null) {
            proxyServerConf.setAuditTopics(auditTopics);
        }
    }
}
