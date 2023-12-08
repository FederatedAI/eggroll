package org.fedai.eggroll.core.flow;


import org.fedai.eggroll.core.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowLogUtil {
    static Logger logger = LoggerFactory.getLogger("flow");

    public static void printFlowLog(Context context) {
        try {
//            if(!context.getUri().startsWith("v1/cluster-manager/job/rendezvous")){
            logger.info(context.toString());

        } catch (Throwable ignore) {
            ignore.printStackTrace();
        }
    }


}
