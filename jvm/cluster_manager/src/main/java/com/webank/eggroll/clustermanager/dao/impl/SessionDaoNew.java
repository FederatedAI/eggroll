package com.webank.eggroll.clustermanager.dao.impl;

import com.webank.eggroll.clustermanager.dao.mapper.SessionMainMapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionOptionMapper;
import com.webank.eggroll.clustermanager.dao.mapper.SessionProcessorMapper;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import com.webank.eggroll.core.meta.ErSessionMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
public class SessionDaoNew {

    @Autowired
    SessionMainMapper  sessionMainMapper;

    @Autowired
    SessionProcessorMapper  sessionProcessorMapper;

    @Autowired
    SessionOptionMapper sessionOptionMapper;


    @Transactional
    public  void  registerSession(ErSessionMeta  erSessionMeta){
//        String sessionId, String name, String status, String tag, Integer totalProcCount, Integer activeProcCount, Date
//        createdAt, Date updatedAt
        SessionMain  sessionMain =  new  SessionMain(erSessionMeta.id(),
                erSessionMeta.name(),
                erSessionMeta.status(),
                erSessionMeta.tag(),
                erSessionMeta.totalProcCount(),
                erSessionMeta.activeProcCount(),
                null,null
                );

        if(erSessionMeta.options().size()!=0){
            Map<String ,String> options = erSessionMeta.getJavaOptions();
            options.forEach(
                    (k,v)->{
                        SessionOption   option  = new SessionOption();
                        sessionOptionMapper.insert(option);
                    }
            );
        }






  //          );

        }
    //    sessionMainMapper.insert(sessionMain);



    public  static  void  main(String[] args){



//        ErSessionMeta  erSessionMeta  = new  ErSessionMeta("", "",
//                "", 0, 0, "", null, null, null, );

        Map data = new HashMap();


        data.put("k1","v1");
        data.entrySet().forEach(entry->{System.err.println(entry);});
        data.forEach((k,v)->{

        });




    }


}
