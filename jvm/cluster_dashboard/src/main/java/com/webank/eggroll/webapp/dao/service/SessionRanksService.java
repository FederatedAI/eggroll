package com.webank.eggroll.webapp.dao.service;

import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.EggRollBaseServiceImpl;
import com.webank.eggroll.clustermanager.entity.SessionRanks;
import com.webank.eggroll.webapp.dao.mapper.SessionRanksMapper;


@Singleton
public class SessionRanksService extends EggRollBaseServiceImpl<SessionRanksMapper, SessionRanks> {
}
