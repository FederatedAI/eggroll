package org.fedai.eggroll.webapp.dao.service;

import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.EggRollBaseServiceImpl;
import org.fedai.eggroll.clustermanager.entity.SessionRanks;
import org.fedai.eggroll.webapp.dao.mapper.SessionRanksMapper;


@Singleton
public class SessionRanksService extends EggRollBaseServiceImpl<SessionRanksMapper, SessionRanks> {
}
