//package com.webank.eggroll.clustermanager;
//
//import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
//import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
//import com.eggroll.core.postprocessor.ApplicationStartedRunner;
//import com.google.inject.Inject;
//import com.google.inject.Singleton;
//import org.fedai.impl.dao.clustermanager.eggroll.StoreLocatorService;
//import org.fedai.entity.clustermanager.eggroll.StoreLocator;
//
//import java.util.Date;
//
//@Singleton
//public class Test implements ApplicationStartedRunner {
//
//    @Inject
//    StoreLocatorService storeLocatorService;
//
//
//    @Override
//    public void run(String[] args) throws Exception {
//        new Thread(()->{
//            try {
//


//                while (true){
//                    QueryWrapper<StoreLocator> queryWrapper = new QueryWrapper<>();
//                    queryWrapper.lambda().eq(StoreLocator::getStoreLocatorId,3);
//                    final long count = storeLocatorService.count(queryWrapper);
//                    System.out.println("count =================> " + count);
//                    Thread.sleep(2000);
//                }
//            }catch (Exception e){
//
//            }
//
//        }).start();
//
//        new Thread(()->{
//            try {
//
//                while (true){
//                    UpdateWrapper<StoreLocator> queryWrapper = new UpdateWrapper<>();
//                    queryWrapper.lambda().set(StoreLocator::getUpdatedAt,new Date()).eq(StoreLocator::getStoreLocatorId,5);
//                    storeLocatorService.update(queryWrapper);
//                    System.out.println("update =================> ");
//                    Thread.sleep(8000);
//                }
//            }catch (Exception e){
//
//            }
//
//        }).start();
//    }
//}
