package com.webank.eggroll.clustermanager.conf;




import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Component
public class ExtendConfigPostProcessor implements EnvironmentPostProcessor {
  Logger logger = LoggerFactory.getLogger(ExtendConfigPostProcessor.class);

  public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
    try {

//        eggroll.resourcemanager.clustermanager.jdbc.driver.class.name=com.mysql.cj.jdbc.Driver
//#eggroll.resourcemanager.clustermanager.jdbc.url=jdbc:h2:./data/meta_h2/eggroll_meta.h2;AUTO_SERVER=TRUE;MODE=MySQL;DATABASE_TO_LOWER=TRUE;SCHEMA=eggroll_meta;
//        eggroll.resourcemanager.clustermanager.jdbc.url=jdbc:mysql://localhost:3306/eggroll_meta?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8&allowPublicKeyRetrieval=true
//        eggroll.resourcemanager.clustermanager.jdbc.username=
//       eggroll.resourcemanager.clustermanager.jdbc.password=

//        spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
//        spring.datasource.url=jdbc:mysql://[#dbaddress]/[#dbname]?characterEncoding=utf8&characterSetResults=utf8&autoReconnect=true&failOverReadOnly=false&serverTimezone=GMT%2B8
//        spring.datasource.username=[#dbuser]
//        spring.datasource.password=[#dbpwd]
        MutablePropertySources propSrcs = environment.getPropertySources();

        Map<String, String> props = StreamSupport.stream(propSrcs.spliterator(), false)
                .filter(ps -> ps instanceof EnumerablePropertySource)
                .map(ps -> ((EnumerablePropertySource) ps).getPropertyNames())
                .flatMap(Arrays::stream)
                .distinct()
                .collect(Collectors.toMap(Function.identity(), environment::getProperty));

//        // key 和 value 之间的最小间隙
//        int interval = 20;
//        int max = props.keySet().stream()
//                .max(Comparator.comparingInt(String::length))
//                .orElse("")
//                .length();
//
//        // 打印
//        props.keySet().stream()
//                .sorted()
//                .forEach(k -> {
//                    int i = max - k.length() + interval;
//                    String join = String.join("", Collections.nCopies(i, " "));
//                //    System.out.println(String.format("%s%s%s", k, join, props.get(k)));
//                });



      //"spring.datasource.druid.password"

//      String isEncrypt = environment.getProperty("spring.datasource.druid.master.encrypt");
//      //String privateKey = environment.getProperty("spring.datasource.druid.master.privatekey");
//      System.err.println("PassWordEncryptPostProcessor begin "+isEncrypt);
//      if ("true".equals(isEncrypt) ) {
        Properties properties = new Properties();


        properties.put("spring.datasource.url",environment.getProperty("eggroll.resourcemanager.clustermanager.jdbc.url") );
        properties.put("spring.datasource.username",environment.getProperty("eggroll.resourcemanager.clustermanager.jdbc.username") );
        properties.put("spring.datasource.password", environment.getProperty("eggroll.resourcemanager.clustermanager.jdbc.password"));
        properties.put("spring.datasource.driver-class-name",environment.getProperty("eggroll.resourcemanager.clustermanager.jdbc.driver.class.name"));

        String driverClass = environment.getProperty("eggroll.resourcemanager.clustermanager.jdbc.driver.class.name");

        String encryptPwd = environment.getProperty("spring.datasource.password");
//        Iterator<PropertySource<?>>   iterator = environment.getPropertySources().iterator();
////        for(;iterator.hasNext();){
////           System.err.println( iterator.next());
////        }
       environment.getPropertySources().addFirst((PropertySource)new PropertiesPropertySource("dbProperties", properties));
     // }
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }




}