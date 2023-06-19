package com.webank.eggroll.core

import com.webank.eggroll.clustermanager.statemechine.ProcessorStateMechine
import org.springframework.context.ApplicationContext

object ContextHolder {
   var  context: ApplicationContext = null
 // var mechine= context.getBean("",ProcessorStateMechine)

}
