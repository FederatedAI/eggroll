package com.webank.eggroll.clustermanager.statemechine;


import com.eggroll.core.constant.SessionEvents;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.pojo.ErSessionMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class StateMachineBuilderConfig {







    public    StateMachine orderOperaMachine() {
        String sessionStateMachine = "sessionStateMachine";
        StateMachineBuilder<SessionStatus, SessionEvents, ErSessionMeta> builder = StateMachineBuilderFactory.create();
        //订单从初始化状态-待发货-状态-转到-关闭订单状态--用户关闭
        builder.externalTransitions()
                .fromAmong(SessionStatus.NEW)
                .to(SessionStatus.ACTIVE)
                .on(SessionEvents.ACTIVE)
                .when(checkCondition())
                .perform(doAction());
//        //订单从-初始化状态-已发货-待发货--转到-关闭订单状态--后台操作人员关闭
//        builder.externalTransitions()
//                .fromAmong(OrderStatusEnum.INIT, OrderStatusEnum.HAVE_BEEN_DELIVERY, OrderStatusEnum.WAITING_FOR_DELIVERY)
//                .to(OrderStatusEnum.CLOSE)
//                .on(OrderEvent.ADMIN_CLOSE)
//                .when(checkCondition())
//                .perform(doAction());
//        //订单从等待发货状态-转为-订单关闭状态-超时关闭
//        builder.externalTransition()
//                .from(OrderStatusEnum.WAITING_FOR_DELIVERY)
//                .to(OrderStatusEnum.CLOSE)
//                .on(OrderEvent.OVERTIME_CLOSE)
//                .when(checkCondition())
//                .perform(doAction());
//        //订单从待发货状态--转为-订单关闭状态-上级审批不通过关闭
//        builder.externalTransition()
//                .from(OrderStatusEnum.WAITING_FOR_DELIVERY)
//                .to(OrderStatusEnum.CLOSE)
//                .on(OrderEvent.CHECK_ERROR_CLOSE)
//                .when(checkCondition())
//                .perform(doAction());
//        //订单从初始化状态--转为待发货状态--用户支付完毕动
//        builder.externalTransition()
//                .from(OrderStatusEnum.INIT)
//                .to(OrderStatusEnum.WAITING_FOR_DELIVERY)
//                .on(OrderEvent.USER_PAY)
//                .when(checkCondition())
//                .perform(doAction());

        StateMachine stateMachine = builder.build(sessionStateMachine);


        //打印uml图
        //String plantUML = orderOperaMachine.generatePlantUML();
     //   System.out.println(plantUML);
        return stateMachine;
    }

    private Condition<ErSessionMeta> checkCondition() {
        return (ctx) -> {
            return true;
        };
    }

    private Action<SessionStatus, SessionEvents, ErSessionMeta> doAction() {
        return (from, to, event, ctx) -> {
            System.out.println(ctx.getId() + " 正在操作 " + ctx.getId() + " from:" + from + " to:" + to + " on:" + event);
        };
    }


    public  static  void main(String[] args){
        StateMachineBuilderConfig  config = new  StateMachineBuilderConfig();
        StateMachine  stateMachine = config.orderOperaMachine();
        stateMachine.fireEvent(SessionStatus.NEW,SessionEvents.CREATE,new ErSessionMeta());
    }

}