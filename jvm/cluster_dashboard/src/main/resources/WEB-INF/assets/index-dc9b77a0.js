import{d as z,j as l,r as S,k as A,b as d,c as C,e as b,g as a,w as x,K as I,E as c,o as k,p as y,n as N,_ as w}from"./index-04664782.js";const B=n=>(y("data-v-90f5cf60"),n=n(),N(),n),D={class:"home"},H=B(()=>b("h4",null,"table",-1)),j={class:"table"},E=z({__name:"index",setup(n){const u=l([]),p=S({page:1,pageSize:5}),_=l(1),g=l(5),i=l(0);function v(o){p.pageSize=o,s()}function f(o){p.page=o,s()}function s(){let o={page:p.page,pageSize:p.pageSize};I(o).then(t=>{t.code===0&&(i.value=t.total,u.value=t.data.map(e=>(e.updatedAt=c(e.updatedAt),e.createdAt=c(e.createdAt),e.lastHeartbeatAt=c(e.lastHeartbeatAt),e)))})}return A(()=>{s()}),(o,t)=>{const e=d("el-table-column"),h=d("el-table"),m=d("el-pagination");return k(),C("div",D,[H,b("div",j,[a(h,{data:u.value,border:""},{default:x(()=>[a(e,{prop:"serverNodeId",label:"服务器节点ID"}),a(e,{prop:"name",label:"节点名称"}),a(e,{prop:"serverClusterId",label:"服务器集群 ID"}),a(e,{prop:"host",label:"主机地址"}),a(e,{prop:"port",label:"端口号"}),a(e,{prop:"nodeType",label:"节点类型"}),a(e,{prop:"status",label:"状态"}),a(e,{prop:"lastHeartbeatAt",label:"上次心跳时间"}),a(e,{prop:"createdAt",label:"创建时间"}),a(e,{prop:"updatedAt",label:"更新时间"})]),_:1},8,["data"]),a(m,{class:"pt-16",background:"",layout:"total, sizes, prev, pager, next, jumper","current-age":_.value,"onUpdate:currentAge":t[0]||(t[0]=r=>_.value=r),"page-size":g.value,"onUpdate:pageSize":t[1]||(t[1]=r=>g.value=r),"page-sizes":[5,10,20,50],total:i.value,onSizeChange:v,onCurrentChange:f},null,8,["current-age","page-size","total"])])])}}});const U=w(E,[["__scopeId","data-v-90f5cf60"]]);export{U as default};
