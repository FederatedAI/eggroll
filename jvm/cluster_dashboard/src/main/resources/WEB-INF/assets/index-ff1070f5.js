import{d as z,n as s,r as S,o as I,b as d,e as x,f as b,i as a,j as C,V as A,Q as g,c as D,v as y,x as N,_ as k}from"./index-bb03f45a.js";const B=p=>(y("data-v-8d308bea"),p=p(),N(),p),E={class:"home"},P=B(()=>b("h4",null,"table",-1)),V={class:"table"},j=z({__name:"index",setup(p){const c=s([]),n=S({page:1,pageSize:5}),i=s(1),u=s(5),_=s(0);function v(o){n.pageSize=o,l()}function m(o){n.page=o,l()}function l(){let o={pageNum:n.page,pageSize:n.pageSize};A(o).then(t=>{t.code===0&&(_.value=t.total,c.value=t.data.map(e=>(e.updatedAt=g(e.updatedAt),e.createdAt=g(e.createdAt),e)))})}return I(()=>{l()}),(o,t)=>{const e=d("el-table-column"),f=d("el-table"),h=d("el-pagination");return D(),x("div",E,[P,b("div",V,[a(f,{data:c.value,border:""},{default:C(()=>[a(e,{prop:"processorId",label:"处理器 ID"}),a(e,{prop:"sessionId",label:"会话 ID"}),a(e,{prop:"serverNodeId",label:"服务器节点 ID"}),a(e,{prop:"processorType",label:"处理器类型"}),a(e,{prop:"tag",label:"标签"}),a(e,{prop:"commandEndpoint",label:"命令端点"}),a(e,{prop:"status",label:"状态"}),a(e,{prop:"transferEndpoint",label:"传输端点"}),a(e,{prop:"processorOption",label:"处理器选项"}),a(e,{prop:"pid",label:"进程 ID"}),a(e,{prop:"createdAt",label:"创建时间"}),a(e,{prop:"updatedAt",label:"更新时间"})]),_:1},8,["data"]),a(h,{class:"pt-16",background:"",layout:"total, sizes, prev, pager, next, jumper","current-age":i.value,"onUpdate:currentAge":t[0]||(t[0]=r=>i.value=r),"page-size":u.value,"onUpdate:pageSize":t[1]||(t[1]=r=>u.value=r),"page-sizes":[5,10,20,50],total:_.value,onSizeChange:v,onCurrentChange:m},null,8,["current-age","page-size","total"])])])}}});const U=k(j,[["__scopeId","data-v-8d308bea"]]);export{U as default};
