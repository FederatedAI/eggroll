import{d as g,n as f,w as v,_ as C,b as o,c as t,q as r,j as d,f as V,l as b,k as h,i as _,e as k,K as S,F as B}from"./index-fe241cd5.js";const K=g({name:"dialogTable",componentName:"dialogTable",props:{tableData:{type:Array,default:[]},dialogVisible:{type:Boolean,default:!1},title:{type:String,default:""},tableLabel:{type:Array,default:[]},width:{type:String,default:"90%"},needSearch:{type:Boolean,default:!1}},setup(e,a){const i=f(!1),n=f("");function u(){n.value="",a.emit("refresh")}function c(){a.emit("updateVisiable",!0)}function p(){a.emit("searchConfig",n.value)}return v(()=>e.dialogVisible,s=>{i.value=s}),{onRefresh:u,onClose:c,visible:i,searchKey:n,onSearch:p}}});const N={class:"action-header"};function T(e,a,i,n,u,c){const p=o("el-input"),s=o("el-button"),m=o("el-table-column"),y=o("el-table"),w=o("el-dialog");return t(),r(w,{modelValue:e.visible,"onUpdate:modelValue":a[1]||(a[1]=l=>e.visible=l),onClose:e.onClose,title:e.title,width:e.width},{default:d(()=>[V("div",N,[e.needSearch?(t(),r(p,{key:0,class:"input-width",clearable:"",modelValue:e.searchKey,"onUpdate:modelValue":a[0]||(a[0]=l=>e.searchKey=l),modelModifiers:{trim:!0},placeholder:"search Key"},null,8,["modelValue"])):b("",!0),e.needSearch?(t(),r(s,{key:1,class:"btn-refresh",type:"primary",onClick:e.onSearch},{default:d(()=>[h("Search")]),_:1},8,["onClick"])):b("",!0),_(s,{class:"btn-refresh",type:"primary",onClick:e.onRefresh},{default:d(()=>[h("Refresh")]),_:1},8,["onClick"])]),_(y,{data:e.tableData,class:"dialog-table"},{default:d(()=>[(t(!0),k(B,null,S(e.tableLabel,l=>(t(),r(m,{"show-overflow-tooltip":["SessionId","ProcessorOption"].includes(l.label),key:l,prop:l.prop,label:l.label,width:l.width?l.width:l.label==="SessionId"?"380px":l.label.includes("At")?"170px":"",fixed:l.label==="ProcessorOption"?"right":null},null,8,["show-overflow-tooltip","prop","label","width","fixed"]))),128))]),_:1},8,["data"])]),_:1},8,["modelValue","onClose","title","width"])}const A=C(K,[["render",T],["__scopeId","data-v-2e1c9d8a"]]);export{A as d};
