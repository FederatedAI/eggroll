import{d as g,u as C,a as $,r as P,b as _,o as n,c as l,e as r,f as k,g as a,w as c,h as b,t as m,i as V,_ as x,n as w,j as M,k as N,l as I,F as J,m as A,p as E}from"./index-f0fc38c8.js";const F={class:"header"},z={key:0,class:"user"},D={class:"user-info"},H={class:"name"},L={class:"info"},O=g({__name:"header",setup(y){const t=C(),u=$(),o=P({name:"eggroll",info:"2023-09-13"});async function s(){await u.push("/login")}return(f,i)=>{const d=_("el-button"),e=_("el-popover");return n(),l("header",F,[r("div",{class:"logo",onClick:i[0]||(i[0]=h=>f.$router.push("/home"))}," Eggroll "),k(t).name!=="login"?(n(),l("div",z,[a(e,{trigger:"click"},{reference:c(()=>[b(m(o.name),1)]),default:c(()=>[r("div",D,[r("div",H,m(o.name),1),r("div",L,m(o.info),1),a(d,{onClick:s},{default:c(()=>[b(" 退出 ")]),_:1})])]),_:1})])):V("",!0)])}}});const R=x(O,[["__scopeId","data-v-0e595ecf"]]),S={class:"app-main"},T=g({__name:"appMain",setup(y){const t=C();return(u,o)=>{const s=_("router-view");return n(),l("div",S,[r("div",{class:w(k(t).name!=="login"?"page-content":"")},[a(s)],2)])}}});const j=x(T,[["__scopeId","data-v-6fe12c7c"]]),q={class:"layout"},G=g({__name:"index",setup(y){const t=C(),u=$();let o=M(["Cluster"]),s={home:{path:"/home",name:"Cluster"},processor:{path:"",name:"Processor"},node:{path:"",name:"Job"}};N(t,e=>{s[e.name].path=e.fullPath,d(e)});let f={Cluster:"home",Processor:"processor",Job:"node"};function i(e){const h=f[e],p=s[h];p&&u.push(p.path)}function d(e){e.name==="home"&&(o.value=["Cluster"]),e.name==="processor"&&(o.value=["Cluster","Processor"]),e.name==="node"&&(o.value=["Cluster","Processor","Job"])}return I(()=>{d(t)}),(e,h)=>{const p=_("el-breadcrumb-item"),B=_("el-breadcrumb");return n(),l("div",q,[a(R),a(B,{separator:"/",class:"router-breadcrumb"},{default:c(()=>[(n(!0),l(J,null,A(k(o),v=>(n(),E(p,{class:"router-item",onClick:K=>i(v),key:v},{default:c(()=>[b(m(v),1)]),_:2},1032,["onClick"]))),128))]),_:1}),a(j)])}}});const U=x(G,[["__scopeId","data-v-af06debe"]]);export{U as default};
