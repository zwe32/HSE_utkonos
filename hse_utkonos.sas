/*1. Импорт данных*/
filename source '~/my_content/items.csv' encoding="cp1251" lrecl=32767;
proc import datafile=source out=work.items dbms=csv replace;
  datarow=2;
run;
/*Сырье/Товар ID,Сырье/Товар Наименование,Группа Закупок,Товарная иерархия (уровень 2) ID,
Товарная иерархия (уровень 2) Наименование,Товарная иерархия (уровень 3) ID,Товарная иерархия (уровень 3) Наименование,
Признак участия товара в акции SAP,Акция SAP Вид акции,Признак участия товара в акции сайта,Акция сайта Вид акции,
Признак отмененного заказа,"Дата создания заказа, дата",Продажи в БЕИ,Выручка с НД*/
proc print data=items(obs=5);run;
/*Подготовим названия колонок*/
proc datasets lib=work;
modify items;
  rename
  VAR1 = product
  VAR2 = product_nm
  VAR3 = UI1_nm /*UI=уровень иерархии*/
  VAR4 = UI2
  VAR5 = UI2_nm
  VAR6 = UI3 
  VAR7 = UI3_nm 
  VAR8 = promo_sap
  VAR9 = promo_type_sap
  VAR10= promo_site
  VAR11= promo_type_site
  VAR12= order_cancel
  VAR13=date
  VAR14=sales
  VAR15=margin
  ;
quit;
/*Преобразование типов для продаж и оборота*/
data items_c;
  set items(rename=(sales=_sales margin=_margin));
  drop _sales _margin;
  sales = input(_sales,commax32.);
  margin= input(_margin,commax32.);
run;
/*view dataset*/
proc print data=items_c(obs=5);run;
/*категории*/
proc sql;
  select ui2_nm, count(*) from items_c
  group by ui2_nm;
quit;

filename source '~/my_content/storehouse_available.csv' encoding="cp1251" lrecl=32767;
proc import datafile=source out=avail dbms=csv replace;
  datarow=2;
run;
/*Дата ,Сырье/Товар ID,"Наличие, %"*/
proc datasets lib=work;
modify avail;
  rename
  var1=date
  var2=product
  var3=avail
;
quit;
proc print data=avail(obs=5);run;
data avail_c; /*преобразование типов*/
  set avail(rename=(avail=_avail));
  drop _avail;
  avail = input(_avail,commax32.);
run;
/*-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=*/
/*2. список категорий items_c*/
proc sql;
select distinct ui2_nm, ui3_nm from items_c;
quit;
/*-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=*/
/*3.1 Пример восстановления спроса по дням на обучающей выборке */
/* схлопываем таблицу с продажами до разреза товар-дата*/
proc sql;
  create table denorm as
  select t1.product,t1.product_nm,t1.ui1_nm,t1.ui2,t1.ui2_nm,t1.ui3,t1.ui3_nm,
      max(t1.promo_sap) as promo_sap,max(ifn(t1.promo_site='1',1,0)) as promo_site, max(calculated promo_sap, calculated promo_site) as promo, 
      t1.date,sum(t1.sales) as sales, sum(t1.margin) as margin,calculated margin/calculated sales as price
  from items_c t1 
  where t1.ui1_nm='Яйцо'
  group by t1.product,t1.product_nm,t1.ui1_nm,t1.ui2,t1.ui2_nm,t1.ui3,t1.ui3_nm,t1.date
  order by t1.product,t1.product_nm,t1.ui1_nm,t1.ui2,t1.ui2_nm,t1.ui3,t1.ui3_nm,t1.date;
quit;
/* Протягиваем denorm по всем датам */
proc timeseries data=denorm out=denorm_exp;
id date interval=day start='05JAN2015'd end='03JAN2018'd;
by product product_nm ui1_nm ui2 ui2_nm ui3 ui3_nm;
var sales margin /setmiss=missing;
var promo_sap / setmiss=0;
var promo_site / setmiss=0;
var promo / setmiss=0;
var price / setmiss=previous;
run;
proc sql;
create table denorm_exp1 as
select t1.*, coalesce(t2.avail,ifn(sales>1,100,0)) as avail from 
  denorm_exp as t1 left join avail_c t2
  on t1.date=t2.date and t1.product=t2.product
  order by t1.product,t1.product_nm,t1.ui1_nm,t1.ui2,t1.ui2_nm,t1.ui3,t1.ui3_nm,t1.date;
quit;
/* распределение avail*/
proc univariate data=denorm_exp1;
var avail;
hist avail ;
run;
/* Процесс восстановления:
   Если доступность <def_avail И продажи < avg-2*std за 14 прошлых дней промо/не-промо, восстановить:
     не-промо день: скользящим средним за не более 14 прошлых недифицитных дней не-промо,
   	 промо день   : скользящим средним за не более 14 прошлых недифицитных дней промо */
%let def_avail=80; /*доступность, которая отсекает дефицитные дни. в коде программы вместо "&def_avail" будет проставлено "80" */
%let max_rest_day=7; /*макс. число последовательных дней, для которых сработает восстановление */
proc timedata data=denorm_exp1 outarrays=sales_rest;
id date interval=day; 
by product product_nm ui1_nm ui2 ui2_nm ui3 ui3_nm;
 var sales promo avail price;
   outarrays sales_rest;
   restored=0;
   do t = 2 to _LENGTH_; /*цикл по каждому ряду с одинаковыми значениями "by"*/
    sum_1=0;
    sum_0=0;
    n_1=0;
    n_0=0;
    do j = t-1 to 1 by -1;
      if (avail[j]>=&def_avail and promo[j]=1 and n_1<14 and sales[j] ne .) then do;
        sum_1+sales[j];
        n_1+1;
      end;
      if (avail[j]>=&def_avail and promo[j]=0 and n_0<14 and sales[j] ne .) then do;
        sum_0+sales[j];
        n_0+1;
      end;
    end;  
    sales_1_avg=sum_1/n_1; /*скользящие средние считаются независимо для промо/не-промо, по 14 (или менее) ближайшим дням, где не было дефицита*/
    sales_0_avg=sum_0/n_0;
    sum_1=0;
    sum_0=0;
    n_1=0;
    n_0=0;
    do j = t-1 to 1 by -1;
      if (avail[j]>=&def_avail and promo[j]=1 and n_1<14 and sales[j] ne .) then do;
        sum_1+(sales[j]-sales_1_avg)**2;
        n_1+1;
      end;  
      if (avail[j]>=&def_avail and promo[j]=0 and n_0<14 and sales[j] ne .) then do;
        sum_0+(sales[j]-sales_0_avg)**2;
        n_0+1;
      end;  
    end;
    sales_1_std= sqrt(sum_1/(n_1-1)); /*скользящие стд откл также по 14 ближайшим дням, где не было дефицита*/
    sales_0_std= sqrt(sum_0/(n_0-1));
         if avail[t]<&def_avail and sales[t]<sales_0_avg-sales_0_std and promo[t]=0 then do;
            if restored<=&max_rest_day then sales_rest[t]=sales_0_avg; /*если дефицит или малые продажи - восст. скользящими средними*/
            else sales_rest[t]=sales[t];
            restored+1;
         end;
    else if avail[t]<&def_avail and sales[t]<sales_1_avg-sales_1_std and promo[t]=1 then do;
            if restored<=&max_rest_day then sales_rest[t]=sales_1_avg;
            else sales_rest[t]=sales[t];
            restored+1;
         end;
    else do;
            restored=0;
    		sales_rest[t]=sales[t];
         end;
    end;
run;
/*3.2 Подготовка данных для понедельного прогноза.*/
libname utkns ' /courses/d827f023ba27fe300';
proc sql;
   create table utkns.denorm_week as 
   select product, product_nm, ui1_nm, ui2, ui2_nm, ui3, ui3_nm, sum(promo) as promo, intnx('week.2',date,0) as date format=date9., coalesce(sum(sales_rest),0) as sales, 
   avg(ifn(promo=0,price,.)) as price_reg, avg(ifn(promo=1,price,.)) as price_prom,coalesce(sum(sales),0) as sales_nr
   from sales_rest
   group by product, product_nm, ui1_nm, ui2, ui2_nm, ui3, ui3_nm, calculated date
   order by product, product_nm, ui1_nm, ui2, ui2_nm, ui3, ui3_nm, calculated date
   ;
quit;
/*заметна ли разница между продажами и спросом?*/
proc sgplot data=utkns.denorm_week(where=(date>'1jan2017'd));
  series x=date y=sales;
  series x=date y=sales_nr;
  by product;
run;
/*Какие проблемы у этого алгоритма восстановления спроса?*/
/*Как их исправить?*/
/*3.3 Создание базовых прогнозов */
proc esm data=utkns.denorm_week(where=(date<'20nov2017'd)) out=predict_esm lead=4;
   id date interval=week.2 accumulate=total;
   by product product_nm ui1_nm ui2 ui2_nm ui3 ui3_nm;
   forecast sales ;
run;
proc sql;
  create table to_plot as select 
  t1.*,ifn('20nov2017'd<=t2.date<='11dec2017'd,ifn(t2.sales>1e-3,t2.sales,0),.) as pred_sales from
  denorm_week t1 left join predict_esm t2 on t1.date=t2.date and t1.product=t2.product
  order by product, product_nm, ui1_nm, ui2, ui2_nm, ui3, ui3_nm, date;
quit;
proc sgplot data=to_plot(where=(date>'1jan2017'd));
  series x=date y=sales;
  scatter x=date y=pred_sales;
  by product;
run;
/*-=-=-=-=-=-=-=-=-=-=*/
/* proc timeseries data=denorm_week out=denorm_week1; */
/* id date interval=week.2 start='05JAN2015'd end='03JAN2018'd; */
/* by product product_nm ui1_nm ui2 ui2_nm ui3 ui3_nm; */
/* var price_reg / setmiss=previous; */
/* var promo / setmiss=0;  */
/* var sales ; */
/* run; */

/* proc arima data=denorm_week1 out=predict_arima; */
/*    by product product_nm ui1_nm ui2 ui2_nm ui3 ui3_nm; */
/*    identify var=sales /*crosscorr=(promo price_reg)*/; */
/*    estimate q=(1) p=(1) /*input=( promo price_reg )*/; */
/*    Forecast */
/*    forecast back=7 lead=4 id=date interval=week.2; */
/* run; */
