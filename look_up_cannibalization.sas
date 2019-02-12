libname utkns ' /courses/d827f023ba27fe300';

/*Кусочек общей картины*/
proc sgplot data=utkns.denorm_week(where=(date>'1jan2017'd));
  series x=date y=sales/group= product;
run;
/*Получим топ-10 продуктов в группе "Яйца" по сумме продаж*/
%let ui=ui2;
proc sql outobs=10;
  create table prod as select product, ui3, ui2,sum(sales) as um_sales from utkns.denorm_week
  where date>'1jan2017'd
  group by 1,2,3
  order by 4 desc
  ;
quit;

/*Найдем товары-каннибализаторы, которые отъедают спрос в ответ на определённые драйверы*/
/*важно - мы должны знать драйверы, используемые для разметки каннибализации,
  в том числе на будущее.
  Упражнение: в качестве ретроспективного эксперимента попробуйте использовать, напр., дефицит */

/*Агрегаты по группе УИ*?*, исключая определённый товар*/
proc sql;
   create table group_no_product as 
   select t2.product,t2.&ui,date,sum(sales_nr) as sum_&ui,median(price_reg) as med_price_reg,
   median(price_prom) as med_price_prom, sum(promo) as sum_promo, sum(promo_site) as sum_promo_site, sum(promo_sap) as sum_promo_sap
   from utkns.denorm_week t1 cross join prod t2
   where t2.product^=t1.product and t2.&ui=t1.&ui
   group by t2.product,t2.&ui,date;
 quit;
 
 proc sql;
   create table product_plus_group as
   select t1.sum_&ui, t1.med_price_reg, t1.med_price_prom, t1.sum_promo, t1.sum_promo_sap, t1.sum_promo_site,
   t2.product, t2.date, t2.promo, t2.promo_site, t2.promo_sap, t2.price_reg, t2.price_prom, t2.sales_nr, t2.price_fact
   from group_no_product t1 inner join utkns.denorm_week t2 on
   (t1.product=t2.product and t1.date=t2.date)
   order by t2.product, t2.date;
 quit;
 
 proc sgplot data=product_plus_group;
    series x=date y=sum_&ui;
    series x=date y=sales_nr ;
    series x=date y=promo_sap / y2axis;
    series x=date y=promo_site / y2axis;
    *series x=date y=price_fact / y2axis;
    *series x=date y=med_price_reg / y2axis;
    by product;
   run;quit;
   proc sgplot data=product_plus_group;
    series x=date y=sum_&ui;
    series x=date y=sales_nr ;
    *series x=date y=promo / y2axis; /*1 driver*/
    series x=date y=price_fact / y2axis;
    series x=date y=med_price_reg / y2axis;
    by product;
   run;quit;
  /* Разметка каннибализации */
proc expand data=product_plus_group out=prod_mkup from=week.2;
  by product;
  id date ;
  convert sales_nr=l1_sales_nr / transformout=(lag 1);
  convert sales_nr=d1_sales_nr / transformout=(dif 1);
  convert sum_&ui=l1_sum_ui / transformout=(lag 1);
  convert sum_&ui=d1_sum_ui / transformout=(dif 1);
  convert price_fact=d1_price_fact / transformout=(dif 1);
  convert promo_sap =d1_promo_sap  / transformout=(dif 1);
  convert price_fact=l1_price_fact / transformout=(lag 1 movave 4);
  convert promo_sap =l1_promo_sap  / transformout=(lag 1);
run;
/* документация к proc expand:
 * https://documentation.sas.com/?docsetId=etsug&docsetTarget=etsug_expand_details19.htm&docsetVersion=14.3&locale=en
 */
proc sql;
  create table prod_mkup as select 
  ifn(promo_sap>1 and promo_sap>0 and price_fact<0.9*l1_price_fact,1,0) as mkup, t1.* 
  /*на товар было ценовое промо И цена упала больше чем на 10% отн скользящего среднего за -1..-4 недели*/
  from prod_mkup t1;
quit;

proc sgplot data=prod_mkup;
  series x=date y=sum_&ui / lineattrs=(color=red);
  series x=date y=sales_nr / lineattrs=(color=green);
  series x=date y=promo_sap / y2axis; 
  NEEDLE X=date Y=mkup / y2axis lineattrs=(color=orange) markers markerattrs=(color=orange );
  by product;
run;quit;
