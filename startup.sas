options notes;
libname utkns '/courses/d827f023ba27fe300'; /*Директория курса*/
data work.denorm_week;
set utkns.denorm_week; /*если исходная таблица называется по-другому, поменять*/
run;
