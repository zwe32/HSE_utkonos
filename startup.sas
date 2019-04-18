options notes;

/* Подготовка таблицы исходных данных для Forecast Studio */
libname utkns '/courses/d827f023ba27fe300'; /*Директория курса, где лежат предобработанные данные*/
data work.denorm_week; /*название целевой таблицы в work (на стороне Forecast Studio)*/
set utkns.denorm_week; /*если исходная таблица называется по-другому, поменять*/
run;

/* Добавление репозитория моделей в Forecast Studio */
libname modelrep '/home/alexromsput0/sasuser.v94'; /*Директория где находится каталог моделей*/
proc catalog cat=modelrep.utkonos_simple; /* здесь указано, что переносим в FS environment, utkonos_simple - название исходного репозитория моделе*/ 
copy out=SASUSER.utkonos_simple; /*куда кладём репозиторий моделей, SASUSER - дефолтная библиотека для репозитория моделей*/
run;
quit;
