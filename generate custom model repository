Proc HPFARIMASPEC
	/* Generated at: Apr 17, 2019 9:04:24 AM
	 * Model: SUBSETARIMA
	 * Label: sales = P=( 1 ) D=(52) Q=( 52 )  + price_reg : NUM=( 1 2 ) D=(52) DEN=( 1 ) 
	 */ 
	
	/*перед запуском кода добавьте библиотеку с именем modelrep*/
	MODELREPOSITORY = modelrep.utkonos_simple /*Lib.ReporsitoryName*/
	SPECNAME=AM_PD52Q52_PR_NUM2DEN /*Model name, длина не более 20 символов!*/
	SPECLABEL="ARIMA:  sales  ~ P = 1  D = (52)  Q = (52)   +  INPUT: Dif(52) price_reg  NUM = 2  DEN = 1"
	SPECTYPE=SUBSETARIMA
	SPECSOURCE=FSUI
	; 
FORECAST SYMBOL = sales TRANSFORM = NONE
	DIF = ( 52 ) 
	P = ( 1 )
	Q = ( 52 ) ; 
INPUT SYMBOL = price_reg
	TRANSFORM = NONE
	DIF = ( 52 ) 
	NUM = ( 1 2 ) 
	DEN = ( 1 );
ESTIMATE 
	METHOD=CLS 
	CONVERGE=0.001 
	MAXITER=50 
	DELTA=0.001 
	SINGULAR=1.0E-7  ; 
run;


proc hpfselect modelrep= modelrep.utkonos_simple name=select;
	spec AM_PD52Q52_PR_NUM2DEN;
run;
