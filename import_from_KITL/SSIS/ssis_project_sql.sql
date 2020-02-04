/******************************************************************/
/* Шаг 1 EXEC SP BO_rep_listcontents4							  */
/* Переменная insert_exec_sp									  */
/* Дудоров ДА | 202001											  */
/******************************************************************/

DECLARE @stock_on_date VARCHAR(20)					-- SQL server

SET @stock_on_date = '" +  @[User::date]   + "'
CREATE TABLE #tmp
(

	 product_id							int					NULL
	,product_name						varchar(300)		NULL
	,box_id								int					NULL
	,box_short_name						varchar(300)		NULL
	,box_name							varchar(300)		NULL
	,production_shop_id					int					NULL
	,production_shop_name				varchar(300)		NULL
	,storage_area_id					int					NULL
	,storage_area_name					varchar(300)		NULL
	,partner_id							int					NULL
	,partner_name						varchar(300)		NULL
	,production_date					datetime			NULL
	,box_quantity						int					NULL
	,product_quantity					int					NULL
	,product_net_kg						decimal(15, 3)		NULL
	,job_id								int					NULL
	,job_name							varchar(300)		NULL
	,product_out						varchar(300)		NULL
	,job_partner						int					NULL
	,job_partnername					varchar(300)		NULL
	,box_id_out							varchar(300)		NULL
	,kod_tm								varchar(300)		NULL
	,expiration_date_in_days			int					NULL
	,priz_new_prod						int					NULL
	,priz_op							int					NULL
	,priz_br							int					NULL
	,srok_op							int					NULL
	,srok_br							int					NULL
	,production_id						int					NULL
	,production_name					varchar(300)		NULL
	,server								varchar(50)			NULL
	,sap_mdg							varchar(50)			NULL
	,kod_1C								varchar(50)			NULL
	,stock_on_date						datetime			NULL
); 

BEGIN TRANSACTION;


INSERT INTO #tmp
				( 
					 product_id
					,product_name
					,box_id
					,box_short_name
					,box_name
					,production_shop_id
					,production_shop_name	
					,storage_area_id
					,storage_area_name
					,partner_id
					,partner_name
					,production_date
					,box_quantity
					,product_quantity		
					,product_net_kg
					,job_id
					,job_name
					,product_out
					,job_partner
					,job_partnername
					,box_id_out
					,kod_tm
					,expiration_date_in_days
					,priz_new_prod
					,priz_op
					,priz_br
					,srok_op
					,srok_br
					,production_id
					,production_name
				)
exec Logistic_chmpz1.dbo.BO_rep_listcontents4  @ctime = @stock_on_date, @outsys=5,@var=6, @report = 59217;



COMMIT TRANSACTION;
UPDATE #tmp					-- Добавляем sap_mdg_id
SET sap_mdg = Logistic_chmpz1.dbo.GetExtNumber(33,product_id,box_id,production_id,2)


SELECT				 product_id
					,product_name
					,box_id
					,box_name
					,production_shop_id
					,production_shop_name	
					,storage_area_id
					,storage_area_name
					,production_date
					,box_quantity
					,product_quantity		
					,product_net_kg
					,job_id
					,job_name
					,expiration_date_in_days
					,production_id
					,production_name
					,sap_mdg
					,'59217' AS report
					,'SQL' AS server
					,CAST(@stock_on_date AS DATETIME) AS stock_on_date
FROM #tmp
FOR XML PATH('row');

--------------------------------------------------------------------------------

DECLARE @stock_on_date VARCHAR(20)		-- SQL_defect server

SET @stock_on_date = '" +  @[User::date]   + "'
CREATE TABLE #brak_chmpz
(
	 numberPal				VARCHAR(200)
	,namePal				VARCHAR(200)
	,number				VARCHAR(200)
	,resource				INT
	,resoursename			VARCHAR(200)
	,tara					INT
	,taraname				VARCHAR(200)
	,kol_tara				INT
	,netto_prod				DECIMAL(18,3)
	,gross_prod				DECIMAL(18,3)
	,kol_prod				INT
	,kod_kit				INT
	,makingdate				DATETIME
	,srok_godn				INT
	,server					VARCHAR(30)
	,sp_skl					VARCHAR(30)
	,parties_id				INT
	,production_shop_id		VARCHAR(90)
	,production_shop_name	VARCHAR(90)
	,manufacturer			INT
	,sp_name				VARCHAR(90)
	,manufacturer_name		VARCHAR(90)
	,sap_mdg_id				VARCHAR(30)
	,stock_on_date			DATETIME
);

INSERT INTO #brak_chmpz		
						(
							 numberPal
							,namePal			
							,number			
							,resource			
							,resoursename		
							,tara				
							,taraname			
							,kol_tara			
							,netto_prod			
							,gross_prod			
							,kol_prod			
							,kod_kit			
							,makingdate
						) 
EXEC Logistic_chmpz1.dbo.BO_rep_resource @datetime = @stock_on_date, @spmx = 102727144;

UPDATE #brak_chmpz
SET sp_skl  = '102727144',
	sp_name = 'ОСБ Склад брака по причинам (ВЕТВРАЧИ)'

INSERT INTO #brak_chmpz		
						(
							 numberPal
							,namePal			
							,number			
							,resource			
							,resoursename		
							,tara				
							,taraname			
							,kol_tara			
							,netto_prod			
							,gross_prod			
							,kol_prod			
							,kod_kit			
							,makingdate
						) 
EXEC Logistic_chmpz1.dbo.BO_rep_resource @datetime = @stock_on_date, @spmx = 227094806;

DELETE #brak_chmpz
WHERE resource IS NULL;

UPDATE #brak_chmpz		-- sp_skl
SET  sp_skl  = '227094806'
	,sp_name = 'ОСБ П/Ф Склад брака по причинам (ВЕТВРАЧИ)'
WHERE sp_skl IS NULL;

UPDATE #brak_chmpz		-- server
SET server = 'SQL_defect';

UPDATE #brak_chmpz		-- parties_id
SET parties_id = Logistic_chmpz1.dbo.GetIntNumSP(number)

UPDATE #brak_chmpz		-- srok_godn
	SET srok_godn = (SELECT TOP 1 CASE WHEN ISNUMERIC(vcfield3)=1 THEN vcfield3 ELSE 300 END 
					 FROM Logistic_chmpz1.dbo.unitproperties u 
					 WHERE u.unitproptype=417 AND u.unit= resource);

UPDATE #brak_chmpz		-- manufacturer
SET manufacturer = p.manufacturer
FROM #brak_chmpz AS b
LEFT JOIN Logistic_chmpz1.dbo.parties p 
ON p.spl =	(SELECT TOP 1 batch 
						 FROM Logistic_chmpz1.dbo.reshandling r 
						 WHERE r.type=3 AND r.splaceto  = b.parties_id);

UPDATE #brak_chmpz		-- manufacturer_name, sap_mdg_id
SET	 manufacturer_name = Logistic_chmpz1.dbo.getunitname(manufacturer)
	,sap_mdg_id = Logistic_chmpz1.dbo.GetExtNumber(33,resource,tara,manufacturer,2);

UPDATE #brak_chmpz
SET stock_on_date = @stock_on_date


SELECT *
FROM 
	(
SELECT 				 resource		AS product_id
					,resoursename	AS product_name
					,tara			AS box_id
					,taraname		AS box_name	
					,sp_skl			AS storage_area_id
					,sp_name		AS storage_area_name
					,makingdate		AS production_date
					,SUM(kol_tara)		AS box_quantity
					,SUM(kol_prod)		AS product_quantity		
					,SUM(netto_prod)	AS product_net_kg
					,srok_godn		AS expiration_date_in_days
					,manufacturer	AS production_id
					,manufacturer_name AS production_name
					,sap_mdg_id		AS sap_mdg
					,server
					,stock_on_date

FROM #brak_chmpz
GROUP BY			 resource		
					,resoursename	
					,tara			
					,taraname		
					,sp_skl			
					,sp_name		
					,makingdate		
					,srok_godn		
					,manufacturer	
					,manufacturer_name
					,sap_mdg_id
					,server
					,stock_on_date
	) AS T
FOR XML PATH('row');




/******************************************************************/
/* Шаг 2 EXEC SP BO_rep_listcontents4							  */
/* Переменная exec_import_xml_sp							  */
/* Дудоров ДА | 202001											  */
/******************************************************************/

EXEC ssis_import.stock" + " '" +   @[User::sql_xml]    + "'" + ","   + "'" +  @[User::date]      + "'			-- SQL
EXEC ssis_import.stock" + " '" +   @[User::sql_defect_xml]    + "'" + ","   + "'" +   @[User::date]    + "'		-- SQL_defect
EXEC ssis_import.stock" + " '" +  @[User::ulyan_xml]    + "'" + ","   +  "'" +   @[User::date]     + "'			-- SQLLOG_ULYAN
EXEC ssis_import.stock" + " '" +  @[User::pmpk_xml]   + "'" + ","   + "'" +   @[User::date]     + "'			-- SQLLOG_PMPK
EXEC ssis_import.stock" + " '" +  @[User::evraz_xml]    + "'" + ","   + "'" +   @[User::date]     + "'			-- SQLLOG_EVRAZ
EXEC ssis_import.stock" + " '" + @[User::bmpk_xml]   + "'" + ","   + "'" +   @[User::date]     + "'				-- SQLLOG_BIKOM