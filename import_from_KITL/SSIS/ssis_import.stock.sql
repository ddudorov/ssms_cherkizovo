/************************************************************************************************/
/* ssis_import.stock - Хранимая процедура импорта из КИТ-Логистика через SSIS-пакет				*/
/* Дудоров ДА | 202001																			*/																	
/************************************************************************************************/
ALTER PROCEDURE ssis_import.stock
(
	 @XML_in			XML		 = '[root]1[/root]'	-- параметр из SSIS пакета
	,@stock_on_date		DATE	 = '19000101'
)

AS

SET NOCOUNT ON;	

--IF  @stock_on_date = '19000101'
--SET @stock_on_date = GETDATE()

DECLARE @hdoc INT;

EXEC SP_XML_PREPAREDOCUMENT @hdoc OUTPUT, @XML_in;

	SELECT 		 
				 products_id			
				,product_id				
				,product_name			
				,box_id					
				,box_name			
				,placement_id			
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
				,server
				,stock_on_date
INTO #temp_stock		
								
FROM OPENXML(@hdoc, '/ROOT/row',2)
	WITH
		(
		 products_id				INT
	    ,product_id					INT
	    ,product_name				VARCHAR(300)
	    ,box_id						INT
	    ,box_name					VARCHAR(300)
		,placement_id				INT
	    ,production_shop_id			VARCHAR(300)
	    ,production_shop_name		VARCHAR(300)
	    ,storage_area_id			INT
	    ,storage_area_name			VARCHAR(300)
	    ,production_date			DATETIME
	    ,box_quantity				INT
	    ,product_quantity			INT
	    ,product_net_kg				DECIMAL(15,3)
	    ,job_id						INT
	    ,job_name					VARCHAR(300)
	    ,expiration_date_in_days	INT
	    ,production_id				INT
	    ,production_name			VARCHAR(300)
		,sap_mdg					VARCHAR(30)
		,server						VARCHAR(30)
		,stock_on_date				DATETIME
		)

EXEC sp_xml_removedocument @hdoc

DECLARE @server VARCHAR(30)
SET @server = (SELECT TOP 1 server FROM #temp_stock)

-- Заполняем Справочник по заводам и местам хранения
INSERT INTO import_from_KITL.info.placement (production_id, production_name,production_shop_id,production_shop_name,storage_area_id,storage_area_name)
SELECT DISTINCT  s.production_id
			    ,s.production_name
				,CASE WHEN s.production_shop_id IN ('',0,' ') THEN NULL ELSE production_shop_id END AS 'production_shop_id'
				,CASE WHEN production_shop_name IN ('',' ')   THEN NULL ELSE production_shop_name END AS 'production_shop_name'
				,s.storage_area_id
				,s.storage_area_name
FROM #temp_stock AS s
WHERE not exists (select * 
				  FROM import_from_KITL.info.placement AS p
				  where ISNULL(s.production_id,0)			= ISNULL(p.production_id,0) 
				    AND ISNULL(s.production_shop_id, 0)		= ISNULL(p.production_shop_id, 0) 
					AND ISNULL(s.storage_area_id,0)			= ISNULL(p.storage_area_id,0));


-- Заполняем Справочник по продукции + короба
INSERT INTO import_from_KITL.info.products (sap_mdg_id,sap_id,product_name,unit_id,box_id,box_name)
SELECT DISTINCT	 s.sap_mdg
				,ps.sap_id
				,s.product_name
				,s.product_id
				,s.box_id
				,s.box_name
FROM #temp_stock AS s
LEFT JOIN cherkizovo.info.products_sap AS ps ON 
s.sap_mdg = ps.product_finished_id
WHERE not exists (SELECT * 
				  FROM import_from_KITL.info.products AS p
				  WHERE ISNULL(s.sap_mdg,0)			= ISNULL(p.sap_mdg_id,0)
					AND ISNULL(s.product_id,0)		= ISNULL(p.unit_id,0)
					AND ISNULL(s.box_id,0)			= ISNULL(p.box_id,0));

DELETE import_from_KITL.hist.stock 
WHERE  stock_on_date = @stock_on_date
AND    server		 = @server;

INSERT INTO import_from_KITL.hist.stock 
									 ( 								
											  products_id		
											 ,placement_id									
											 ,job_id				
											 ,job_name			
											 ,production_date	
											 ,expiration_date_in_days
											 ,box_quantity		
											 ,product_quantity	
											 ,product_net_kg	
											 ,server 
											 ,stock_on_date
									  )
							SELECT			  
											  p.products_id		
											 ,sa.placement_id									
											 ,s.job_id
											 ,s.job_name		
											 ,CAST(s.production_date AS date)
											 ,s.expiration_date_in_days
											 ,SUM(s.box_quantity)		
											 ,SUM(s.product_quantity)	
											 ,SUM(s.product_net_kg)		
											 ,MAX(s.server)
											 ,MAX(s.stock_on_date)
FROM #temp_stock AS s
JOIN import_from_KITL.info.placement AS sa
ON s.production_id = sa.production_id AND ISNULL(s.production_shop_id, 0) = ISNULL(sa.production_shop_id, 0) AND s.storage_area_id = sa.storage_area_id
JOIN  import_from_KITL.info.products AS p
ON ISNULL(s.sap_mdg,0) = ISNULL(p.sap_mdg_id,0) AND ISNULL(s.product_id,0) = ISNULL(p.unit_id,0) AND ISNULL(s.box_id,0) = ISNULL(p.box_id,0)
GROUP BY   p.products_id		
		  ,sa.placement_id									
		  ,s.job_id				
		  ,s.job_name	
		  ,CAST(s.production_date AS date)
		  ,s.expiration_date_in_days;


---------------------------------------------------
-- ОТПРАВКА ПИСЬМА
---------------------------------------------------

	--IF @count_hist_stock < @count_temp_stock
	--BEGIN 
	
			-- рощин МЮ \/\/\/
			declare @body varchar(max);
			declare @time_stop time(0); 

			set @body = (SELECT '<table><tr><th>Сервер</th><th>Остатки на дату</th><th>Кол-во строк</th><th>Сумма, кг</th></tr>' + STRING_AGG(table1, '') + '</table>'
						FROM 
						(
							SELECT top 1000
									'<tr ' + iif(convert(date, h.stock_on_date) = convert(date, s.stock_on_date), 'id="select_row"', '') + ' ><td>' + h.server + '</td>
									<td>' + ISNULL(FORMAT(h.stock_on_date, 'dd.MM.yyyy'), 'Нет даты') + '</td>
									<td>' + FORMAT(COUNT(*), '### ###' ) + '</td>
									<td>' + FORMAT(SUM(h.product_net_kg), '### ### ###') + '</td></tr>' AS table1
							FROM import_from_KITL.hist.stock as h
							join (select top 1 s.server, s.stock_on_date
								  from #temp_stock as s) as s on  h.server = s.server and h.stock_on_date between  DATEADD(day,-10, s.stock_on_date) and  DATEADD(day,10, s.stock_on_date)
							GROUP BY h.server, h.stock_on_date, s.stock_on_date
							ORDER BY h.server, h.stock_on_date
						) AS t)

			set @time_stop = getdate();
--			set @body = 'Время выполнения: ' + format( DATEDIFF(second,	@time_start, @time_stop) / 3600 		,'00') + ':' + 
--											   format( DATEDIFF(second, @time_start, @time_stop) % 3600 / 60 	,'00') + ':' + 
--											   format( DATEDIFF(second, @time_start, @time_stop) % 60 			,'00') + '<br>' + isnull(@body, '')

			DECLARE @text VARCHAR(150) = 'Остатки КИТ-Логистика ' + @server + ' на ' + CONVERT(VARCHAR(30),@stock_on_date, 104)

			exec monitoring.dbo.send_mail @subject = @text, @body = @body

