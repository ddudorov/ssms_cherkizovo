use project_plan_production_finished_products

go

select * from project_plan_production_finished_products.data_import.transits
--drop table project_plan_production_finished_products.data_import.transits
create table project_plan_production_finished_products.data_import.transits
(

		 row_id							INT					NOT NULL	IDENTITY(1,1)
		,name_table						as 'transits'
		,reason_ignore_in_calculate		VARCHAR(300)			NULL
		,product_status					varchar(100)			NULL

		,sap_id							BIGINT					NULL
		,sap_id_text					as convert(varchar(24), FORMAT(sap_id, '000000000000000000000000'))

		,stuffing_id					VARCHAR(40)				NULL
		,product_1C_full_name			varchar(200)		NOT NULL

		--- остатки ---
		,stock_production_date			DATETIME			NOT NULL
		,stock_on_date					DATETIME			NOT NULL
		,stock_expiration_date			DATETIME				NULL
		,stock_current_KOS				as case when stock_production_date >= stock_expiration_date then null  
												when stock_production_date >  stock_on_date		    then null
												when stock_expiration_date <= stock_on_date		    then 0.000000	
												when stock_production_date	= stock_on_date			then 1
												else DATEDIFF(day, stock_on_date, stock_expiration_date) * 1.0 / DATEDIFF(day, stock_production_date, stock_expiration_date) end

		,stock_KOS_in_day				as case when stock_production_date >= stock_expiration_date then null	
												when stock_production_date >  stock_on_date			then null
												else 1.0 / DATEDIFF(day, stock_production_date, stock_expiration_date) end	
		-- расчетные поля
		,stock_kg						dec(11,5)			NOT NULL
		,stock_shipment_kg				dec(11,5)				NULL
		,stock_after_shipment_kg		as nullif(stock_kg - isnull(stock_shipment_kg, 0), 0)

);



--- логи транзиты в project_plan_production_finished_products.data_import.stock_log_calculation
