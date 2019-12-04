use project_plan_production_finished_products

go


--drop table project_plan_production_finished_products.data_import.stock
create table project_plan_production_finished_products.data_import.stock
(

		 stock_row_id							INT					NOT NULL IDENTITY(1,1)
		,stock_data_type						VARCHAR(30)			NOT NULL					-- stock / transit
		,stock_reason_ignore_in_calculate		VARCHAR(300)			NULL					-- пишем ошибки

		,stock_sap_id							BIGINT					NULL
		,stock_stuffing_id						VARCHAR(40)				NULL
		,product_1C_full_name					varchar(200)			NULL
		,product_finished_id					decimal(14, 0)			NULL
		
		--- остатки ---
		,stock_warehouse_name					VARCHAR(100)			NULL	
		,stock_storage_area_name				VARCHAR(100)			NULL	
		,stock_branch_name						VARCHAR(100)			NULL
		,stock_production_date					DATETIME				NULL
		,stock_on_date							DATETIME				NULL
		,stock_expiration_date					DATETIME				NULL
		,stock_current_KOS						as case when stock_production_date >= stock_expiration_date then null  
														when stock_production_date >  stock_on_date		    then null
														when stock_expiration_date <= stock_on_date		    then 0.000000	
														when stock_production_date	= stock_on_date			then 1
														else DATEDIFF(day, stock_on_date, stock_expiration_date) * 1.0 / DATEDIFF(day, stock_production_date, stock_expiration_date) end

		,stock_KOS_in_day						as case when stock_production_date >= stock_expiration_date then null	
														when stock_production_date >  stock_on_date			then null
														else 1.0 / DATEDIFF(day, stock_production_date, stock_expiration_date) end	
		-- расчетные поля
		,stock_kg								dec(11,5)				NULL
		,stock_shipment_kg						dec(11,5)				NULL
		,stock_after_shipment_kg				as nullif(stock_kg - isnull(stock_shipment_kg, 0), 0)

);






--- логи остатки
select * from project_plan_production_finished_products.data_import.stock_log_calculation
drop table project_plan_production_finished_products.data_import.stock_log_calculation
create table project_plan_production_finished_products.data_import.stock_log_calculation
( 
		 sort_id				INT				NOT NULL	
		,shipment_row_id		INT				NOT NULL		
		,shipment_date			datetime			NULL	
		,shipment_kg			dec(11,5)		NOT NULL
		,stock_row_id			INT					NULL
		,stock_kg				dec(11,5)			NULL	
		,stock_shipment_kg		dec(11,5)			NULL				
);
