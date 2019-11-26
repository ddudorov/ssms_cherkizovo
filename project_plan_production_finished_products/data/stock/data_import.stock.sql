use project_plan_production_finished_products

go


--drop table project_plan_production_finished_products.data_import.stock
create table project_plan_production_finished_products.data_import.stock
(

		 row_id							INT					NOT NULL	IDENTITY(1,1)
		,name_table						as 'stock'
		,reason_ignore_in_calculate		VARCHAR(300)			NULL
		,product_status					varchar(100)			NULL


		,sap_id							BIGINT					NULL
		,sap_id_text					as convert(varchar(24), FORMAT(sap_id, '000000000000000000000000'))
		,stuffing_id					VARCHAR(40)				NULL
		,product_finished_id			decimal(14, 0)			NULL
		
		--- ������� ---
		,stock_warehouse_name			VARCHAR(100)			NULL	
		,stock_storage_area_name		VARCHAR(100)			NULL	
		,stock_branch_name				VARCHAR(100)			NULL
		,stock_production_date			DATETIME				NULL
		,stock_on_date					DATETIME				NULL
		,stock_expiration_date			DATETIME				NULL
		,stock_current_KOS				as case when stock_production_date >= stock_expiration_date then null  
												when stock_production_date >  stock_on_date		    then null
												when stock_expiration_date <= stock_on_date		    then 0.000000	
												when stock_production_date	= stock_on_date			then 1
												else DATEDIFF(day, stock_on_date, stock_expiration_date) * 1.0 / DATEDIFF(day, stock_production_date, stock_expiration_date) end

		,stock_KOS_in_day				as case when stock_production_date >= stock_expiration_date then null	
												when stock_production_date >  stock_on_date			then null
												else 1.0 / DATEDIFF(day, stock_production_date, stock_expiration_date) end	
		-- ��������� ����
		,stock_kg						dec(11,5)				NULL
		,stock_shipment_kg				dec(11,5)				NULL
		,stock_after_shipment_kg		as nullif(stock_kg - isnull(stock_shipment_kg, 0), 0)

);






--- ���� �������
select * from project_plan_production_finished_products.data_import.stock_log_calculation
drop table project_plan_production_finished_products.data_import.stock_log_calculation
create table project_plan_production_finished_products.data_import.stock_log_calculation
( 
		 sort_id				INT				NOT NULL
		,stock_row_id			INT					NULL
		,stock_name_table		varchar(40)			NULL	
		,shipment_row_id		INT				NOT NULL	
		,shipment_name_table	varchar(40)			NULL	
		,shipment_kg			dec(11,5)		NOT NULL
		,stock_kg				dec(11,5)			NULL	
		,stock_shipment_kg		dec(11,5)			NULL				
);
