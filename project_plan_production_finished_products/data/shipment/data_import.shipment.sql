use project_plan_production_finished_products

go

select * from project_plan_production_finished_products.data_import.shipment
--drop table project_plan_production_finished_products.data_import.shipment
create table project_plan_production_finished_products.data_import.shipment
(

		 shipment_row_id							INT					NOT NULL IDENTITY(1,1)
		,shipment_data_type							VARCHAR(30)			NOT NULL					-- shipment_SAP / shipment_1C / shipment_sales_plan
		,shipment_reason_ignore_in_calculate		VARCHAR(300)			NULL					-- пишем ошибки
		,shipment_delete							tinyint				NOT NULL default 0			-- 0 строка участвует в расчетах - для сверки SAP и План продаж

		,shipment_sap_id							BIGINT					NULL
		,shipment_product_status					VARCHAR(100)			NULL					-- статус блокировки из РГП
		,shipment_sap_id_expiration_date_in_days	SMALLINT				NULL
		
		,shipment_stuffing_id						VARCHAR(40)				NULL
		,shipment_stuffing_id_box					VARCHAR(40)				NULL
		,shipment_stuffing_id_box_row_id			INT						NULL
		,shipment_stuffing_id_box_type				TINYINT				NOT NULL default 0			-- 0 обычная набивка / 1 коробка / 2 разбитая коробка на набивки
					
		,shipment_promo_status						VARCHAR(100)			NULL	-- план продаж	
		,shipment_promo								VARCHAR(100)			NULL	-- план продаж	
		,shipment_promo_kos_listing					VARCHAR(100)			NULL	-- план продаж	

		,sap_id										BIGINT					NULL	-- ПЛАН ПРОДАЖ		
		,position_dependent_id						INT						NULL	-- SAP / ПЛАН ПРОДАЖ	
		,individual_marking_id						TINYINT					NULL	-- SAP / ПЛАН ПРОДАЖ
		,article_nomenclature						VARCHAR(20)				NULL	-- ПЛАН ПРОДАЖ
		,article_packaging							VARCHAR(25)				NULL	-- 1C / план продаж	
		,product_finished_id						decimal(14, 0)			NULL	-- ПЛАН ПРОДАЖ

	
		,shipment_branch_id							VARCHAR(20)				NULL	
		,shipment_branch_name						VARCHAR(100)			NULL	
		,shipment_sales_channel_id					TINYINT					NULL
		,shipment_sales_channel_name				VARCHAR(25)				NULL
		,shipment_customer_id						VARCHAR(20)				NULL
		,shipment_customer_name						VARCHAR(100)			NULL
		,shipment_delivery_address					VARCHAR(1000)			NULL

		,shipment_priority							TINYINT					NULL
		,shipment_min_KOS							DEC(7,6)				NULL
		
		,shipment_with_branch_date					DATETIME				NULL
		,shipment_date								DATETIME				NULL
		,shipment_kg								dec(11,5)			NOT NULL

		-- расчетные поля
		,shipment_from_stock_kg						dec(11,5)				NULL
		,shipment_after_stock_kg					as nullif( shipment_kg - isnull(shipment_from_stock_kg, 0)   , 0)
		
		,shipment_from_stuffing_fact_kg				dec(11,5)				NULL
		,shipment_after_stuffing_fact_kg			as nullif( shipment_kg - isnull(shipment_from_stock_kg, 0) - isnull(shipment_from_stuffing_fact_kg, 0)  , 0)

		,shipment_from_stuffing_plan_kg				dec(11,5)				NULL
		,shipment_after_stuffing_plan_kg			as nullif( shipment_kg - isnull(shipment_from_stock_kg, 0) - isnull(shipment_from_stuffing_fact_kg, 0) - isnull(shipment_from_stuffing_plan_kg, 0) , 0)

		,shipment_from_marking_kg					dec(11,5)				NULL
		,shipment_after_marking_kg					as nullif( shipment_kg - isnull(shipment_from_stock_kg, 0) - isnull(shipment_from_marking_kg, 0)   , 0)


)


	

