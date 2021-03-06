﻿use project_plan_production_finished_products

go

select * from .data_import.marking
--drop table .data_import.marking
create table .data_import.marking
(

		 marking_row_id							INT					NOT NULL	IDENTITY(1,1)
		,marking_data_type						as 'marking'
		,marking_reason_ignore_in_calculate		VARCHAR(300)			NULL
		,marking_sap_id							BIGINT				NOT NULL
		,marking_stuffing_id					VARCHAR(40)				NULL

		,marking_warehouse_name					VARCHAR(100)		NOT NULL	
		,marking_production_date				DATETIME			NOT NULL
		,marking_on_date						DATETIME			NOT NULL
		,marking_expiration_date				DATETIME			NOT NULL
		,marking_current_KOS					as case when marking_production_date >= marking_expiration_date then null  
														when marking_production_date >  marking_on_date		    then null
														when marking_expiration_date <= marking_on_date		    then 0.000000	
														when marking_production_date  = marking_on_date			then 1
														else DATEDIFF(day, marking_on_date, marking_expiration_date) * 1.0 / DATEDIFF(day, marking_production_date, marking_expiration_date) end

		,marking_KOS_in_day						as case when marking_production_date >= marking_expiration_date then null	
														when marking_production_date >  marking_on_date			then null
														else 1.0 / DATEDIFF(day, marking_production_date, marking_expiration_date) end	
		-- расчетные поля
		,marking_kg								dec(11,5)			NOT NULL
		,marking_shipment_kg					dec(11,5)				NULL
		,marking_after_shipment_kg				as nullif(marking_kg - isnull(marking_shipment_kg, 0), 0)
)


-- drop table .data_import.marking_save
create table data_import.marking_save
(
		 marking_sap_id				BIGINT				NOT NULL
		,marking_stuffing_id		VARCHAR(40)				NULL
		,marking_warehouse_name		VARCHAR(100)		NOT NULL	
		,marking_production_date	DATETIME			NOT NULL
		,marking_on_date			DATETIME			NOT NULL
		,marking_kg					dec(11,5)			NOT NULL
)





--- логи остатки
select * from .data_import.marking_log_calculation
--drop table .data_import.marking_log_calculation
create table .data_import.marking_log_calculation
( 
		 sort_id				INT				NOT NULL	
		,shipment_row_id		INT				NOT NULL		
		,shipment_date			datetime			NULL	
		,shipment_kg			dec(11,5)		NOT NULL
		,marking_row_id			INT					NULL
		,marking_kg				dec(11,5)			NULL	
		,marking_shipment_kg	dec(11,5)			NULL				
);

