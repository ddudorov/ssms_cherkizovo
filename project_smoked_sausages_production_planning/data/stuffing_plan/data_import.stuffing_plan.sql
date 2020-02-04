use project_plan_production_finished_products

go

select * from project_plan_production_finished_products.data_import.stuffing_plan
--drop table project_plan_production_finished_products.data_import.stuffing_plan
create table project_plan_production_finished_products.data_import.stuffing_plan
(

		 stuffing_row_id						INT					NOT NULL	IDENTITY(1,1)
		,stuffing_sap_id_row_id					INT						NULL
		,stuffing_data_type						as 'stuffing_plan'
						
		,stuffing_reason_ignore_in_calculate	VARCHAR(300)			NULL

		,stuffing_sap_id						BIGINT					NULL

		,stuffing_id							VARCHAR(40)			NOT NULL
		,stuffing_production_name				VARCHAR(15)				NULL
		
		,stuffing_production_date_from			DATETIME			NOT NULL
		,stuffing_production_date_to			DATETIME			NOT NULL
		,stuffing_available_date				DATETIME			NOT NULL
		,stuffing_before_next_available_date	DATETIME				NULL
		
		,stuffing_count_planned					dec(11,5)				NULL	 -- замесы в плане набивок и кол-во камер
		,stuffing_kg							dec(11,5)				NULL
		,stuffing_surplus_kg					as nullif(case when stuffing_sap_id is null then stuffing_kg - isnull(stuffing_marking_kg, 0) - isnull(stuffing_shipment_kg, 0) end, 0)
		,stuffing_marking_kg					dec(11,5)				NULL			   
		,stuffing_shipment_kg					dec(11,5)				NULL	-- кол-во которое уже отгружено из набивки включая маркировку

		,CONSTRAINT [UI stuffing_plan | stuffing_sap_id, stuffing_id, stuffing_available_date] UNIQUE(stuffing_sap_id, stuffing_id, stuffing_available_date)

)







--- логи распределения фактических набивок
select * from project_plan_production_finished_products.data_import.stuffing_plan_log_calculation
-- drop table project_plan_production_finished_products.data_import.stuffing_plan_log_calculation
create table project_plan_production_finished_products.data_import.stuffing_plan_log_calculation
( 
		 sort_id				INT				NOT NULL
		,shipment_row_id		INT				NOT NULL	
		,shipment_date			datetime		NOT NULL
		,shipment_kg			dec(11,5)		NOT NULL
		,stuffing_row_id		INT					NULL	
		,stuffing_sap_id		bigint			NOT NULL
		,stuffing_kg			dec(11,5)			NULL	
		,stuffing_marking_kg	dec(11,5)			NULL	
		,stuffing_shipment_kg	dec(11,5)			NULL		
);









