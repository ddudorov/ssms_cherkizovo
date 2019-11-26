use project_plan_production_finished_products

go

select * from project_plan_production_finished_products.data_import.stuffing_fact
--drop table project_plan_production_finished_products.data_import.stuffing_fact
create table project_plan_production_finished_products.data_import.stuffing_fact
(

		 row_id									INT					NOT NULL	IDENTITY(1,1)
		,stuffing_row_id						INT						NULL
		,name_table								as 'stuffing_fact'
						
		,reason_ignore_in_calculate				VARCHAR(300)			NULL

		,sap_id									BIGINT					NULL
		,sap_id_text							as convert(varchar(24), FORMAT(sap_id, '000000000000000000000000'))

		,stuffing_id							VARCHAR(40)			NOT NULL
		,stuffing_production_name				VARCHAR(15)				NULL

		,stuffing_production_date_from			DATETIME				NULL
		,stuffing_production_date_to			DATETIME				NULL
		,stuffing_available_date				DATETIME				NULL
		,stuffing_before_next_available_date	DATETIME				NULL
		,stuffing_expiration_date				DATETIME				NULL

		,stuffing_current_KOS					as case when stuffing_production_date_to >= stuffing_expiration_date	then null  
														when stuffing_production_date_to >  stuffing_available_date		then null
														when stuffing_expiration_date	 <= stuffing_available_date		then 0.000000
														when stuffing_production_date_to  = stuffing_available_date		then 1		
														else DATEDIFF(day, stuffing_available_date, stuffing_expiration_date) * 1.0 / DATEDIFF(day, stuffing_production_date_to, stuffing_expiration_date) end

		,stuffing_KOS_in_day					as case when stuffing_production_date_to >= stuffing_expiration_date then null	
														when stuffing_production_date_to >  stuffing_available_date	 then null
														else 1.0 / DATEDIFF(day, stuffing_production_date_to, stuffing_expiration_date) end	

		,stuffing_kg							dec(11,5)				NULL
		,stuffing_surplus_kg					as nullif(case when sap_id is null then stuffing_kg - isnull(stuffing_marking_kg, 0) - isnull(stuffing_shipment_kg, 0) end, 0)
		,stuffing_marking_kg					dec(11,5)				NULL			   
		,stuffing_shipment_kg					dec(11,5)				NULL	-- кол-во которое уже отгружено из набивки включая маркировку

		,CONSTRAINT [UI stuffing_fact | sap_id, stuffing_id, stuffing_available_date] UNIQUE(sap_id, stuffing_id, stuffing_available_date)

)





--- логи распределения фактических набивок
select * from project_plan_production_finished_products.data_import.stuffing_fact_log_calculation
-- drop table project_plan_production_finished_products.data_import.stuffing_fact_log_calculation
create table project_plan_production_finished_products.data_import.stuffing_fact_log_calculation
( 
		 sort_id				INT				NOT NULL
		,shipment_row_id		INT				NOT NULL	
		,shipment_name_table	varchar(40)			NULL	
		,shipment_sap_id		bigint			NOT NULL
		,shipment_kg			dec(11,5)		NOT NULL
		,stuffing_row_id		INT					NULL
		,stuffing_kg			dec(11,5)			NULL	
		,stuffing_marking_kg	dec(11,5)			NULL	
		,stuffing_shipment_kg	dec(11,5)			NULL			
);





