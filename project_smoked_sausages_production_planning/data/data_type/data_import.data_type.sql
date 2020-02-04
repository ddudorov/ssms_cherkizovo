use project_plan_production_finished_products

go

select * from data_import.data_type
--drop table data_import.data_type
create table data_import.data_type
(
		
		 data_type							varchar(30)		NOT NULL
		,source_data						varchar(300)	NOT NULL
		,path_file							varchar(300)		NULL
		,uploaded_user						varchar(50)		NOT NULL	DEFAULT ORIGINAL_LOGIN()
		,uploaded_dt_tm						datetime		NOT NULL	DEFAULT getdate()
		
		,data_on_date						datetime		NOT NULL
		,uploaded_kg						dec(15,5)			NULL
		,shipment_kg						dec(15,5)			NULL
		,shipment_from_stock_kg				dec(15,5)			NULL
		,deficit_after_shipment_stock_kg	dec(15,5)			NULL
		,shipment_from_stuffing_fact_kg		dec(15,5)			NULL
		,shipment_from_stuffing_plan_kg		dec(15,5)			NULL
		,shipment_from_marking_kg			dec(15,5)			NULL
		,deficit_after_shipment_marking_kg	dec(15,5)			NULL
				
		,stuffing_surplus_kg				dec(15,5)			NULL															  
		,stuffing_marking_kg				dec(15,5)			NULL															  
		,surplus_kg							dec(15,5)			NULL
				
		,CONSTRAINT [PK data_type | data_type] PRIMARY KEY CLUSTERED (data_type) 
)
