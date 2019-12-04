use project_plan_production_finished_products

go

select * from project_plan_production_finished_products.data_import.data_type
--drop table project_plan_production_finished_products.data_import.data_type
create table project_plan_production_finished_products.data_import.data_type
(
		
		 data_type		varchar(30)		NOT NULL
		,source_data	varchar(300)	NOT NULL
		,path_file		varchar(300)		NULL
		,data_on_date	datetime		NOT NULL
		,uploaded_user	varchar(50)		NOT NULL	DEFAULT ORIGINAL_LOGIN()
		,uploaded_dt_tm	datetime		NOT NULL	DEFAULT getdate()
		,CONSTRAINT [PK data_type | data_type] PRIMARY KEY CLUSTERED (data_type) 
)



go












