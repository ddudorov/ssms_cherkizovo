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



create procedure check_import.data_type @data_type		varchar(30)		
									   ,@source_data	varchar(300)
									   ,@path_file		varchar(300) = null
									   ,@data_on_date	datetime	
as										
BEGIN
			SET NOCOUNT ON;
						
			-- удаляем данные
			delete project_plan_production_finished_products.data_import.data_type
			where data_type = @data_type;

			-- добавляем данные
			insert into project_plan_production_finished_products.data_import.data_type
				   ( data_type,  source_data,  path_file,  data_on_date)
			values (@data_type, @source_data, @path_file, @data_on_date);

			
			-- удаляем данные и делаем select 
			if @data_type in ('shipments_SAP', 'shipments_1C', 'shipments_sales_plan')
			begin

					delete from project_plan_production_finished_products.data_import.shipments
					where shipment_data_type = @data_type;

					select top 0 * from project_plan_production_finished_products.data_import.shipments;

					
			end

			


end;












