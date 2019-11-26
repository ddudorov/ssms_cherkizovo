use project_plan_production_finished_products

go

select * from project_plan_production_finished_products.data_import.info_excel
--drop table project_plan_production_finished_products.data_import.info_excel
create table project_plan_production_finished_products.data_import.info_excel
(

		 path_file		varchar(300)	NOT NULL
		,date_file		datetime		NOT NULL
		,name_table		varchar(50)		NOT NULL
		,user_insert	varchar(50)		NOT NULL	DEFAULT ORIGINAL_LOGIN()
		,dt_tm_insert	datetime		NOT NULL	DEFAULT getdate()
		,CONSTRAINT [PK info_excel | name_table] PRIMARY KEY CLUSTERED (name_table) 
)



go



create procedure check_import.info_excel @path_file varchar(300)
									    ,@date_file datetime		
									    ,@name_table varchar(50)	
									    ,@select bit = 1 	
as
BEGIN
			SET NOCOUNT ON;
						
			-- удаляем данные
			delete project_plan_production_finished_products.data_import.info_excel
			where name_table = @name_table;
			
			-- добавляем данные
			insert into project_plan_production_finished_products.data_import.info_excel
				   ( path_file,  date_file,  name_table)
			values (@path_file, @date_file, @name_table);

							exec('TRUNCATE TABLE project_plan_production_finished_products.data_import.' + @name_table); -- очищаем таблицу
			if @select = 1	exec( 'select * from project_plan_production_finished_products.data_import.' + @name_table);

end;












