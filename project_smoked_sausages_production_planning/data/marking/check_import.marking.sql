use project_plan_production_finished_products
--exec project_plan_production_finished_products.check_import.marking

go

alter procedure check_import.marking @path_file		varchar(300) = null
									,@data_on_date	datetime = null								
as
BEGIN
			SET NOCOUNT ON;
			
			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ФАЙЛЕ И ВЫГРУЖАЕМ ТАБЛИЦУ ДЛЯ ЗАГРУЗКИ
			if not @path_file is null	 
			begin
						-- удаляем данные
						delete data_import.data_type where data_type = 'marking';
						
						-- добавляем данные
						insert into data_import.data_type
							   (data_type, source_data,  path_file,  data_on_date)
						values ('marking', 'Excel',		@path_file, @data_on_date);
			
						-- удаляем и выгружаем
						TRUNCATE TABLE data_import.marking_log_calculation;
						TRUNCATE TABLE data_import.marking;
						SELECT TOP 0 * FROM data_import.marking;

						return(0);
			end;


			-- подтягиваем набивку
			update c
			set  c.marking_stuffing_id = sm.stuffing_id
			from data_import.marking as c
			join info_view.sap_id as sm on c.marking_SAP_id = sm.sap_id_for_join


			-- добавляем данные в общию таблицу, которую выводим на форму
			exec report.for_form
			


			-- ВЫГРУЖАЕМ ДАННЫЕ ---------------------------------------------------------------
			select 
					 s.marking_reason_ignore_in_calculate	
					,convert(varchar(24), FORMAT(s.marking_sap_id, '000000000000000000000000')) as sap_id
					,sp.product_1C_full_name
					,s.marking_stuffing_id
					,s.marking_warehouse_name
					,s.marking_production_date
					,s.marking_on_date
					,s.marking_expiration_date
					,s.marking_current_KOS
					,s.marking_kg		
					,ie.path_file
					,ie.data_on_date
			from data_import.marking as s
			join data_import.data_type as ie on s.marking_data_type = ie.data_type
			left join info_view.sap_id as sp on s.marking_SAP_id = sp.sap_id_for_join;

end;
