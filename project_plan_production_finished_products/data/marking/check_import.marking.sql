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
						delete project_plan_production_finished_products.data_import.data_type where data_type = 'marking';
						
						-- добавляем данные
						insert into project_plan_production_finished_products.data_import.data_type
							   (data_type, source_data,  path_file,  data_on_date)
						values ('marking', 'Excel',		@path_file, @data_on_date);
			
						-- удаляем и выгружаем
						TRUNCATE TABLE project_plan_production_finished_products.data_import.marking_log_calculation;
						TRUNCATE TABLE project_plan_production_finished_products.data_import.marking;
						SELECT TOP 0 * FROM project_plan_production_finished_products.data_import.marking;

						return(0);
			end;


			-- подтягиваем набивку
			update c
			set  c.marking_stuffing_id = sm.stuffing_id
			from project_plan_production_finished_products.data_import.marking as c
			join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm on c.marking_SAP_id = sm.SAP_id;



			---- пишем ошибки ---------------------------------------------------------------
			--update project_plan_production_finished_products.data_import.marking
			--Set reason_ignore_in_calculate = 
			--	nullif(
			--			  case when sap_id is null then 'Не найден sap id | ' else '' end
			--			+ case when stuffing_id is null then 'Код набивки отсутствует | ' else '' end
			--			+ case when marking_current_KOS is null then 'КОС некорректный | ' else '' end
			--			+ case when marking_current_KOS < 0.1 then 'КОС меньше 10% | ' else '' end
			--			, '');




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
			from project_plan_production_finished_products.data_import.marking as s
			join project_plan_production_finished_products.data_import.data_type as ie on s.marking_data_type = ie.data_type
			left join cherkizovo.info.products_sap as sp on s.marking_sap_id = sp.sap_id;

end;
