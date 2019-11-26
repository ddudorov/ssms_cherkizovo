use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.marking

go

alter procedure check_import.marking					
as
BEGIN
			SET NOCOUNT ON;
			

			-- подтягиваем набивку
			update c
			set  c.stuffing_id		= sm.stuffing_id
			from project_plan_production_finished_products.data_import.marking as c
			join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm on c.SAP_id = sm.SAP_id;



			-- пишем ошибки ---------------------------------------------------------------
			update project_plan_production_finished_products.data_import.marking
			Set reason_ignore_in_calculate = 
				nullif(
						  case when sap_id is null then 'Не найден sap id | ' else '' end
						+ case when stuffing_id is null then 'Код набивки отсутствует | ' else '' end
						+ case when marking_current_KOS is null then 'КОС некорректный | ' else '' end
						+ case when marking_current_KOS < 0.1 then 'КОС меньше 10% | ' else '' end
						, '');




			-- выгружаем данные в excel
			select 
				 h.reason_ignore_in_calculate
				,h.sap_id_text
				,h.marking_warehouse_name
				,h.marking_production_date
				,h.marking_on_date
				,h.marking_expiration_date
				,h.marking_current_KOS
				,h.marking_kg
				,'Путь/Имя файла' = ie.path_file
				,'Данные на дату' = ie.date_file
				,'Кто загрузил' = ie.user_insert
				,'Дата и время загрузки' = ie.dt_tm_insert
			from project_plan_production_finished_products.data_import.marking as h
			join project_plan_production_finished_products.data_import.info_excel as ie on h.name_table = ie.name_table;
		
end;
