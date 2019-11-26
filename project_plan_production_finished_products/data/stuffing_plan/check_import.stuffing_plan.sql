use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.shipments_SAP

go

alter procedure check_import.stuffing_plan		
as
BEGIN
			SET NOCOUNT ON;
			
			-- проставляем срок годности
			Update h
			Set h.stuffing_expiration_date = DateAdd(Day, i.expiration_date_in_days, h.stuffing_production_date_to)
			from project_plan_production_finished_products.data_import.stuffing_plan as h
			join project_plan_production_finished_products.info.stuffing as i on h.stuffing_id = i.stuffing_id;

					
			-- пишем ошибки
			Update h
			Set h.reason_ignore_in_calculate = 
				nullif(
							case when i.stuffing_id is null then 'Набивка отсутствует | ' else '' end 
						+ case when not h.stuffing_production_name is null and not i.production_name is null
								and h.stuffing_production_name <> i.production_name
															then 'Производитель отличается в справочнике | ' else '' end 
						, '')
			from project_plan_production_finished_products.data_import.stuffing_plan as h
			left join project_plan_production_finished_products.info.stuffing as i on h.stuffing_id = i.stuffing_id;
						

			-- проставляем дату до выхода следующей набивки		
			update s
			set s.stuffing_before_next_available_date = l.stuffing_before_next_available_date
			from project_plan_production_finished_products.data_import.stuffing_plan as s
			join (
					select l.row_id, isnull(lead(l.stuffing_available_date) over (partition by l.stuffing_id order by l.stuffing_available_date) - 1, '29990101')  as stuffing_before_next_available_date
					from project_plan_production_finished_products.data_import.stuffing_plan as l
					) as l on s.row_id = l.row_id;
						 		

			-- обновляем уникальный ключ набивки, который будет использоватся для расчетов
			update project_plan_production_finished_products.data_import.stuffing_plan
			set stuffing_row_id = row_id;
										
			-- выгружаем данные в excel
			select 
				 h.reason_ignore_in_calculate
				,h.stuffing_id
				,h.stuffing_production_name
				,h.stuffing_production_date_from
				,h.stuffing_production_date_to
				,h.stuffing_available_date
				,h.stuffing_before_next_available_date
				,h.count_plan_stuffing
				,h.stuffing_kg
			from project_plan_production_finished_products.data_import.stuffing_plan as h;
					
end;
