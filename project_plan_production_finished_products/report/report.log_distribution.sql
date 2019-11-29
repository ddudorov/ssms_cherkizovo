use project_plan_production_finished_products 

--exec project_plan_production_finished_products.report.log_distribution @log_type = 'stuffing_plan'

go

alter procedure report.log_distribution @log_type varchar(50)															
as
BEGIN

			if @log_type = 'stock' -- остатки
			begin

						SELECT 
								 'Порядок'				= l.sort_id
								,'Источник потребности' = l.shipment_name_table
								,'SAP ID потребности'	= convert(varchar(24),FORMAT(COALESCE(sp.sap_id, c1.sap_id, sl.sap_id), '000000000000000000000000'))
								,'Дата отгрузки'		= COALESCE(sp.shipment_date, c1.shipment_date, sl.shipment_date)
								,'Пр отг'				= COALESCE(sp.shipment_priority, c1.shipment_priority, sl.shipment_priority)
								,'Мин КОС отгрузки'		= COALESCE(sp.shipment_min_KOS, c1.shipment_min_KOS, sl.shipment_min_KOS)
								,'Потреность в отг'		= l.shipment_kg

								,'Источник остатка'		= l.stock_name_table
								,'Остатки ID'			= l.stock_row_id
								,'SAP ID остатков'		= convert(varchar(24),FORMAT(COALESCE(st.sap_id, tr.sap_id), '000000000000000000000000'))

								,'Ост на дату'			= COALESCE(st.stock_on_date, tr.stock_on_date)
								,'КОС на дату'			= COALESCE(st.stock_current_KOS, tr.stock_current_KOS)
								,'Ост на дату отгрузки' = l.stock_kg
							
								,'Отг из остатков'		=l.stock_shipment_kg

						FROM project_plan_production_finished_products.data_import.stock_log_calculation		as l
						left join project_plan_production_finished_products.data_import.stock					as st on l.stock_row_id = st.row_id		and l.stock_name_table = st.name_table
						left join project_plan_production_finished_products.data_import.transits				as tr on l.stock_row_id = tr.row_id		and l.stock_name_table = tr.name_table
						left join project_plan_production_finished_products.data_import.shipments_SAP			as sp on l.shipment_row_id = sp.row_id	and l.shipment_name_table = sp.name_table
						left join project_plan_production_finished_products.data_import.shipments_1C			as c1 on l.shipment_row_id = c1.row_id	and l.shipment_name_table = c1.name_table
						left join project_plan_production_finished_products.data_import.shipments_sales_plan	as sl on l.shipment_row_id = sl.row_id	and l.shipment_name_table = sl.name_table
						order by l.sort_id

			end;




			if @log_type = 'stuffing_fact' -- распределение фактических набивок
			begin

						SELECT 
								 'sort_id'								= l.sort_id																						--'Порядок расчетов'				
								,'shipment_name_table'					= l.shipment_name_table																			--'Источник потребности'			
								,'stuffing_row_id'						= l.stuffing_row_id																				--'id набивки'					
								,'sap_id'								= convert(varchar(24),FORMAT(l.shipment_sap_id, '000000000000000000000000')) 					--'sap id'						
								,'product_1C_full_name'					= sa.product_1C_full_name																		--'Название SKU 1С'				
								,'stuffing_id'							= COALESCE(st.stuffing_id, sp.stuffing_id, c1.stuffing_id, sl.stuffing_id)						--'Код набивки'					
								,'stuffing_id_box'						= COALESCE(sp.stuffing_id_box , c1.stuffing_id_box, sl.stuffing_id_box)							--'Коробка'						
								,'stuffing_production_date_to'			= st.stuffing_production_date_to																--'Дата выхода набивки'			
								,'stuffing_before_next_available_date'	= nullif(st.stuffing_before_next_available_date,'29990101')										--'Дата выхода след набивки'		
								,'stuffing_kg'							= l.stuffing_kg																					--'Остаток набивки'				
								,'stuffing_marking_kg'					= l.stuffing_marking_kg																			--'Остаток маркировки'			
								,'stuffing_shipment_kg'					= l.stuffing_shipment_kg																		--'Отгружено из маркировки'		
																																		
								,'shipment_customer_name'				= COALESCE(sp.shipment_customer_name, c1.shipment_customer_name, sl.shipment_customer_name)		--'Название контрагента'			
								,'shipment_date'						= COALESCE(sp.shipment_date, c1.shipment_date, sl.shipment_date)								--'Дата отгрузки потребности'	 
								,'shipment_priority'					= COALESCE(sp.shipment_priority, c1.shipment_priority, sl.shipment_priority)					--'Приоритет отгрузки'			
								,'shipment_min_KOS'						= COALESCE(sp.shipment_min_KOS, c1.shipment_min_KOS, sl.shipment_min_KOS)						--'КОС отгрузки'					
								,'shipment_kg'							= l.shipment_kg																					--'Потребность'		

						FROM project_plan_production_finished_products.data_import.stuffing_fact_log_calculation	as l
						left join project_plan_production_finished_products.data_import.stuffing_fact				as st on l.stuffing_row_id = st.stuffing_row_id and l.shipment_sap_id = st.sap_id
						left join project_plan_production_finished_products.data_import.shipments_SAP				as sp on l.shipment_row_id = sp.row_id			and l.shipment_name_table = sp.name_table
						left join project_plan_production_finished_products.data_import.shipments_1C				as c1 on l.shipment_row_id = c1.row_id			and l.shipment_name_table = c1.name_table
						left join project_plan_production_finished_products.data_import.shipments_sales_plan		as sl on l.shipment_row_id = sl.row_id			and l.shipment_name_table = sl.name_table
						left join cherkizovo.info.products_sap														as sa on l.shipment_sap_id = sa.sap_id
						order by l.sort_id

			end;


			if @log_type = 'stuffing_plan' -- распределение плановых набивок
			begin

						SELECT 
								 'sort_id'								= l.sort_id																						--'Порядок расчетов'				
								,'shipment_name_table'					= l.shipment_name_table																			--'Источник потребности'			
								,'stuffing_row_id'						= l.stuffing_row_id																				--'id набивки'					
								,'sap_id'								= convert(varchar(24),FORMAT(l.shipment_sap_id, '000000000000000000000000')) 					--'sap id'						
								,'product_1C_full_name'					= sa.product_1C_full_name																		--'Название SKU 1С'				
								,'stuffing_id'							= COALESCE(st.stuffing_id, sp.stuffing_id, c1.stuffing_id, sl.stuffing_id)						--'Код набивки'					
								,'stuffing_id_box'						= COALESCE(sp.stuffing_id_box , c1.stuffing_id_box, sl.stuffing_id_box)							--'Коробка'						
								,'stuffing_production_date_to'			= st.stuffing_production_date_to																--'Дата выхода набивки'			
								,'stuffing_before_next_available_date'	= nullif(st.stuffing_before_next_available_date,'29990101')										--'Дата выхода след набивки'		
								,'stuffing_kg'							= l.stuffing_kg																					--'Остаток набивки'				
								,'stuffing_marking_kg'					= l.stuffing_marking_kg																			--'Остаток маркировки'			
								,'stuffing_shipment_kg'					= l.stuffing_shipment_kg																		--'Отгружено из маркировки'		
																																		
								,'shipment_customer_name'				= COALESCE(sp.shipment_customer_name, c1.shipment_customer_name, sl.shipment_customer_name)		--'Название контрагента'			
								,'shipment_date'						= COALESCE(sp.shipment_date, c1.shipment_date, sl.shipment_date)								--'Дата отгрузки потребности'	 
								,'shipment_priority'					= COALESCE(sp.shipment_priority, c1.shipment_priority, sl.shipment_priority)					--'Приоритет отгрузки'			
								,'shipment_min_KOS'						= COALESCE(sp.shipment_min_KOS, c1.shipment_min_KOS, sl.shipment_min_KOS)						--'КОС отгрузки'					
								,'shipment_kg'							= l.shipment_kg																					--'Потребность'					
																																		
						FROM project_plan_production_finished_products.data_import.stuffing_plan_log_calculation	as l
						left join project_plan_production_finished_products.data_import.stuffing_fact				as st on l.stuffing_row_id = st.stuffing_row_id and l.shipment_sap_id = st.sap_id
						left join project_plan_production_finished_products.data_import.shipments_SAP				as sp on l.shipment_row_id = sp.row_id			and l.shipment_name_table = sp.name_table
						left join project_plan_production_finished_products.data_import.shipments_1C				as c1 on l.shipment_row_id = c1.row_id			and l.shipment_name_table = c1.name_table
						left join project_plan_production_finished_products.data_import.shipments_sales_plan		as sl on l.shipment_row_id = sl.row_id			and l.shipment_name_table = sl.name_table
						left join cherkizovo.info.products_sap														as sa on l.shipment_sap_id = sa.sap_id
						order by l.sort_id

			end;
end;

