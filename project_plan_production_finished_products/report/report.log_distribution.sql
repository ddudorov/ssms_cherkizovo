use project_plan_production_finished_products 

--exec project_plan_production_finished_products.report.log_distribution @log_type = 'stuffing_plan'
--exec project_plan_production_finished_products.report.log_distribution @log_type = 'stock'

go

alter procedure report.log_distribution @log_type varchar(50)															
as
BEGIN

			if @log_type = 'stock' -- остатки
			begin
						
						SELECT 
								 l.sort_id
								,sh.shipment_data_type
								,convert(varchar(24),FORMAT(sh.shipment_sap_id, '000000000000000000000000')) as shipment_sap_id
								,shs.product_1C_full_name as shipment_product_1C_full_name
								,sh.shipment_stuffing_id
								,sh.shipment_date
								,sh.shipment_priority
								,sh.shipment_min_KOS
								,l.shipment_kg

								,st.stock_data_type
								,l.stock_row_id
								,convert(varchar(24),FORMAT(st.stock_sap_id, '000000000000000000000000')) as stock_sap_id
								,sts.product_1C_full_name as stock_product_1C_full_name

								,st.stock_on_date
								,st.stock_current_KOS
								,l.stock_kg
							
								,l.stock_shipment_kg

						FROM project_plan_production_finished_products.data_import.stock_log_calculation	as l
						left join project_plan_production_finished_products.data_import.stock				as st  on l.stock_row_id = st.stock_row_id
						left join cherkizovo.info.products_sap												as sts on st.stock_sap_id = sts.sap_id

						left join project_plan_production_finished_products.data_import.shipment			as sh  on l.shipment_row_id = sh.shipment_row_id
						left join cherkizovo.info.products_sap												as shs on sh.shipment_sap_id = shs.sap_id
						order by l.sort_id;


			end;




			if @log_type = 'stuffing_fact' -- распределение фактических набивок
			begin

						SELECT 
								 l.sort_id																						--Порядок расчетов		
								,sh.shipment_data_type																			--Источник потребности	
								,convert(varchar(24),FORMAT(sh.shipment_sap_id, '000000000000000000000000')) as shipment_sap_id
								,shs.product_1C_full_name as shipment_product_1C_full_name
								,sh.shipment_stuffing_id
								,sh.shipment_stuffing_id_box							
								,sh.shipment_date
								,sh.shipment_priority
								,sh.shipment_min_KOS
								,l.shipment_kg
								
									
								,l.stuffing_row_id																				--id набивки					
								,convert(varchar(24),FORMAT(l.stuffing_sap_id, '000000000000000000000000')) 					--sap id					
								,sts.product_1C_full_name																		--Название SKU 1С	
								,st.stuffing_id																											
								,st.stuffing_production_date_to																	--Дата выхода набивки		
								,st.stuffing_available_date																		--Дата выхода набивки
								--,nullif(st.stuffing_before_next_available_date, '29990101')										--Дата выхода след набивки	
								,l.stuffing_kg																					--Остаток набивки	
								,l.stuffing_marking_kg																			--Остаток маркировки			
								,l.stuffing_shipment_kg																			--Отгружено из маркировки		
				
					

						FROM project_plan_production_finished_products.data_import.stuffing_fact_log_calculation	as l
						left join project_plan_production_finished_products.data_import.stuffing_fact				as st on l.stuffing_row_id = st.stuffing_row_id
						left join cherkizovo.info.products_sap														as sts on l.stuffing_sap_id = sts.sap_id

						left join project_plan_production_finished_products.data_import.shipment					as sh on l.shipment_row_id = sh.shipment_row_id	
						left join cherkizovo.info.products_sap														as shs on sh.shipment_sap_id = shs.sap_id	
						order by l.sort_id

			end;


			if @log_type = 'stuffing_plan' -- распределение плановых набивок
			begin

						SELECT 
								 l.sort_id																						--Порядок расчетов		
								,sh.shipment_data_type																			--Источник потребности	
								,convert(varchar(24),FORMAT(sh.shipment_sap_id, '000000000000000000000000')) as shipment_sap_id
								,shs.product_1C_full_name as shipment_product_1C_full_name
								,sh.shipment_stuffing_id
								,sh.shipment_stuffing_id_box							
								,sh.shipment_date
								,sh.shipment_priority
								,sh.shipment_min_KOS
								,l.shipment_kg
								
									
								,l.stuffing_row_id																				--id набивки					
								,convert(varchar(24),FORMAT(l.stuffing_sap_id, '000000000000000000000000')) 					--sap id					
								,sts.product_1C_full_name																		--Название SKU 1С	
								,st.stuffing_id																											
								,st.stuffing_production_date_to																	--Дата выхода набивки		
								,st.stuffing_available_date																		--Дата выхода набивки
								--,nullif(st.stuffing_before_next_available_date, '29990101')										--Дата выхода след набивки	
								,l.stuffing_kg																					--Остаток набивки	
								,l.stuffing_marking_kg																			--Остаток маркировки			
								,l.stuffing_shipment_kg																			--Отгружено из маркировки		
				
					

						FROM project_plan_production_finished_products.data_import.stuffing_plan_log_calculation	as l
						left join project_plan_production_finished_products.data_import.stuffing_plan				as st on l.stuffing_row_id = st.stuffing_row_id
						left join cherkizovo.info.products_sap														as sts on l.stuffing_sap_id = sts.sap_id

						left join project_plan_production_finished_products.data_import.shipment					as sh on l.shipment_row_id = sh.shipment_row_id	
						left join cherkizovo.info.products_sap														as shs on sh.shipment_sap_id = shs.sap_id	
						order by l.sort_id;

			end;

			if @log_type = 'marking' -- маркировка
			begin
						SELECT 
								 l.sort_id
								,sh.shipment_data_type
								,convert(varchar(24),FORMAT(sh.shipment_sap_id, '000000000000000000000000')) as shipment_sap_id
								,shs.product_1C_full_name as shipment_product_1C_full_name
								,sh.shipment_stuffing_id
								,sh.shipment_date
								,sh.shipment_priority
								,sh.shipment_min_KOS
								,l.shipment_kg

								,st.marking_data_type
								,l.marking_row_id
								,convert(varchar(24),FORMAT(st.marking_sap_id, '000000000000000000000000')) as marking_sap_id
								,sts.product_1C_full_name as marking_product_1C_full_name

								,st.marking_on_date
								,st.marking_current_KOS
								,l.marking_kg
							
								,l.marking_shipment_kg

						FROM project_plan_production_finished_products.data_import.marking_log_calculation	as l
						left join project_plan_production_finished_products.data_import.marking				as st  on l.marking_row_id = st.marking_row_id
						left join cherkizovo.info.products_sap												as sts on st.marking_sap_id = sts.sap_id

						left join project_plan_production_finished_products.data_import.shipment			as sh  on l.shipment_row_id = sh.shipment_row_id
						left join cherkizovo.info.products_sap												as shs on sh.shipment_sap_id = shs.sap_id
						order by l.sort_id;
			end;
end;

