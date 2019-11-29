use project_plan_production_finished_products 

go

-- exec project_plan_production_finished_products.report.for_deficit_and_surplus

ALTER PROCEDURE report.for_deficit_and_surplus
as
BEGIN
			SET NOCOUNT ON;
			
			declare @max_dt_shipment datetime;

			-- создаем таблицу куда будут загружены все данные
			begin
					IF OBJECT_ID('tempdb..#union_data','U') is not null drop table #union_data; 
					
					select top 0
								 sap_id															
								,stuffing_id 																				
								,shipment_customer_name															
								,shipment_sales_channel_name	
								,shipment_priority															
								,convert(datetime, null) as dt																
								
								,shipment_kg as stock_total_kg	
								,shipment_kg as stock_marking_total_kg															
								,shipment_kg as stock_KOS__0_49_kg														
								,shipment_kg as stock_KOS_50_59_kg														
								,shipment_kg as stock_KOS_60_69_kg														
								,shipment_kg as stock_KOS_70_79_kg														
								,shipment_kg as stock_KOS_80_99_kg															
								,shipment_kg as stock_KOS___100_kg														
								,shipment_kg as shipment_kg																
								,shipment_kg as net_need_kg																
								,shipment_kg as stock_after_shipment_kg																
																	
					into #union_data
					from project_plan_production_finished_products.data_import.shipments_sales_plan

			end;

			-- отгрузки в одну таблицу и добавляем в основную
			begin
					IF OBJECT_ID('tempdb..#shipments_union','U') is not null drop table #shipments_union; 

					select
						 row_id
						,name_table
						,sap_id
						,stuffing_id
						,shipment_sales_channel_name
						,shipment_customer_name
						,shipment_priority
						,shipment_date
						,shipment_kg
						,marking_net_need_kg 
					into #shipments_union
					from (

							select row_id, name_table, sap_id, stuffing_id, shipment_sales_channel_name, shipment_customer_name, shipment_priority, shipment_date, shipment_kg, marking_net_need_kg 
							from project_plan_production_finished_products.data_import.shipments_SAP
							where shipment_delete = 0 and stuffing_id_box_type in (0, 1)

							union all

							select row_id, name_table, sap_id, stuffing_id, shipment_sales_channel_name, shipment_customer_name, shipment_priority, shipment_date, shipment_kg, marking_net_need_kg 
							from project_plan_production_finished_products.data_import.shipments_1C
							where stuffing_id_box_type in (0, 1)

							union all

							select row_id, name_table, sap_id, stuffing_id, shipment_sales_channel_name, shipment_customer_name, shipment_priority, shipment_date, shipment_kg, marking_net_need_kg 
							from project_plan_production_finished_products.data_import.shipments_sales_plan
							where shipment_delete = 0 and stuffing_id_box_type in (0, 1)

						 ) as o;


					set @max_dt_shipment = (select max(shipment_date) from #shipments_union)

					-- добавляем данные
					insert into #union_data
					(
							 sap_id															
							,stuffing_id 																				
							,shipment_customer_name															
							,shipment_sales_channel_name	
							,shipment_priority															
							,dt													
							,shipment_kg																
							,net_need_kg																
					) 

					select
						 sap_id
						,stuffing_id
						,shipment_customer_name
						,shipment_sales_channel_name
						,shipment_priority
						,shipment_date
						,sum(shipment_kg) as shipment_kg
						,sum(marking_net_need_kg) as net_need_kg
					from #shipments_union
					group by 
						 sap_id
						,stuffing_id
						,shipment_customer_name
						,shipment_sales_channel_name
						,shipment_priority
						,shipment_date;


			end;
			

			-- остатки по дням
			begin

					IF OBJECT_ID('tempdb..#stock','U') is not null drop table #stock; 
					-- select * from #stock order by stock_on_date

					select
							 st.sap_id
							,st.stuffing_id
							,st.stock_on_date
							,iif(st.KOS_on_date < 0, 0, st.KOS_on_date) as KOS_on_date
							,sum(st.stock_kg) as stock_kg
							,sum(st.stock_after_shipment_kg) as stock_after_shipment_kg
					into #stock
					from (

							select 
									 st.row_id
									,st.name_table
									,st.sap_id
									,st.stuffing_id
									,cl.dt_tm as stock_on_date
									,st.stock_current_KOS - st.stock_KOS_in_day * DATEDIFF(day, st.stock_on_date, cl.dt_tm) as KOS_on_date
									--,st.stock_kg
									--,lst.shipment_kg
									,st.stock_kg - isnull(   sum(lst.shipment_kg) over (partition by st.row_id, st.name_table order by cl.dt_tm ROWS UNBOUNDED PRECEDING),   0) as stock_kg
									,iif(cl.dt_tm = st.stock_on_date, st.stock_after_shipment_kg, null) as stock_after_shipment_kg
							from cherkizovo.info.calendar as cl
							join (

										select st.row_id, st.name_table, st.sap_id, st.stuffing_id, st.stock_on_date, st.stock_current_KOS, st.stock_KOS_in_day, st.stock_kg, st.stock_after_shipment_kg
										from project_plan_production_finished_products.data_import.stock as st
										union all
										select st.row_id, st.name_table, st.sap_id, st.stuffing_id, st.stock_on_date, st.stock_current_KOS, st.stock_KOS_in_day, st.stock_kg, st.stock_after_shipment_kg
										from project_plan_production_finished_products.data_import.transits as st

								 ) as st on st.stock_on_date <= cl.dt_tm and cl.dt_tm <= @max_dt_shipment
							left join (
										select 
												 lg.stock_row_id
												,lg.stock_name_table
												,lg.shipment_date + 1
												,sum(lg.stock_shipment_kg) as shipment_kg
										from project_plan_production_finished_products.data_import.stock_log_calculation as lg
										where not lg.stock_shipment_kg is null
										group by 
												 lg.stock_row_id
												,lg.stock_name_table
												,lg.shipment_date
									  ) as lst on st.row_id = lst.stock_row_id and st.name_table = lst.stock_name_table and cl.dt_tm = lst.shipment_date
							--order by st.row_id
						) as st
					where st.stock_kg > 0
					group by 
							 st.sap_id
							,st.stuffing_id
							,st.stock_on_date
							,iif(st.KOS_on_date < 0, 0, st.KOS_on_date);

			end;

			-- маркировка по дням
			begin

					IF OBJECT_ID('tempdb..#marking','U') is not null drop table #marking; 
					-- select * from #marking order by stock_on_date
					select
							 st.sap_id		
							,st.stuffing_id					
							,st.stock_on_date
							,iif(st.KOS_on_date < 0, 0, st.KOS_on_date) as KOS_on_date
							,sum(st.stock_kg) as stock_kg
							,sum(st.stock_after_shipment_kg) as stock_after_shipment_kg
					into #marking
					from (
							select 
									 st.row_id
									,st.sap_id	
									,st.stuffing_id								
									,cl.dt_tm as stock_on_date
									,st.marking_current_KOS - st.marking_KOS_in_day * DATEDIFF(day, st.marking_on_date, cl.dt_tm) as KOS_on_date
									--,st.marking_kg
									--,lst.shipment_kg
									,st.marking_kg - isnull(   sum(lst.shipment_kg) over (partition by st.row_id order by cl.dt_tm ROWS UNBOUNDED PRECEDING),   0) as stock_kg
									,iif(cl.dt_tm = st.marking_on_date, st.marking_after_shipment_kg, null) as stock_after_shipment_kg
							from cherkizovo.info.calendar as cl
							join project_plan_production_finished_products.data_import.marking as st on st.marking_on_date <= cl.dt_tm and cl.dt_tm <= @max_dt_shipment
							left join (
										select 
												 lg.marking_row_id
												,sh.shipment_date + 1 as shipment_date
												,sum(lg.marking_shipment_kg) as shipment_kg
										from project_plan_production_finished_products.data_import.marking_log_calculation as lg
										join #shipments_union as sh on lg.shipment_row_id = sh.row_id and lg.shipment_name_table  = sh.name_table
										group by 
												 lg.marking_row_id
												,sh.shipment_date
									  ) as lst on st.row_id = lst.marking_row_id and cl.dt_tm = lst.shipment_date
							--order by st.row_id
						) as st
					where st.stock_kg > 0
					group by 
							 st.sap_id
							,st.stuffing_id
							,st.stock_on_date
							,iif(st.KOS_on_date < 0, 0, st.KOS_on_date);

			end;

			-- остатки + маркировка - вставляем в основную
			begin

					insert into #union_data
					(

								 sap_id															
								,stuffing_id 																
								,dt	
								,stock_total_kg															
								,stock_marking_total_kg															
								,stock_KOS__0_49_kg														
								,stock_KOS_50_59_kg														
								,stock_KOS_60_69_kg														
								,stock_KOS_70_79_kg														
								,stock_KOS_80_99_kg															
								,stock_KOS___100_kg		
								,stock_after_shipment_kg													
					)

					select 
							 st.sap_id
							,st.stuffing_id
							,st.stock_on_date as dt
							,sum(st.stock_kg) as stock_total_kg
							,sum(st.marking_kg) as stock_marking_total_kg 		

							,sum(case st.category_KOS when '0% - 49%'	then st.stock_kg end) as stock_KOS__0_49_kg		
							,sum(case st.category_KOS when '50% - 59%'	then st.stock_kg end) as stock_KOS_50_59_kg		
							,sum(case st.category_KOS when '60% - 69%'	then st.stock_kg end) as stock_KOS_60_69_kg		
							,sum(case st.category_KOS when '70% - 79%'	then st.stock_kg end) as stock_KOS_70_79_kg		
							,sum(case st.category_KOS when '80% - 99%'	then st.stock_kg end) as stock_KOS_80_99_kg		
							,sum(case st.category_KOS when '100%'		then st.stock_kg end) as stock_KOS___100_kg	
								
							,sum(st.stock_after_shipment_kg) as stock_after_shipment_kg

					from (
							select 
									 sap_id
									,stuffing_id
									,stock_on_date
									,KOS_on_date
									,stock_kg 
									,null as marking_kg
									,case 
										when KOS_on_date <0.5	then	'0% - 49%'	
										when KOS_on_date <0.6	then	'50% - 59%'	
										when KOS_on_date <0.7	then	'60% - 69%'	
										when KOS_on_date <0.8	then	'70% - 79%'	
										when KOS_on_date <1		then	'80% - 99%'	
										else							'100%'								
									end category_KOS
									,stock_after_shipment_kg
							from #stock

							union all

							select 
									 sap_id
									,stuffing_id
									,stock_on_date
									,KOS_on_date
									,stock_kg 
									,stock_kg as marking_kg
									,case 
										when KOS_on_date <0.5	then	'0% - 49%'	
										when KOS_on_date <0.6	then	'50% - 59%'	
										when KOS_on_date <0.7	then	'60% - 69%'	
										when KOS_on_date <0.8	then	'70% - 79%'	
										when KOS_on_date <1		then	'80% - 99%'	
										else							'100%'	
									end category_KOS
									,stock_after_shipment_kg
							from #marking

						 ) as st
					group by 
							 st.sap_id
							,st.stuffing_id
							,st.stock_on_date;

			end;



			--выгружаем отчет
			begin

					select 
							 'Общий итог' = 'Общий итог'
							,'Код набивки' = ud.stuffing_id
							,'SAP ID' = convert(varchar(24),FORMAT(ud.sap_id, '000000000000000000000000')) 
							,'Наименование SKU' = ps.product_1C_full_name
							,'Контрагент' = ud.shipment_customer_name
							,'Канал сбыта' = ud.shipment_sales_channel_name

							,'Наименование SKU ->' = ps.product_1C_full_name
							,'Контрагент ->' = ud.shipment_customer_name
							,'Канал сбыта ->' = ud.shipment_sales_channel_name

							,'ДТ' = ud.dt

							,'Тек ост' = ud.stock_total_kg	
							,'Набивка ост' = ud.stock_marking_total_kg	

							,'Ост КОС 0%-49%'	= ud.stock_KOS__0_49_kg													
							,'Ост КОС 50%-59%'	= ud.stock_KOS_50_59_kg													
							,'Ост КОС 60%-69%'	= ud.stock_KOS_60_69_kg													
							,'Ост КОС 70%-79%'	= ud.stock_KOS_70_79_kg													
							,'Ост КОС 80%-99%'	= ud.stock_KOS_80_99_kg														
							,'Ост КОС 100%'		= ud.stock_KOS___100_kg				
								
							,'Заявка/план' = ud.shipment_kg																
							,'Дефицит' = ud.net_need_kg																	
							,'Профицит' = ud.stock_after_shipment_kg																
					from #union_data as ud
					left join cherkizovo.info.products_sap as ps on ud.sap_id = ps.sap_id;
					--left join cherkizovo.info.stuffing as st on u.stuffing_id = st.stuffing_id
			end;


end;








































