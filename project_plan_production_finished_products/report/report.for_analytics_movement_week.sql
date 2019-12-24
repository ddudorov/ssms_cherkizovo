﻿use project_plan_production_finished_products 

go

-- exec project_plan_production_finished_products.report.for_analytics_movement_week


ALTER PROCEDURE report.for_analytics_movement_week
as
BEGIN
			SET NOCOUNT ON;
			
			set language Russian;

			
			begin -- СОЗДАЕМ ТАБЛИЦУ КУДА БУДУТ ЗАГРУЖЕНЫ ВСЕ ДАННЫЕ

					IF OBJECT_ID('tempdb..#union_data','U') is not null drop table #union_data; 

					create table #union_data
					(
								 sap_id							bigint			null														
								,stuffing_id					varchar(40)		null												
								,sales_channel_name				varchar(25)		null																				
								,customer_name					varchar(100)	null								
								,dt								varchar(10)	not null		
								
								,stock_total_kg					dec(11, 5)		null
								,stock_marking_total_kg			dec(11, 5)		null						
								,stock_KOS__0_49_kg				dec(11, 5)		null				
								,stock_KOS_50_59_kg				dec(11, 5)		null				
								,stock_KOS_60_69_kg				dec(11, 5)		null				
								,stock_KOS_70_79_kg				dec(11, 5)		null				
								,stock_KOS_80_99_kg				dec(11, 5)		null					
								,stock_KOS___100_kg				dec(11, 5)		null				
								,shipment_kg					dec(11, 5)		null					
								,deficit_kg						dec(11, 5)		null				
								,surplus_kg					    dec(11, 5)		null
					);

			end;
						
			begin -- ВСТАВЛЯЕМ ТО, ЧТО НЕ СМОГЛИ ОТГРУЗИТЬ, ОНО ИДЕТ КАК ОТГРУЗКА ТАК И ДЕФИЦИТ 

					insert into #union_data
					(
							 sap_id
							,stuffing_id
							,sales_channel_name
							,customer_name
							,dt
							,shipment_kg
							,deficit_kg
					)

					select 
							 s.shipment_sap_id
							,s.shipment_stuffing_id
							,s.shipment_sales_channel_name
							,s.shipment_customer_name
							,left(c.year_week_number, 4) + '|' + RIGHT(c.year_week_number, 2) as dt
							,sum(s.shipment_after_marking_kg) as shipment_kg
							,sum(s.shipment_after_marking_kg) as deficit_kg 
					from project_plan_production_finished_products.data_import.shipment as s
					join cherkizovo.info.calendar as c on s.shipment_date = c.dt_tm
					where shipment_delete = 0 and shipment_stuffing_id_box_type in (0, 1)
						and not shipment_after_marking_kg is null
					group by 
							 s.shipment_sap_id
							,s.shipment_stuffing_id
							,s.shipment_sales_channel_name
							,s.shipment_customer_name
							,c.year_week_number;

			end;

			begin -- ВСТАВЛЯЕМ ОТГРУЗКУ ИЗ ОСТАТКОВ И МАРКИРОВКИ, ОНА ИДЕТ КАК ПОТРЕБНОСТЬ, НО ТАК КАК SAP ID МОЖЕТ БЫТЬ РАЗНЫЙ ИЗ-ЗА ГРУППЫ АРТИКУЛОВ
			
					insert into #union_data
					(
							 sap_id
							,stuffing_id
							,sales_channel_name
							,customer_name
							,dt
							,shipment_kg
					)
					select 
							 st.stock_sap_id
							,st.stock_stuffing_id
							,sh.shipment_sales_channel_name
							,sh.shipment_customer_name
							,left(c.year_week_number, 4) + '|' + RIGHT(c.year_week_number, 2) as dt
							,l.stock_shipment_kg
					from project_plan_production_finished_products.data_import.stock_log_calculation as l
					join project_plan_production_finished_products.data_import.shipment as sh on l.shipment_row_id = sh.shipment_row_id
					join project_plan_production_finished_products.data_import.stock	as st on l.stock_row_id = st.stock_row_id
					join cherkizovo.info.calendar										as c on l.shipment_date = c.dt_tm
					where not l.stock_shipment_kg is null;


					insert into #union_data
					(
							 sap_id
							,stuffing_id
							,sales_channel_name
							,customer_name
							,dt
							,shipment_kg
					)
					select 
							 st.marking_sap_id
							,st.marking_stuffing_id
							,sh.shipment_sales_channel_name
							,sh.shipment_customer_name
							,left(c.year_week_number, 4) + '|' + RIGHT(c.year_week_number, 2) as dt
							,l.marking_shipment_kg
					from project_plan_production_finished_products.data_import.marking_log_calculation as l
					join project_plan_production_finished_products.data_import.shipment as sh on l.shipment_row_id = sh.shipment_row_id
					join project_plan_production_finished_products.data_import.marking  as st on l.marking_row_id = st.marking_row_id
					join cherkizovo.info.calendar										as c on l.shipment_date = c.dt_tm
					where not l.marking_shipment_kg is null;

			end;

			begin -- ОСТАТКИ И МАРКИРОВКА
					
					IF OBJECT_ID('tempdb..#calendar','U') is not null drop table #calendar; 

					select dt_tm, left(year_week_number, 4) + '|' + RIGHT(year_week_number, 2) as dt, week_day
					into #calendar
					from cherkizovo.info.calendar
					where dt_tm between (select min(shipment_date) from project_plan_production_finished_products.data_import.shipment)
					                and (select max(shipment_date) from project_plan_production_finished_products.data_import.shipment);


					begin -- вставляем остатки 

							insert into #union_data
							(
										 sap_id																	
										,stuffing_id					
										,dt		
										,stock_total_kg		
										,stock_KOS__0_49_kg				
										,stock_KOS_50_59_kg				
										,stock_KOS_60_69_kg				
										,stock_KOS_70_79_kg				
										,stock_KOS_80_99_kg					
										,stock_KOS___100_kg	
										,surplus_kg		
							)
							select
									 st.stock_sap_id
									,st.stock_stuffing_id
									,c.dt
									,sum(st.stock_kg - isnull(st.stock_log_shipment_kg, 0)) as stock_total_kg
									,sum(iif(st.stock_KOS <  0.5						, st.stock_kg - isnull(st.stock_log_shipment_kg, 0), null)) as stock_KOS__0_49_kg
									,sum(iif(st.stock_KOS >= 0.5 and st.stock_KOS < 0.6 , st.stock_kg - isnull(st.stock_log_shipment_kg, 0), null)) as stock_KOS_50_59_kg
									,sum(iif(st.stock_KOS >= 0.6 and st.stock_KOS < 0.7 , st.stock_kg - isnull(st.stock_log_shipment_kg, 0), null)) as stock_KOS_60_69_kg
									,sum(iif(st.stock_KOS >= 0.7 and st.stock_KOS < 0.8 , st.stock_kg - isnull(st.stock_log_shipment_kg, 0), null)) as stock_KOS_70_79_kg
									,sum(iif(st.stock_KOS >= 0.8 and st.stock_KOS < 1   , st.stock_kg - isnull(st.stock_log_shipment_kg, 0), null)) as stock_KOS_80_99_kg
									,sum(iif(st.stock_KOS >= 1							, st.stock_kg - isnull(st.stock_log_shipment_kg, 0), null)) as stock_KOS___100_kg
									,sum(st.stock_after_shipment_kg) as surplus_kg
							from (
									select 
											 st.stock_row_id
											,st.stock_data_type
											,st.stock_sap_id
											,st.stock_stuffing_id
											,c.dt_tm
											,iif(st.stock_current_KOS - DATEDIFF(day,st.stock_on_date, c.dt_tm) * st.stock_KOS_in_day > 0
												,st.stock_current_KOS - DATEDIFF(day,st.stock_on_date, c.dt_tm) * st.stock_KOS_in_day, 0) as stock_KOS
						
											,st.stock_kg
											,st.stock_shipment_kg
											,st.stock_after_shipment_kg
											,(select sum(l.stock_shipment_kg) 
											  from project_plan_production_finished_products.data_import.stock_log_calculation as l
											  where st.stock_row_id = l.stock_row_id
												and c.dt_tm > l.shipment_date 
												and not l.stock_shipment_kg is null) as stock_log_shipment_kg
									from project_plan_production_finished_products.data_import.stock as st
									join #calendar as c on st.stock_on_date <= c.dt_tm
									where st.stock_reason_ignore_in_calculate is null
								 ) as st
							join #calendar as c on st.dt_tm = c.dt_tm 
							where st.stock_kg - isnull(st.stock_log_shipment_kg, 0) <> 0
							  and c.week_day = 1
							group by 
									 st.stock_sap_id
									,st.stock_stuffing_id
									,c.dt;

					end;


					begin -- вставляем маркировку 

							insert into #union_data
							(
										 sap_id																	
										,stuffing_id					
										,dt		
										,stock_total_kg
										,stock_KOS__0_49_kg				
										,stock_KOS_50_59_kg				
										,stock_KOS_60_69_kg				
										,stock_KOS_70_79_kg				
										,stock_KOS_80_99_kg					
										,stock_KOS___100_kg	
										,surplus_kg		
							)
							select
									 st.marking_sap_id
									,st.marking_stuffing_id
									,c.dt
									,sum( st.marking_kg - isnull(st.marking_log_shipment_kg, 0)) as marking_total_kg
									,sum(iif(st.marking_KOS <  0.5							, st.marking_kg - isnull(st.marking_log_shipment_kg, 0), null)) as marking_KOS__0_49_kg
									,sum(iif(st.marking_KOS >= 0.5 and st.marking_KOS < 0.6	, st.marking_kg - isnull(st.marking_log_shipment_kg, 0), null)) as marking_KOS_50_59_kg
									,sum(iif(st.marking_KOS >= 0.6 and st.marking_KOS < 0.7	, st.marking_kg - isnull(st.marking_log_shipment_kg, 0), null)) as marking_KOS_60_69_kg
									,sum(iif(st.marking_KOS >= 0.7 and st.marking_KOS < 0.8	, st.marking_kg - isnull(st.marking_log_shipment_kg, 0), null)) as marking_KOS_70_79_kg
									,sum(iif(st.marking_KOS >= 0.8 and st.marking_KOS < 1.0	, st.marking_kg - isnull(st.marking_log_shipment_kg, 0), null)) as marking_KOS_80_99_kg
									,sum(iif(st.marking_KOS >= 1.0							, st.marking_kg - isnull(st.marking_log_shipment_kg, 0), null)) as marking_KOS___100_kg
									,sum(st.marking_after_shipment_kg) as surplus_kg
							from (
									select 
											 st.marking_row_id
											,st.marking_data_type
											,st.marking_sap_id
											,st.marking_stuffing_id
											,c.dt_tm
											,iif(st.marking_current_KOS - DATEDIFF(day,st.marking_on_date, c.dt_tm) * st.marking_KOS_in_day > 0
												,st.marking_current_KOS - DATEDIFF(day,st.marking_on_date, c.dt_tm) * st.marking_KOS_in_day, 0) as marking_KOS
											,iif(c.dt = st.marking_on_date, st.marking_kg, null) as marking_marking_total_kg
											,st.marking_kg
											,st.marking_shipment_kg
											,st.marking_after_shipment_kg
											,(select sum(l.marking_shipment_kg) 
											  from project_plan_production_finished_products.data_import.marking_log_calculation as l
											  where st.marking_row_id = l.marking_row_id
												and c.dt_tm > l.shipment_date
												and not l.marking_shipment_kg is null) as marking_log_shipment_kg
									from project_plan_production_finished_products.data_import.marking as st
									join #calendar as c on st.marking_on_date <= c.dt_tm
									where st.marking_reason_ignore_in_calculate is null
								 ) as st
							join #calendar as c on st.dt_tm = c.dt_tm 
							where st.marking_kg - isnull(st.marking_log_shipment_kg, 0) <> 0
							  and c.week_day = 1
							group by 
									 st.marking_sap_id
									,st.marking_stuffing_id
									,c.dt;

					end;


					
					begin  -- загружаем приход ГП

							insert into #union_data
							(
										 sap_id																		
										,stuffing_id	
										,dt			
										,stock_marking_total_kg						
								
							)

							select 
									 st.marking_sap_id
									,st.marking_stuffing_id
									,c.dt
									,sum(st.marking_kg)
							from project_plan_production_finished_products.data_import.marking as st
							join #calendar as c on st.marking_on_date = c.dt_tm
							where st.marking_reason_ignore_in_calculate is null
							group by 
									 st.marking_sap_id
									,st.marking_stuffing_id
									,c.dt

					end;



			end;
			

			
			begin --выгружаем отчет 

					select 
							 'Общий итог'			= 'Общий итог'
							,'Наименование набивки'	= st.stuffing_name						
							,'Наименование SKU'		= ps.product_1C_full_name									
							,'Контрагент'			= isnull(ud.customer_name,' остатки')
							,'Канал сбыта'			= isnull(ud.sales_channel_name,' остатки')
							
							--,'Код набивки'			= ud.stuffing_id
							--,'SAP ID'				= convert(varchar(24),FORMAT(ud.sap_id, '000000000000000000000000')) 

							,'Наименование SKU ->'	= ps.product_1C_full_name
							,'Контрагент ->'		= ud.customer_name
							,'Канал сбыта ->'		= ud.sales_channel_name

							,'ДТ'					= ud.dt

							,'Тек ост'				= sum(ud.stock_total_kg)
							,'Приход ГП'			= sum(ud.stock_marking_total_kg)

							,'Ост КОС 0%-49%'		= sum(ud.stock_KOS__0_49_kg)												
							,'Ост КОС 50%-59%'		= sum(ud.stock_KOS_50_59_kg)												
							,'Ост КОС 60%-69%'		= sum(ud.stock_KOS_60_69_kg)												
							,'Ост КОС 70%-79%'		= sum(ud.stock_KOS_70_79_kg)												
							,'Ост КОС 80%-99%'		= sum(ud.stock_KOS_80_99_kg)													
							,'Ост КОС 100%'			= sum(ud.stock_KOS___100_kg)			
								
							,'Заявка/план'			= sum(ud.shipment_kg)																
							,'Дефицит'				= sum(ud.deficit_kg)																
							,'Профицит'				= sum(ud.surplus_kg)															
					from #union_data as ud
					join cherkizovo.info.products_sap as ps on ud.sap_id = ps.sap_id
					left join project_plan_production_finished_products.info.stuffing as st on ud.stuffing_id = st.stuffing_id
					group by 
							 st.stuffing_name
							,ps.product_1C_full_name
							--,ud.stuffing_id	
							--,ud.sap_id
							,ud.customer_name
							,ud.sales_channel_name
							,ud.dt;
							
			end;


end;







