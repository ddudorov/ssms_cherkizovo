use project_smoked_sausages_production_planning

go

-- exec report.for_analytics_movement_day @level_date='day'
-- exec report.for_analytics_movement_day @level_date='week'


ALTER PROCEDURE report.for_analytics_movement @level_date varchar(10)
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
								,customer_id					varchar(20)		null 																				
								,customer_name					varchar(100)	null												
								,sales_channel_name				varchar(25)		null											
								,type_channel_name				varchar(25)		null									
								,dt								datetime	not null							
								,dt_shipment_customer			datetime	not null			
								
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
							,customer_id
							,customer_name
							,sales_channel_name
							,dt
							,dt_shipment_customer
							,shipment_kg
							,deficit_kg
					)

					select 
							 s.shipment_sap_id
							,s.shipment_stuffing_id
							,s.shipment_customer_id
							,s.shipment_customer_name
							,s.shipment_sales_channel_name
							,s.shipment_date
							,isnull(s.shipment_with_branch_date,s.shipment_date) as shipment_with_branch_date -- для заявок SAP и заявок 1С берем дату отгрузки
							,s.shipment_after_marking_kg as shipment_kg
							,s.shipment_after_marking_kg as deficit_kg 
					from .data_import.shipment as s
					where shipment_delete = 0 and shipment_stuffing_id_box_type in (0, 1)
						and not shipment_after_marking_kg is null;

			end;

			begin -- ВСТАВЛЯЕМ ОТГРУЗКУ ИЗ ОСТАТКОВ И МАРКИРОВКИ, ОНА ИДЕТ КАК ПОТРЕБНОСТЬ, НО ТАК КАК SAP ID МОЖЕТ БЫТЬ РАЗНЫЙ ИЗ-ЗА ГРУППЫ АРТИКУЛОВ 
			
					insert into #union_data
					(
							 sap_id
							,stuffing_id
							,customer_id
							,customer_name
							,sales_channel_name
							,dt
							,dt_shipment_customer
							,shipment_kg
					)
					select 
							 st.stock_sap_id
							,st.stock_stuffing_id
							,sh.shipment_customer_id
							,sh.shipment_customer_name
							,sh.shipment_sales_channel_name
							,l.shipment_date
							,isnull(sh.shipment_with_branch_date,sh.shipment_date) as shipment_with_branch_date -- для заявок SAP и заявок 1С берем дату отгрузки
							,l.stock_shipment_kg
					from .data_import.stock_log_calculation as l
					join .data_import.shipment as sh on l.shipment_row_id = sh.shipment_row_id
					join .data_import.stock	as st on l.stock_row_id = st.stock_row_id
					where not l.stock_shipment_kg is null

					union all

					select 
							 st.marking_sap_id
							,st.marking_stuffing_id
							,sh.shipment_customer_id
							,sh.shipment_customer_name
							,sh.shipment_sales_channel_name
							,l.shipment_date
							,isnull(sh.shipment_with_branch_date,sh.shipment_date) as shipment_with_branch_date -- для заявок SAP и заявок 1С берем дату отгрузки
							,l.marking_shipment_kg
					from .data_import.marking_log_calculation as l
					join .data_import.shipment as sh on l.shipment_row_id = sh.shipment_row_id
					join .data_import.marking  as st on l.marking_row_id = st.marking_row_id
					where not l.marking_shipment_kg is null;

			end;

			begin -- ОСТАТКИ И МАРКИРОВКА 
					
					IF OBJECT_ID('tempdb..#calendar','U') is not null drop table #calendar; 

					select dt_tm as dt
					into #calendar
					from cherkizovo.info.calendar
					where dt_tm between (select min(shipment_date)				from .data_import.shipment)
					                and (select max(shipment_with_branch_date)	from .data_import.shipment);


					begin -- вставляем остатки 

							insert into #union_data
							(
										 sap_id																	
										,stuffing_id					
										,dt							
										,dt_shipment_customer
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
									,st.dt
									,st.dt as dt_shipment_customer
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
											,c.dt
											,iif(st.stock_current_KOS - DATEDIFF(day,st.stock_on_date, c.dt) * st.stock_KOS_in_day > 0
												,st.stock_current_KOS - DATEDIFF(day,st.stock_on_date, c.dt) * st.stock_KOS_in_day, 0) as stock_KOS
						
											,st.stock_kg
											,st.stock_shipment_kg
											,st.stock_after_shipment_kg
											,(select sum(l.stock_shipment_kg) 
											  from .data_import.stock_log_calculation as l
											  where st.stock_row_id = l.stock_row_id
												and c.dt > l.shipment_date 
												and not l.stock_shipment_kg is null) as stock_log_shipment_kg
									from .data_import.stock as st
									join #calendar as c on st.stock_on_date <= c.dt
									where st.stock_reason_ignore_in_calculate is null
								 ) as st
							where st.stock_kg - isnull(st.stock_log_shipment_kg, 0) <> 0
							group by 
									 st.stock_sap_id
									,st.stock_stuffing_id
									,st.dt;

					end;


					begin -- вставляем маркировку 

							insert into #union_data
							(
										 sap_id																	
										,stuffing_id					
										,dt		
										,dt_shipment_customer
										,stock_total_kg
										,stock_marking_total_kg		
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
									,st.dt
									,st.dt as dt_shipment_customer
									,sum(st.marking_kg - isnull(st.marking_log_shipment_kg, 0)) as marking_total_kg
									,sum(st.marking_marking_total_kg) as marking_marking_total_kg
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
											,c.dt
											,iif(st.marking_current_KOS - DATEDIFF(day,st.marking_on_date, c.dt) * st.marking_KOS_in_day > 0
												,st.marking_current_KOS - DATEDIFF(day,st.marking_on_date, c.dt) * st.marking_KOS_in_day, 0) as marking_KOS
											,iif(c.dt = st.marking_on_date, st.marking_kg, null) as marking_marking_total_kg
											,st.marking_kg
											,st.marking_shipment_kg
											,st.marking_after_shipment_kg
											,(select sum(l.marking_shipment_kg) 
											  from .data_import.marking_log_calculation as l
											  where st.marking_row_id = l.marking_row_id
												and c.dt > l.shipment_date
												and not l.marking_shipment_kg is null) as marking_log_shipment_kg
									from .data_import.marking as st
									join #calendar as c on st.marking_on_date <= c.dt
									where st.marking_reason_ignore_in_calculate is null
								 ) as st
							where st.marking_kg - isnull(st.marking_log_shipment_kg, 0) <> 0
							group by 
									 st.marking_sap_id
									,st.marking_stuffing_id
									,st.dt;

					end;

			end;
			

			begin -- добавляем канал
					
					update ud
					set ud.type_channel_name = isnull(ic.type_channel_name,'Канал отсутствует')
					from #union_data as ud 
					left join info.customers as ic on ud.customer_id = ic.customer_id and ud.sales_channel_name = ic.sales_channel_name;
				
			end;

			begin --выгружаем отчет 

					--declare @level_date varchar(10); set @level_date='week'

					if  @level_date='day'
					begin
					
							select 
									 'Общий итог'			= 'Общий итог'
									,'Наименование набивки'	= st.stuffing_name						
									,'Наименование SKU'		= ps.product_1C_full_name									
									,'Контрагент'			= isnull(ud.customer_name,' остатки')
									,'Канал'				= ud.type_channel_name
									,'Канал сбыта'			= isnull(ud.sales_channel_name,' остатки')

									,'Наименование SKU ->'	= ps.product_1C_full_name
									,'Контрагент ->'		= ud.customer_name
									,'Канал ->'				= ud.type_channel_name
									,'Канал сбыта ->'		= ud.sales_channel_name

									,'ДТ'					= ud.dt
									,'ДТ Клиенту'			= ud.dt_shipment_customer

									,'Тек ост'				= sum(ud.stock_total_kg)
									,'Приход ГП'			= sum(ud.stock_marking_total_kg)

									,'Ост КОС 0%-49%'		= sum(ud.stock_KOS__0_49_kg)												
									,'Ост КОС 50%-59%'		= sum(ud.stock_KOS_50_59_kg)												
									,'Ост КОС 60%-69%'		= sum(ud.stock_KOS_60_69_kg)												
									,'Ост КОС 70%-79%'		= sum(ud.stock_KOS_70_79_kg)												
									,'Ост КОС 80%-99%'		= sum(ud.stock_KOS_80_99_kg)													
									,'Ост КОС 100%'			= sum(ud.stock_KOS___100_kg)			
								
									,'Продажи'				= sum(ud.shipment_kg)																
									,'Дефицит'				= sum(ud.deficit_kg)																
									,'Профицит'				= sum(ud.surplus_kg)															
							from #union_data as ud
							join info_view.sap_id as ps				on ud.sap_id = ps.sap_id_for_join
							join info.stuffing as st				on ud.stuffing_id = st.stuffing_id
							group by 
									 st.stuffing_name
									,ps.product_1C_full_name
									,ud.customer_name
									,ud.type_channel_name
									,ud.sales_channel_name
									,ud.dt
									,ud.dt_shipment_customer;
					end;

					if  @level_date='week'
					begin
					
							select 
									 'Общий итог'			= 'Общий итог'
									,'Наименование набивки'	= st.stuffing_name						
									,'Наименование SKU'		= ps.product_1C_full_name									
									,'Контрагент'			= isnull(ud.customer_name,' остатки')
									,'Канал'				= ud.type_channel_name
									,'Канал сбыта'			= isnull(ud.sales_channel_name,' остатки')


									,'Наименование SKU ->'	= ps.product_1C_full_name
									,'Контрагент ->'		= ud.customer_name
									,'Канал ->'				= ud.type_channel_name
									,'Канал сбыта ->'		= ud.sales_channel_name

									,'ДТ'					= left(cl1.year_week_number, 4) + '|' + RIGHT(cl1.year_week_number, 2)
									,'ДТ Клиенту'			= left(cl2.year_week_number, 4) + '|' + RIGHT(cl2.year_week_number, 2)

									,'Тек ост'				= sum(iif(cl1.week_day = 1, ud.stock_total_kg, null))	
									,'Приход ГП'			= sum(ud.stock_marking_total_kg)
							
									,'Ост КОС 0%-49%'		= sum(iif(cl1.week_day = 1, ud.stock_KOS__0_49_kg, null))												
									,'Ост КОС 50%-59%'		= sum(iif(cl1.week_day = 1, ud.stock_KOS_50_59_kg, null))												
									,'Ост КОС 60%-69%'		= sum(iif(cl1.week_day = 1, ud.stock_KOS_60_69_kg, null))												
									,'Ост КОС 70%-79%'		= sum(iif(cl1.week_day = 1, ud.stock_KOS_70_79_kg, null))												
									,'Ост КОС 80%-99%'		= sum(iif(cl1.week_day = 1, ud.stock_KOS_80_99_kg, null))													
									,'Ост КОС 100%'			= sum(iif(cl1.week_day = 1, ud.stock_KOS___100_kg, null))			
								
									,'Продажи'				= sum(ud.shipment_kg)																
									,'Дефицит'				= sum(ud.deficit_kg)																
									,'Профицит'				= sum(iif(cl1.week_day = 1, ud.surplus_kg, null))																
							from #union_data as ud
							join info_view.sap_id as ps				on ud.sap_id = ps.sap_id_for_join
							join info.stuffing as st				on ud.stuffing_id = st.stuffing_id
							join cherkizovo.info.calendar as cl1	on ud.dt = cl1.dt
							join cherkizovo.info.calendar as cl2	on ud.dt_shipment_customer = cl2.dt
							group by 
									 st.stuffing_name
									,ps.product_1C_full_name
									,ud.customer_name
									,ud.type_channel_name
									,ud.sales_channel_name
									,left(cl1.year_week_number, 4) + '|' + RIGHT(cl1.year_week_number, 2)
									,left(cl2.year_week_number, 4) + '|' + RIGHT(cl2.year_week_number, 2);
					end;



							
			end;


end;








































