use project_plan_production_finished_products

-- exec project_plan_production_finished_products.report.for_analytics_total_week		

go

alter procedure report.for_analytics_total_week													
as
BEGIN
			SET NOCOUNT ON;
			
			begin -- ТАБЛИЦА ДЛЯ ОТЧЕТА 

						IF OBJECT_ID('tempdb..#analytics_total','U') is not null drop table #analytics_total; 

						create table #analytics_total
						(
								 dt									int				null
								 
								,stock_on_monday_total_kg			dec(15,5)		null	-- итого остатков на понедельник		
								,stock_on_monday_for_shipment_kg	as stock_on_monday_total_kg - isnull(stock_on_monday_unclaimed_kg, 0)
								,stock_on_monday_unclaimed_kg		dec(15,5)		null	-- остаток невостребованный на понедельник

								,stuffing_fact_kg					dec(11,5)		null	-- выход фактических набивок
								,stuffing_plan_kg					dec(11,5)		null	-- выход плановых набивок
								
								,marking_kg							dec(11,5)		null	-- маркировка

								,shipment_kg						dec(15,5)		null	-- потребность всего	
								,deficit_kg							dec(15,5)		null	-- дефицит		
								,deficit_blocked_kg					dec(15,5)		null	-- дефицит на блокированных артикулах		

						);


			end;


			begin -- набивки факт | набивки план | маркировка | потребность
						

						-- набивки факт
						insert into #analytics_total 
								( dt, stuffing_fact_kg)

						select	c.year_week_number, sum(f.stuffing_kg)
						from project_plan_production_finished_products.data_import.stuffing_fact as f
						join cherkizovo.info.calendar as c on f.stuffing_available_date = c.dt_tm
						group by c.year_week_number;


						-- набивки план
						insert into #analytics_total 
								( dt, stuffing_plan_kg)

						select	c.year_week_number, sum(f.stuffing_kg)
						from project_plan_production_finished_products.data_import.stuffing_plan as f
						join cherkizovo.info.calendar as c on f.stuffing_available_date = c.dt_tm
						group by c.year_week_number;


						-- маркировка
						insert into #analytics_total 
								( dt, marking_kg)

						select	c.year_week_number, sum(f.marking_kg)
						from project_plan_production_finished_products.data_import.marking as f
						join cherkizovo.info.calendar as c on f.marking_on_date = c.dt_tm
						group by c.year_week_number;


						-- потребность
						insert into #analytics_total 
								( dt, shipment_kg, deficit_kg, deficit_blocked_kg)

						select	c.year_week_number, sum(f.shipment_kg), sum(f.shipment_after_marking_kg), sum(iif( isnull(s.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший'), f.shipment_after_marking_kg, null))
						from project_plan_production_finished_products.data_import.shipment as f
						join cherkizovo.info.calendar as c on f.shipment_date = c.dt_tm
						join cherkizovo.info.products_sap as s on f.shipment_sap_id = s.sap_id
						where f.shipment_delete = 0
						  and f.shipment_stuffing_id_box_type in (0, 1)
						group by c.year_week_number;

			end;


			begin -- остатки | маркировка на понедельник

					IF OBJECT_ID('tempdb..#calendar','U') is not null drop table #calendar; 

					select dt_tm
					into #calendar
					from cherkizovo.info.calendar
					where dt_tm between (select min(shipment_date) from project_plan_production_finished_products.data_import.shipment)
					                and (select max(shipment_date) from project_plan_production_finished_products.data_import.shipment);


					begin -- вставляем остатки 

							insert into #analytics_total
							(
								 dt			
								 
								,stock_on_monday_total_kg		
								,stock_on_monday_unclaimed_kg	
							)
					
							select
									 c.year_week_number
									,sum(st.stock_kg - isnull(st.stock_log_shipment_kg, 0)) as stock_total_kg
									,sum(st.stock_after_shipment_kg) as unclaimed_kg
							from (
									select 
											 st.stock_row_id
											,st.stock_sap_id
											,st.stock_stuffing_id
											,c.dt_tm
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
							join cherkizovo.info.calendar as c on st.dt_tm = c.dt_tm
							where st.stock_kg - isnull(st.stock_log_shipment_kg, 0) <> 0
							  and c.week_day = 1

							group by c.year_week_number;

					end;

					
					begin -- вставляем маркировку 

							insert into #analytics_total
							(
								 dt			
								 
								,stock_on_monday_total_kg		
								,stock_on_monday_unclaimed_kg	
							)
					
							select
									 c.year_week_number
									,sum(st.marking_kg - isnull(st.marking_log_shipment_kg, 0)) as marking_on_monday_total_kg
									,sum(st.marking_after_shipment_kg)							as marking_on_monday_unclaimed_kg
							from (
									select 
											 st.marking_row_id
											,st.marking_sap_id
											,st.marking_stuffing_id
											,c.dt_tm
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
							join cherkizovo.info.calendar as c on st.dt_tm = c.dt_tm 
							where st.marking_kg - isnull(st.marking_log_shipment_kg, 0) <> 0
							  and c.week_day = 1
							group by c.year_week_number;

					end;

			end;


			begin -- ВЫГРУЖАЕМ ОТЧЕТ


					select 
							 'Год|№ недели'										= left(o.dt, 4) + '|' + RIGHT(o.dt, 2)
																												
							,'Подходящий остаток для отгрузки с понедельника'	= sum(o.stock_on_monday_for_shipment_kg)			
							,'Невостребованный остаток (накопительно)'			= sum(o.stock_on_monday_unclaimed_kg)
							,'Итого остаток'									= sum(o.stock_on_monday_total_kg)			
							
							,'Выход (в камерах)'								= sum(o.stuffing_fact_kg)																			
							,'Выход (план)'										= sum(o.stuffing_plan_kg)

							,'Итого приход маркированной ГП'					= sum(o.marking_kg)									
																																	
							,'План отгрузок'									= sum(o.shipment_kg)	
																										
							,'Дефицит'											= - sum(o.deficit_kg)	
							,'Дефицит на заблокированных артикулах'				= - sum(o.deficit_blocked_kg)				
							,'Уровень сервиса (SL)'								= 1 - isnull(sum(o.deficit_kg), 0) / sum(o.shipment_kg)
																																									
					from #analytics_total as o 
					group by o.dt

			end;

end;




			