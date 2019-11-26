use project_plan_production_finished_products

-- exec project_plan_production_finished_products.report.analytics		

go

alter procedure report.analytics													
as
BEGIN
			SET NOCOUNT ON;
			
			begin -- ТАБЛИЦА ДЛЯ ОТЧЕТА 

						IF OBJECT_ID('tempdb..#analytics_total','U') is not null drop table #analytics_total; 

						create table #analytics_total
						(
								 year_week_number								int			null
								 
								,stock_on_monday_total							dec(15,5)	null	-- итого остатков на понедельник			
								,stock_on_monday_for_shipment					dec(15,5)	null	-- остаток к отгрузке на понедельник	
								,stock_on_monday_unclaimed						dec(15,5)	null	-- остаток невостребованный на понедельник
								--,transit_on_monday_total						dec(15,5)	null	-- итого транзитов на понедельник			
								--,transit_on_monday_for_shipment					dec(15,5)	null	-- транзитов к отгрузке на понедельник	
								--,transit_on_monday_unclaimed					dec(15,5)	null	-- транзитов невостребованный на понедельник
								,transit_received								dec(15,5)	null	-- транзит приход
								,shipment_from_stock							dec(15,5)	null	-- отгружено из остатков и транзита

								,marking_total									dec(15,5)	null	-- итого маркировка (набивка факт + план) выход
								,marking_on_monday_total						dec(15,5)	null	-- итого маркировка (набивка факт + план) на понедельник			
								,marking_on_monday_for_shipment					dec(15,5)	null	-- маркировка (набивка факт + план) к отгрузке на понедельник	
								,marking_on_monday_unclaimed					dec(15,5)	null	-- маркировка (набивка факт + план) невостребованный на понедельник
								,shipment_from_marking							dec(15,5)	null	-- отгружено из набивок
								
								,stuffing_fact_output							dec(11,5)	null	-- выход фактических набивок
								,stuffing_plan_output							dec(11,5)	null	-- выход плановых набивок
								

								,shipment_total									dec(15,5)	null	-- потребность всего				
								,shipment_SAP									dec(15,5)	null	-- потребность SAP	
								,shipment_1C									dec(15,5)	null	-- потребность 1C	
								,shipment_sales_plan							dec(15,5)	null	-- потребность план продаж	
																

								,deficit_total									dec(15,5)	null	-- потребность всего				
								,deficit_SAP									dec(15,5)	null	-- потребность SAP	
								,deficit_1C										dec(15,5)	null	-- потребность 1C	
								,deficit_sales_plan								dec(15,5)	null	-- потребность план продаж	

						);


			end;

			begin -- ТРАНЗИТ ПРИХОДЫ 

						insert into #analytics_total 
						(
							 year_week_number
							,transit_received
						)
						select 
							cl.year_week_number as year_week_number
							,SUM(tr.stock_kg) as transit_received
						from project_plan_production_finished_products.data_import.transits as tr
						join cherkizovo.info.calendar as cl on tr.stock_on_date = cl.dt_tm
						group by cl.year_week_number
			end;

			begin -- ВЫХОД ПО ДАТУ ДОСТУПНОСТИ НАБИВКИ ФАКТ И ПЛАН И МАРКИРОВКА ВЫХОД ИТОГ ГП

						-- набивки выход факт
						insert into #analytics_total (year_week_number, stuffing_fact_output)
						select 
								cl.year_week_number as year_week_number
								,sum(o.stuffing_kg) as stuffing_fact_output
						from project_plan_production_finished_products.data_import.stuffing_fact as o
						join cherkizovo.info.calendar as cl on o.stuffing_available_date = cl.dt_tm
						group by cl.year_week_number;

						-- набивки выход план
						insert into #analytics_total (year_week_number, stuffing_plan_output)
						select 
								cl.year_week_number as year_week_number
								,sum(o.stuffing_kg) as stuffing_plan_output
						from project_plan_production_finished_products.data_import.stuffing_plan as o
						join cherkizovo.info.calendar as cl on o.stuffing_available_date = cl.dt_tm
						group by cl.year_week_number;

						-- ВЫХОД ИТОГО ГП
						insert into #analytics_total (year_week_number, marking_total)
						select 
								cl.year_week_number as year_week_number
								,sum(o.marking_kg) as marking_total
						from project_plan_production_finished_products.data_import.marking as o
						join cherkizovo.info.calendar as cl on o.marking_on_date = cl.dt_tm
						group by cl.year_week_number;

			end;

			begin -- ПОТРЕБНОСТЬ И ДЕФИЦИТ

						insert into #analytics_total 
						(
							 year_week_number

							,shipment_total			
							,shipment_SAP			
							,shipment_1C			
							,shipment_sales_plan	
												
							,shipment_from_stock	
							,shipment_from_marking	
						
							,deficit_total			
							,deficit_SAP			
							,deficit_1C				
							,deficit_sales_plan		
						)
												
						select 
								 cl.year_week_number
								,sum(o.shipment_kg) as shipment_total
								,SUM( IIF(o.name_table = 'shipments_SAP'		,o.shipment_kg,null) ) as shipment_SAP
								,SUM( IIF(o.name_table = 'shipments_1C'			,o.shipment_kg,null) ) as shipment_1C
								,SUM( IIF(o.name_table = 'shipments_sales_plan'	,o.shipment_kg,null) ) as shipment_sales_plan

								,sum(o.stock_shipment_kg) as shipment_from_stock
								,sum(o.marking_shipment_kg) as shipment_from_marking
								
								,sum(o.marking_net_need_kg) as deficit_total
								,SUM( IIF(o.name_table = 'shipments_SAP'		,o.marking_net_need_kg,null) ) as deficit_SAP
								,SUM( IIF(o.name_table = 'shipments_1C'			,o.marking_net_need_kg,null) ) as deficit_1C
								,SUM( IIF(o.name_table = 'shipments_sales_plan'	,o.marking_net_need_kg,null) ) as deficit_sales_plan


						from (
								select o.shipment_date, o.name_table, o.shipment_kg, o.stock_shipment_kg, o.marking_shipment_kg, o.marking_net_need_kg
								from project_plan_production_finished_products.data_import.shipments_SAP as o
								where o.stuffing_id_box_type in (0, 1)
								  and o.shipment_delete = 0

								union all
								
								select o.shipment_date, o.name_table, o.shipment_kg, o.stock_shipment_kg, o.marking_shipment_kg, o.marking_net_need_kg
								from project_plan_production_finished_products.data_import.shipments_1C as o
								where o.stuffing_id_box_type in (0, 1)
		  
								union all
								
								select o.shipment_date, o.name_table, o.shipment_kg, o.stock_shipment_kg, o.marking_shipment_kg, o.marking_net_need_kg
								from project_plan_production_finished_products.data_import.shipments_sales_plan as o
								where o.stuffing_id_box_type in (0, 1)
								  and o.shipment_delete = 0

							 ) as o
						join cherkizovo.info.calendar as cl on o.shipment_date = cl.dt_tm
						group by cl.year_week_number;

			end;

			begin -- ОСТАТКИ 

						declare @dt_min_stock as datetime; set @dt_min_stock = (select MIN(stock_on_date) from project_plan_production_finished_products.data_import.stock);
						declare @dt_max_stock as datetime; set @dt_max_stock = (select MAX(shipment_date) from project_plan_production_finished_products.data_import.shipments_sales_plan);

						IF OBJECT_ID('tempdb..#calendar_stock','U') is not null drop table #calendar_stock; 

						select
							 dt_tm
							,week_day
							,year_week_number
						into #calendar_stock
						from cherkizovo.info.calendar
						where dt_tm between @dt_min_stock and @dt_max_stock;

						insert into #analytics_total 
						(
								 year_week_number
								,stock_on_monday_total
								,stock_on_monday_for_shipment
								,stock_on_monday_unclaimed
						)

						select
								 st.year_week_number
								,sum(st.stock_on_date_kg) as stock_on_monday_total
								,nullif( isnull(sum(st.stock_on_date_kg),0) - isnull(sum(st.stock_after_shipment_kg), 0) , 0)  as stock_on_monday_for_shipment
								,sum(st.stock_after_shipment_kg	) as stock_on_monday_unclaimed
						from (
								select 
										 st.dt_tm
										,st.year_week_number
										,st.week_day
										,st.row_id
										,st.name_table
										,st.stock_kg
										,st.stock_shipment_kg
										,st.stock_on_date_kg
										,IIF(st.stock_after_shipment_kg = st.stock_on_date_kg, st.stock_after_shipment_kg, null) as stock_after_shipment_kg
										--,max(IIF(st.stock_after_shipment_kg = st.stock_on_date_kg and st.stock_shipment_kg is null, st.stock_after_shipment_kg, null)) over (partition by st.row_id, st.name_table, st.year_week_number) as stock_after_shipment_kg
								from (
										select
												 cl.dt_tm
												,cl.year_week_number
												,cl.week_day
												,st.row_id
												,st.name_table
												,st.stock_kg
												,st.stock_after_shipment_kg
												,l.stock_shipment_kg
												,st.stock_kg - isnull(SUM(l.stock_shipment_kg) over (partition by st.row_id, st.name_table order by st.row_id, st.name_table, cl.dt_tm ROWS UNBOUNDED PRECEDING ), 0) as stock_on_date_kg

										from (
												select st.row_id, st.name_table, st.stock_on_date, st.stock_kg, st.stock_after_shipment_kg
												from project_plan_production_finished_products.data_import.stock as st

												union all
				
												select st.row_id, st.name_table, st.stock_on_date, st.stock_kg, st.stock_after_shipment_kg
												from project_plan_production_finished_products.data_import.transits as st
												) as st 
										join #calendar_stock as cl on st.stock_on_date <= cl.dt_tm
										left join (
													select 
															 l.stock_row_id
															,l.stock_name_table
															,COALESCE(s.shipment_date, c.shipment_date, p.shipment_date) as shipment_date
															,sum(l.stock_shipment_kg) as stock_shipment_kg
													from project_plan_production_finished_products.data_import.stock_log_calculation as l
													left join project_plan_production_finished_products.data_import.shipments_SAP			as s	on l.shipment_row_id = s.row_id   and l.shipment_name_table = s.name_table
													left join project_plan_production_finished_products.data_import.shipments_1C			as c	on l.shipment_row_id = c.row_id   and l.shipment_name_table = c.name_table
													left join project_plan_production_finished_products.data_import.shipments_sales_plan	as p	on l.shipment_row_id = p.row_id   and l.shipment_name_table = p.name_table
													where not l.stock_shipment_kg is null
													group by 
																l.stock_row_id
															,l.stock_name_table
															,COALESCE(s.shipment_date, c.shipment_date, p.shipment_date)
													) as l on st.row_id = l.stock_row_id and st.name_table = l.stock_name_table and cl.dt_tm = l.shipment_date + 1 -- сдвигаем на день, что бы получить остатки на утро
										) as st
								--order by  st.row_id, st.name_table, st.dt_tm
								) as st	
						where st.week_day = 1 and stock_on_date_kg <> 0 		
						group by st.year_week_number;	
			

						IF OBJECT_ID('tempdb..#calendar_stock','U') is not null drop table #calendar_stock; 
			end;

			begin -- НАБИВКИ МАРКИРОВКА

						declare @dt_min_marking as datetime; set @dt_min_marking = (select MIN(marking_on_date) from project_plan_production_finished_products.data_import.marking);
						declare @dt_max_marking as datetime; set @dt_max_marking = (select MAX(shipment_date)	from project_plan_production_finished_products.data_import.shipments_sales_plan);

						IF OBJECT_ID('tempdb..#calendar_marking','U') is not null drop table #calendar_marking; 

						select
							 dt_tm
							,week_day
							,year_week_number
						into #calendar_marking
						from cherkizovo.info.calendar
						where dt_tm between @dt_min_marking and @dt_max_marking;

						
						insert into #analytics_total 
						(
								year_week_number
								,marking_on_monday_total
								,marking_on_monday_for_shipment
								,marking_on_monday_unclaimed
						)
						select
								 st.year_week_number
								,sum(st.marking_on_date_kg) as marking_on_monday_total
								,nullif( isnull(sum(st.marking_on_date_kg),0) - isnull(sum(st.marking_after_shipment_kg), 0) , 0) as marking_on_monday_for_shipment
								,sum(st.marking_after_shipment_kg	) as marking_on_monday_unclaimed
						from (
								select 
										 st.dt_tm
										,st.year_week_number
										,st.week_day
										,st.row_id
										,st.name_table
										,st.marking_kg
										,st.marking_shipment_kg
										,st.marking_on_date_kg
										,IIF(st.marking_after_shipment_kg = st.marking_on_date_kg, st.marking_after_shipment_kg, null) as marking_after_shipment_kg
										--,max(IIF(st.stock_after_shipment_kg = st.stock_on_date_kg and st.stock_shipment_kg is null, st.stock_after_shipment_kg, null)) over (partition by st.row_id, st.name_table, st.year_week_number) as stock_after_shipment_kg
								from (
											select
													 cl.dt_tm
													,cl.year_week_number
													,cl.week_day
													,st.row_id
													,st.name_table
													,st.marking_kg
													,st.marking_after_shipment_kg
													,l.marking_shipment_kg
													,st.marking_kg - isnull(SUM(l.marking_shipment_kg) over (partition by st.row_id order by st.row_id, cl.dt_tm ROWS UNBOUNDED PRECEDING ), 0) as marking_on_date_kg

											from project_plan_production_finished_products.data_import.marking as st
											join #calendar_marking as cl on st.marking_on_date <= cl.dt_tm
											left join (
														select 
																 l.marking_row_id
																,COALESCE(s.shipment_date, c.shipment_date, p.shipment_date) as shipment_date
																,sum(l.marking_shipment_kg) as marking_shipment_kg
														from project_plan_production_finished_products.data_import.marking_log_calculation as l
														left join project_plan_production_finished_products.data_import.shipments_SAP			as s	on l.shipment_row_id = s.row_id   and l.shipment_name_table = s.name_table
														left join project_plan_production_finished_products.data_import.shipments_1C			as c	on l.shipment_row_id = c.row_id   and l.shipment_name_table = c.name_table
														left join project_plan_production_finished_products.data_import.shipments_sales_plan	as p	on l.shipment_row_id = p.row_id   and l.shipment_name_table = p.name_table
														where not l.marking_shipment_kg is null
														group by 
																 l.marking_row_id
																,COALESCE(s.shipment_date, c.shipment_date, p.shipment_date)
														) as l on st.row_id = l.marking_row_id and cl.dt_tm = l.shipment_date + 1 -- сдвигаем на день, что бы получить остатки на утро
											--order by  st.row_id, cl.dt_tm
									 ) as st
								--order by  st.row_id, st.dt_tm
								
								) as st	
						where st.week_day = 1 and marking_on_date_kg <> 0 		
						group by st.year_week_number;	
			
						IF OBJECT_ID('tempdb..#calendar_marking','U') is not null drop table #calendar_marking; 

			end;



			begin -- ВЫГРУЖАЕМ ОТЧЕТ


					select 
							 left(o.year_week_number, 4) + '|' + RIGHT(o.year_week_number, 2) as [Год|№ недели]
																											
							--,'Итого остатков на понедельник'				= sum(o.stock_on_monday_total)			
							--,'Остатки для отгрузки на понедельник'			= sum(o.stock_on_monday_for_shipment)		
							--,'Невостребованный остаток на понедельник'		= sum(o.stock_on_monday_unclaimed)		
							,'Итого остатков на понедельник'				= nullif(   sum(   isnull(o.stock_on_monday_total,0)		+ ISNULL(o.marking_on_monday_total,0)			)   ,0)
							,'Остатки для отгрузки на понедельник'			= nullif(   sum(   isnull(o.stock_on_monday_for_shipment,0) + ISNULL(o.marking_on_monday_for_shipment,0)	)   ,0)
							,'Невостребованный остаток на понедельник'		= nullif(   sum(   isnull(o.stock_on_monday_unclaimed,0)	+ ISNULL(o.marking_on_monday_unclaimed,0)		)   ,0)
								
							,'Приход транзита'								= sum(o.transit_received)	
							,'Отгружено из остатков'						= sum(o.shipment_from_stock)	
		
							,'Итого выход ГП'								= sum(o.marking_total)			
							,'Итого ГП на понедельник'						= sum(o.marking_on_monday_total)			
							,'ГП для отгрузки на понедельник'				= sum(o.marking_on_monday_for_shipment)			
							,'Невостребованный ГП на понедельник'			= sum(o.marking_on_monday_unclaimed)	
							,'Отгружено из набивок'							= sum(o.shipment_from_marking)	
									
							,'Выход (в камерах)'							= sum(o.stuffing_fact_output)																			
							,'Выход (план)'									= sum(o.stuffing_plan_output)								
																																
							,'План отгрузки'								= sum(o.shipment_total)	
							,'Заявки SAP'									= sum(o.shipment_SAP)																			
							,'Заявки 1С'									= sum(o.shipment_1C)					
							,'План продаж'									= sum(o.shipment_sales_plan)
																									
							,'Дефицит'										= sum(o.deficit_total)													
							,'Дефицит SAP'									= sum(o.deficit_SAP)													
							,'Дефицит 1С'									= sum(o.deficit_1C)	
							,'Дефицит План продаж'							= sum(o.deficit_sales_plan)	
							,'Доля дефицита'								= sum(o.deficit_total) / sum(o.shipment_total)								
																 																																		
																																									
					from #analytics_total as o group by o.year_week_number

			end;

			


end;




			