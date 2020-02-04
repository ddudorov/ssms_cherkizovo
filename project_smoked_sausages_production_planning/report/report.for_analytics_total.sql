use project_plan_production_finished_products

-- exec .report.for_analytics_total_day		

go

ALTER procedure report.for_analytics_total @level_dt varchar(10)												
as
BEGIN
			SET NOCOUNT ON;
			
			--declare @level_dt varchar(10); set @level_dt = 'day'; set @level_dt = 'week'; 

			begin -- ТАБЛИЦА ДЛЯ ОТЧЕТА 

						IF OBJECT_ID('tempdb..#analytics_total','U') is not null drop table #analytics_total; 

						create table #analytics_total
						(
								 dt						datetime	not null

								,sap_id					bigint			null	
								,stuffing_id			varchar(40)		null

								,stock_total_kg			dec(15,5)		null	-- итого остатков на понедельник		
								,stock_for_shipment_kg	as stock_total_kg - isnull(stock_unclaimed_kg, 0)
								,stock_unclaimed_kg		dec(15,5)		null	-- остаток невостребованный на понедельник

								,stuffing_fact_kg		dec(11,5)		null	-- выход фактических набивок
								,stuffing_plan_kg		dec(11,5)		null	-- выход плановых набивок
								
								,marking_kg				dec(11,5)		null	-- маркировка

								,shipment_kg			dec(15,5)		null	-- потребность всего	
								,deficit_kg				dec(15,5)		null	-- дефицит		

						);


			end;
			
			begin -- НАБИВКИ ФАКТ | НАБИВКИ ПЛАН | МАРКИРОВКА | ПОТРЕБНОСТЬ 
						

						-- набивки факт
						insert into #analytics_total 
								( dt, stuffing_id, stuffing_fact_kg)

						select f.stuffing_available_date, f.stuffing_id, sum(f.stuffing_kg)
						from data_import.stuffing_fact as f
						where f.stuffing_sap_id is null
						group by f.stuffing_available_date, f.stuffing_id;


						-- набивки план
						insert into #analytics_total 
								( dt, stuffing_id, stuffing_plan_kg)

						select	f.stuffing_available_date, f.stuffing_id, sum(f.stuffing_kg)
						from data_import.stuffing_plan as f
						where f.stuffing_sap_id is null
						group by f.stuffing_available_date, f.stuffing_id;


						-- маркировка
						insert into #analytics_total 
								( dt, sap_id, stuffing_id, marking_kg)

						select	f.marking_on_date, f.marking_sap_id, f.marking_stuffing_id, sum(f.marking_kg)
						from data_import.marking as f
						group by f.marking_on_date, f.marking_sap_id, f.marking_stuffing_id;





						-- потребность отгруженная из остатков, так как потребность может быть отгружена из аналога
						insert into #analytics_total 
								( dt, sap_id, stuffing_id, shipment_kg)

						select ls.shipment_date, st.stock_sap_id, st.stock_stuffing_id, sum(ls.stock_shipment_kg) 
						from data_import.stock_log_calculation as ls
						join data_import.stock as st on ls.stock_row_id = st.stock_row_id
						group by ls.shipment_date, st.stock_sap_id, st.stock_stuffing_id;
						

						-- потребность отгруженная из маркировки, так как потребность может быть отгружена из аналога
						insert into #analytics_total 
								( dt, sap_id, stuffing_id, shipment_kg)

						select ls.shipment_date, st.marking_sap_id, st.marking_stuffing_id, sum(ls.marking_shipment_kg) 
						from data_import.marking_log_calculation as ls
						join data_import.marking as st on ls.marking_row_id = st.marking_row_id
						group by ls.shipment_date, st.marking_sap_id, st.marking_stuffing_id;


						-- остался дефицит только
						insert into #analytics_total 
								( dt, sap_id, stuffing_id, shipment_kg, deficit_kg)
									
						select sh.shipment_date, sh.shipment_sap_id, sh.shipment_stuffing_id, sum(sh.shipment_after_marking_kg), sum(sh.shipment_after_marking_kg)
						from data_import.shipment as sh
						where not sh.shipment_after_marking_kg is null
						  and sh.shipment_delete = 0
						  and sh.shipment_stuffing_id_box_type in (0, 1)
						group by sh.shipment_date, sh.shipment_sap_id, sh.shipment_stuffing_id;

			end;

			begin -- ОСТАТКИ | МАРКИРОВКА 

					IF OBJECT_ID('tempdb..#calendar','U') is not null drop table #calendar; 

					select dt_tm
					into #calendar
					from cherkizovo.info.calendar
					where dt_tm between (select min(shipment_date) from .data_import.shipment)
					                and (select max(shipment_date) from .data_import.shipment);


					begin -- вставляем остатки 

							insert into #analytics_total
							(
								 dt		
								,sap_id
								,stuffing_id	
								,stock_total_kg		
								,stock_unclaimed_kg	
							)
					
							select
									 st.dt_tm
									,st.stock_sap_id
									,st.stock_stuffing_id
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
											  from .data_import.stock_log_calculation as l
											  where st.stock_row_id = l.stock_row_id
												and c.dt_tm > l.shipment_date
												and not l.stock_shipment_kg is null) as stock_log_shipment_kg
									from .data_import.stock as st
									join #calendar as c on st.stock_on_date <= c.dt_tm
									where st.stock_reason_ignore_in_calculate is null
								 ) as st
							where st.stock_kg - isnull(st.stock_log_shipment_kg, 0) <> 0
							group by 
									 st.dt_tm
									,st.stock_sap_id
									,st.stock_stuffing_id;

					end;

					
					begin -- вставляем маркировку 

							insert into #analytics_total
							(
								 dt		
								,sap_id
								,stuffing_id	
								,stock_total_kg		
								,stock_unclaimed_kg		
							)
					
							select
									 st.dt_tm
									,st.marking_sap_id
									,st.marking_stuffing_id
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
											  from .data_import.marking_log_calculation as l
											  where st.marking_row_id = l.marking_row_id
												and c.dt_tm > l.shipment_date
												and not l.marking_shipment_kg is null) as marking_log_shipment_kg
									from .data_import.marking as st
									join #calendar as c on st.marking_on_date <= c.dt_tm
									where st.marking_reason_ignore_in_calculate is null
								 ) as st
							where st.marking_kg - isnull(st.marking_log_shipment_kg, 0) <> 0
							group by st.dt_tm
									,st.marking_sap_id
									,st.marking_stuffing_id;

					end;

			end;


			begin -- ВЫГРУЖАЕМ ОТЧЕТ
					
					if @level_dt = 'day'
					begin

							select 

									 'Данные на'										= o.dt

									,'Наименование SKU'									= ps.product_1C_full_name
									,'Код набивки'										= o.stuffing_id			
									,'Название набивки'									= sf.stuffing_name	

									,'Подходящий остаток для отгрузки'					= sum(o.stock_for_shipment_kg)			
									,'Невостребованный остаток (накопительно)'			= sum(o.stock_unclaimed_kg)
									,'Итого остаток'									= sum(o.stock_total_kg)			
							
									,'Выход (в камерах)'								= sum(o.stuffing_fact_kg)																			
									,'Выход (план)'										= sum(o.stuffing_plan_kg)

									,'Итого приход маркированной ГП'					= sum(o.marking_kg)									
																																	
									,'План отгрузок'									= sum(o.shipment_kg)	
																										
									,'Дефицит'											= - sum(o.deficit_kg)			
									,'В том числе дефицит на заблокированных артикулах' = - sum(iif( isnull(ps.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший'), o.deficit_kg, null))		
																																									
							from #analytics_total as o 
							left join .info_view.sap_id as ps on o.sap_id = ps.sap_id_for_join
							left join .info.stuffing as sf on o.stuffing_id = sf.stuffing_id
							group by o.dt, ps.product_1C_full_name, o.stuffing_id, sf.stuffing_name;

					end
					else
					begin

							select 
									 'Данные на'										= left(c.year_week_number, 4) + '|' + RIGHT(c.year_week_number, 2)

									,'Наименование SKU'									= ps.product_1C_full_name
									,'Код набивки'										= o.stuffing_id			
									,'Название набивки'									= sf.stuffing_name	

									,'Подходящий остаток для отгрузки с понедельника'	= sum(iif(c.week_day = 1, o.stock_for_shipment_kg	, null))			
									,'Невостребованный остаток (накопительно)'			= sum(iif(c.week_day = 1, o.stock_unclaimed_kg		, null))
									,'Итого остаток'									= sum(iif(c.week_day = 1, o.stock_total_kg			, null))		
							
									,'Выход (в камерах)'								= sum(o.stuffing_fact_kg)																			
									,'Выход (план)'										= sum(o.stuffing_plan_kg)

									,'Итого приход маркированной ГП'					= sum(o.marking_kg)									
																																	
									,'План отгрузок'									= sum(o.shipment_kg)	
																										
									,'Дефицит'											= - sum(o.deficit_kg)		
									,'В том числе дефицит на заблокированных артикулах' = - sum(iif( isnull(ps.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший'), o.deficit_kg, null))		
																																									
							from #analytics_total as o 
							join cherkizovo.info.calendar as c on o.dt = c.dt
							left join .info_view.sap_id as ps on o.sap_id = ps.sap_id_for_join
							left join info.stuffing as sf on o.stuffing_id = sf.stuffing_id
							group by left(c.year_week_number, 4) + '|' + RIGHT(c.year_week_number, 2), ps.product_1C_full_name, o.stuffing_id, sf.stuffing_name;

					end;

					

			end;

end;




			