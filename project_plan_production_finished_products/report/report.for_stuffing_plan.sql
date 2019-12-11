use project_plan_production_finished_products 

go

-- exec project_plan_production_finished_products.report.for_stuffing_plan @ProductionDateFrom_Kashira = '20191127',  @ProductionDateFrom_CHMPZ = '20191125' 

ALTER PROCEDURE report.for_stuffing_plan  @ProductionDateFrom_Kashira datetime
										 ,@ProductionDateFrom_CHMPZ datetime										 
										 ,@type_report varchar(50) = 'report_main'
as
BEGIN
			SET NOCOUNT ON;
			
			-- для теста
			--declare @ProductionDateFrom_Kashira datetime;	set @ProductionDateFrom_Kashira = '20191127'
			--declare @ProductionDateFrom_CHMPZ datetime;		set @ProductionDateFrom_CHMPZ = '20191125'
			--declare @type_report varchar(50);				set @type_report = 'report_for_pivot' --report_for_pivot   report_main
			
			declare @report_dt_from datetime;	set @report_dt_from =	(select min(stuffing_production_date_from)	from project_plan_production_finished_products.data_import.stuffing_fact);	
			declare @report_dt_to datetime;		set @report_dt_to =		(select max(shipment_date)					from project_plan_production_finished_products.data_import.shipment);

			declare @while_dt_Kashira datetime;
			declare @while_dt_CHMPZ datetime;

			declare @sql varchar(max);
			
			declare @sql_iif			varchar(500);
			declare @sql_name_column	varchar(500);

			--------------------
			-- ПОДГОТОВКА ДАННЫХ
			--------------------

			-- НОРМАТИВ ОСТАТКОВ --- ПЕРВЫЕ 45 ДНЕЙ 
			begin

						IF OBJECT_ID('tempdb..#normative_stock','U') is not null drop table #normative_stock;

						select
								 p.shipment_stuffing_id
								,p.shipment_sap_id
								,sum(p.normative_stock_kg) as normative_stock_kg
						into #normative_stock
						from (
								select 
										 p.shipment_sap_id
										,p.shipment_stuffing_id
										,'normative_stock_kg' = iif(p.shipment_date <= min(p.shipment_date) over (partition by p.shipment_sap_id, p.shipment_stuffing_id) + 45, p.shipment_kg, null) / 45 * s.number_days_normative_stock
								from project_plan_production_finished_products.data_import.shipment as p
								join project_plan_production_finished_products.info.finished_products_sap_id_manual as s 
									on p.shipment_sap_id = s.SAP_id 
									and not s.number_days_normative_stock is null
									and p.shipment_data_type = 'shipment_sales_plan'
								where p.shipment_stuffing_id_box_type in (0,2)
							 ) as p
						where not p.normative_stock_kg is null
						group by p.shipment_stuffing_id
								,p.shipment_sap_id;

			end;

			
			-- НАБИВКИ ФАКТ + НАБИВКИ ФАКТ ОТГРУЗКА + ЧИСТАЯ ПОТРЕБНОСТЬ + ПЛАН НАБИВОК
			begin
						-- создаем таблицу с данными
						begin

									IF OBJECT_ID('tempdb..#data','U') is not null drop table #data;

									select 
												 st.stuffing_production_date_from
												,st.stuffing_production_name
												,st.stuffing_id
												,st.stuffing_sap_id
												,sum(st.stuffing_surplus_kg)				as stuffing_surplus_kg
												,sum(st.stuffing_marking_kg)				as stuffing_marking_kg
												,sum(st.shipment_after_stuffing_fact_kg)	as shipment_after_stuffing_fact_kg
												,sum(st.stuffing_count_planned)				as stuffing_count_planned
									into #data
									from (
												-- ЭТО ИСХОДНЫЕ ДАННЫЕ НАБИВОК / ВЫХОД НАБИВКИ
												select 
														 st.stuffing_production_date_from
														,st.stuffing_production_name
														,st.stuffing_id
														,st.stuffing_sap_id
														,st.stuffing_surplus_kg
														,iif(not st.stuffing_sap_id is null, isnull(st.stuffing_marking_kg ,0) + ISNULL(st.stuffing_shipment_kg, 0), null) as stuffing_marking_kg
														,null as shipment_after_stuffing_fact_kg
														,null as stuffing_count_planned
												from project_plan_production_finished_products.data_import.stuffing_fact as st
									
												union all
									
												-- ЭТО ОТГРУЗКА НАБИВОК
												select   
														 sp.shipment_date - DATEDIFF(day, st.stuffing_production_date_from, st.stuffing_available_date) as stuffing_production_date_from
														,st.stuffing_production_name
														,st.stuffing_id
														,l.stuffing_sap_id
														,null as stuffing_surplus_kg
														,- l.stuffing_shipment_kg as stuffing_shipment_kg
														,null as shipment_after_stuffing_fact_kg
														,null as stuffing_count_planned
												from project_plan_production_finished_products.data_import.stuffing_fact_log_calculation	as l
												join project_plan_production_finished_products.data_import.stuffing_fact					as st on l.stuffing_row_id = st.stuffing_row_id
												left join project_plan_production_finished_products.data_import.shipment					as sp on l.shipment_row_id = sp.shipment_row_id 
												where not l.stuffing_shipment_kg is null

												union all

												-- ЧИСТАЯ ПОТРЕБНОСТЬ ---------------------------
												select 
														 s.shipment_date - st.transit_from_production_days - st.maturation_and_packaging_days as stuffing_production_date_from  -- 'это день закладки
														,st.production_name as stuffing_production_name
														,s.shipment_stuffing_id 
														,s.shipment_sap_id as stuffing_sap_id	
														,null as stuffing_surplus_kg
														,null as stuffing_shipment_kg
														,sum(s.shipment_after_stuffing_fact_kg) as shipment_after_stuffing_fact_kg
														,null as stuffing_count_planned
												from project_plan_production_finished_products.data_import.shipment as s	
												join project_plan_production_finished_products.info.stuffing as st on s.shipment_stuffing_id = st.stuffing_id			   
												WHERE s.shipment_delete = 0
													and s.shipment_stuffing_id_box_type in (0, 2) -- берем не коробки
													and s.shipment_after_stuffing_fact_kg > 0
													and s.shipment_exclude_for_stuffing_plan = 0
													and not st.transit_from_production_days is null	
													and not st.maturation_and_packaging_days is null
												group by 
														 s.shipment_date - st.transit_from_production_days - st.maturation_and_packaging_days
														,st.production_name
														,s.shipment_stuffing_id
														,s.shipment_sap_id
									
												union all

												-- ПЛАН НАБИВОК ---------------------------
												select 
														 st.stuffing_production_date_from	
														,st.stuffing_production_name	
														,st.stuffing_id		
														,null as stuffing_sap_id
														,null as stuffing_surplus_kg
														,null as stuffing_shipment_kg
														,null as shipment_after_stuffing_fact_kg
														,st.stuffing_count_planned
												from project_plan_production_finished_products.data_import.stuffing_plan as st												
												
										 ) as st
									group by 
												 st.stuffing_production_date_from
												,st.stuffing_production_name
												,st.stuffing_id
												,st.stuffing_sap_id--;
									order by 3,1,4
						end;

						-- переворачиваем таблицу
						begin
						
									-- СОЗДАЕМ ТАБЛИЦУ ДЛЯ НАБИВОК
									IF OBJECT_ID('tempdb..#data_pivot','U') is not null drop table #data_pivot;
									
									create table #data_pivot
									(		
											  stuffing_id		VARCHAR(40)	NOT NULL
											 ,stuffing_sap_id	BIGINT			NULL
									);


									-- СОЗДАЕМ СТОЛБЦЫ
									set @while_dt_Kashira = @ProductionDateFrom_Kashira
									set @while_dt_CHMPZ = @ProductionDateFrom_CHMPZ

									while @while_dt_CHMPZ <= @report_dt_to
									begin
											set @sql_name_column = format(@while_dt_Kashira, 'yyyyMMdd')  + '_' + format(@while_dt_CHMPZ, 'yyyyMMdd')

											set @sql = 'alter table #data_pivot add stuffing_surplus_kg_'				+ @sql_name_column + ' dec(11, 5) null;'; exec (@sql);
											set @sql = 'alter table #data_pivot add stuffing_marking_kg_'				+ @sql_name_column + ' dec(11, 5) null;'; exec (@sql);
											set @sql = 'alter table #data_pivot add shipment_after_stuffing_fact_kg_'	+ @sql_name_column + ' dec(11, 5) null;'; exec (@sql);
											set @sql = 'alter table #data_pivot add stuffing_count_planned_'			+ @sql_name_column + ' dec(11, 5) null;'; exec (@sql);
											
											set @while_dt_Kashira = @while_dt_Kashira + 1;
											set @while_dt_CHMPZ = @while_dt_CHMPZ + 1; 
									end;


									-- НАПОЛНЯЕМ ДАННЫМИ
											set @sql = ''
											set @sql = @sql + char(10) + 'insert into #data_pivot
																		  select d.stuffing_id, d.stuffing_sap_id'
									set @while_dt_Kashira = @ProductionDateFrom_Kashira
									set @while_dt_CHMPZ = @ProductionDateFrom_CHMPZ

									while @while_dt_CHMPZ <= @report_dt_to
									begin

											set @sql = @sql + char(10) + '		,sum(case 
																						when d.stuffing_production_date_from <= ''' + format(@while_dt_Kashira, 'yyyyMMdd') + ''' and	  d.stuffing_production_name in (''Кашира'') then d.stuffing_surplus_kg
																						when d.stuffing_production_date_from <= ''' + format(@while_dt_CHMPZ  , 'yyyyMMdd') + ''' and not d.stuffing_production_name in (''Кашира'') then d.stuffing_surplus_kg
																					 end) as stuffing_surplus_kg'
																					 
									
											set @sql = @sql + char(10) + '		,sum(case 
																						when d.stuffing_production_date_from <= ''' + format(@while_dt_Kashira, 'yyyyMMdd') + ''' and	  d.stuffing_production_name in (''Кашира'') then d.stuffing_marking_kg
																						when d.stuffing_production_date_from <= ''' + format(@while_dt_CHMPZ  , 'yyyyMMdd') + ''' and not d.stuffing_production_name in (''Кашира'') then d.stuffing_marking_kg
																					 end) as stuffing_marking_kg'
																					 
											set @sql = @sql + char(10) + '		,sum(case 
																						when d.stuffing_production_date_from =  ''' + format(@while_dt_Kashira, 'yyyyMMdd') + ''' and	  d.stuffing_production_name in (''Кашира'') then d.shipment_after_stuffing_fact_kg
																						when d.stuffing_production_date_from =  ''' + format(@while_dt_CHMPZ  , 'yyyyMMdd') + ''' and not d.stuffing_production_name in (''Кашира'') then d.shipment_after_stuffing_fact_kg
																					 end) as shipment_after_stuffing_fact_kg'
																					 
											set @sql = @sql + char(10) + '		,sum(case 
																						when d.stuffing_production_date_from =  ''' + format(@while_dt_Kashira, 'yyyyMMdd') + ''' and	  d.stuffing_production_name in (''Кашира'') then d.stuffing_count_planned
																						when d.stuffing_production_date_from =  ''' + format(@while_dt_CHMPZ  , 'yyyyMMdd') + ''' and not d.stuffing_production_name in (''Кашира'') then d.stuffing_count_planned
																					 end) as stuffing_count_planned'
						
											set @while_dt_Kashira = @while_dt_Kashira + 1;
											set @while_dt_CHMPZ = @while_dt_CHMPZ + 1; 
									end;

											set @sql = @sql + char(10) + 'from #data as d 
																		  group by d.stuffing_id, d.stuffing_sap_id'
										
											--print @sql;
											exec (@sql);
											
									IF OBJECT_ID('tempdb..#data','U') is not null drop table #data;
									

						end;

			end;		
				
										
			-- СТОЛБЦЫ ДЛЯ ОТЧЕТА
			begin

						IF OBJECT_ID('tempdb..#columns','U') is not null drop table #columns;				

						select 
								 t.production_name as stuffing_production_name
								,t.stuffing_name
								,c.stuffing_id
								,t.stuffing_id_box
								,t.mml
								,t.stuffing_type
								,t.stuffing_group	

								,t.maturation_days
								,t.maturation_and_packaging_days
								,t.transit_from_production_days

								,t.count_chamber
								,t.minimum_preparation_materials_kg
								,t.minimum_volume_for_chamber_kg
								
								,c.stuffing_sap_id as sap_id
								,convert(varchar(24), FORMAT(c.stuffing_sap_id, '000000000000000000000000')) as  sap_id_text
								,p.product_1C_full_name
								,p.product_SAP_full_name
								,p.production_name as product_production_name
								,s.normative_stock_kg

						into #columns
						from (
								select distinct stuffing_id, stuffing_sap_id from #data_pivot
								union 
								select distinct stuffing_id, null			 from #data_pivot
						
						) as c
						join project_plan_production_finished_products.info.stuffing as t on c.stuffing_id = t.stuffing_id
						left join cherkizovo.info.products_sap as p on c.stuffing_sap_id = p.SAP_id
						left join #normative_stock as s on c.stuffing_sap_id = s.shipment_sap_id and c.stuffing_id = s.shipment_stuffing_id;

						
			end;
			

			-- ОСНОВНОЙ ОТЧЕТ
			if @type_report = 'report_main'
			begin

						-- ОТЧЕТ
								set @sql = ''
								set @sql = @sql + char(10) + 'select '
								set @sql = @sql + char(10) + '			 ROW_NUMBER() over(order by c.stuffing_production_name
																					   ,GROUPING_ID(c.stuffing_type) desc, c.stuffing_type
																					   ,GROUPING_ID(c.stuffing_id) desc, c.stuffing_id
																					   ,GROUPING_ID(c.sap_id_text) desc, c.sap_id_text) as row_id'
								set @sql = @sql + char(10) + '			,case		
																			when GROUPING_ID(c.stuffing_type) = 1	then 1
																			when GROUPING_ID(c.stuffing_id) = 1		then 2
																			when GROUPING_ID(c.sap_id_text) = 1		then 3
																			when GROUPING_ID(c.sap_id_text) = 0		then 4
																		end as frm_id'
								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_type) = 1	then isnull(c.stuffing_production_name, ''Завод не указан'')
																				when GROUPING_ID(c.stuffing_id) = 1		then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'')
																				when GROUPING_ID(c.sap_id_text) = 1		then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|'' + isnull(c.stuffing_id , ''Код набивки не указан'')
																				when GROUPING_ID(c.sap_id_text) = 0		then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|'' + isnull(c.stuffing_id , ''Код набивки не указан'') + ''|'' + c.sap_id_text
																		 end as data_hierarchy'

								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_type) = 1	then isnull(c.stuffing_production_name, ''Завод не указан'')
																				when GROUPING_ID(c.stuffing_id) = 1		then isnull(c.stuffing_type , ''Тип набивки не указан'')
																				when GROUPING_ID(c.sap_id_text) = 1		then isnull(c.stuffing_id , ''Код набивки не указан'')
																				when GROUPING_ID(c.sap_id_text) = 0		then c.sap_id_text
																		 end as data_id'		
								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_type) = 1	then null
																				when GROUPING_ID(c.stuffing_id) = 1		then null
																				when GROUPING_ID(c.sap_id_text) = 1		then (select top 1 s.stuffing_name from #columns as s where c.stuffing_id = s.stuffing_id)
																				when GROUPING_ID(c.sap_id_text) = 0		then (select top 1 s.product_1C_full_name from #columns as s where c.sap_id_text = s.sap_id_text)
																		 end as data_name'

								set @sql = @sql + char(10) + '		
																	,(select top 1 s.product_production_name			from #columns as s where c.sap_id_text = s.sap_id_text) as product_production_name
																	,(select top 1 s.mml								from #columns as s where c.stuffing_id = s.stuffing_id) as mml

																	,(select top 1 s.stuffing_group						from #columns as s where c.stuffing_id = s.stuffing_id) as stuffing_group
																	,(select top 1 s.count_chamber						from #columns as s where c.stuffing_id = s.stuffing_id) as count_chamber

																	,(select top 1 s.maturation_days					from #columns as s where c.stuffing_id = s.stuffing_id) as maturation_days
																	,(select top 1 s.maturation_and_packaging_days		from #columns as s where c.stuffing_id = s.stuffing_id) as maturation_and_packaging_days
																	,(select top 1 s.transit_from_production_days		from #columns as s where c.stuffing_id = s.stuffing_id) as transit_from_production_days
																	,(select top 1 s.minimum_preparation_materials_kg	from #columns as s where c.stuffing_id = s.stuffing_id) as minimum_preparation_materials_kg
																	,(select top 1 s.minimum_volume_for_chamber_kg		from #columns as s where c.stuffing_id = s.stuffing_id) as minimum_volume_for_chamber_kg
																	,sum(c.normative_stock_kg)																					as normative_stock_kg

															'	

						-- СТОЛБЦЫ с данными
						set @while_dt_Kashira = @ProductionDateFrom_Kashira
						set @while_dt_CHMPZ = @ProductionDateFrom_CHMPZ
						--set  @report_dt_to =  @while_dt_CHMPZ +1  -- для текста
						while @while_dt_CHMPZ <= @report_dt_to
						begin

								set @sql_iif = 'convert(datetime, iif(c.stuffing_production_name in (''Кашира''), ''' + format(@while_dt_Kashira, 'yyyyMMdd') + ''',''' + format(@while_dt_CHMPZ, 'yyyyMMdd')  + '''))'
								set @sql_name_column = format(@while_dt_Kashira, 'yyyyMMdd')  + '_' + format(@while_dt_CHMPZ, 'yyyyMMdd')
								
								set @sql = @sql + char(10) + '		,' + @sql_iif + ' + max(c.maturation_and_packaging_days) + max(c.transit_from_production_days) + iif(not c.stuffing_id is null, 0, null) as stuffing_available_date_'	+ @sql_name_column -- дата доступности набивки
								set @sql = @sql + char(10) + '		,sum( d.stuffing_surplus_kg_'				+ @sql_name_column + ') as stuffing_surplus_kg_'				+ @sql_name_column -- остаток нераспределенной набивки	
								set @sql = @sql + char(10) + '		,sum( d.stuffing_marking_kg_'				+ @sql_name_column + ') as stuffing_marking_kg_'				+ @sql_name_column -- остаток маркировки
								set @sql = @sql + char(10) + '		,sum( d.shipment_after_stuffing_fact_kg_'	+ @sql_name_column + ') as shipment_after_stuffing_fact_kg_'	+ @sql_name_column -- потребность после (чистая потребность)						
								set @sql = @sql + char(10) + '		,sum( d.stuffing_count_planned_'			+ @sql_name_column + ') as stuffing_count_planned_'				+ @sql_name_column -- Закладка (замес)	
								set @sql = @sql + char(10) + '		,convert(dec(15,5), null)											as formula_preparation_kg_'			+ @sql_name_column --  Закладка (кг ГП)
								set @sql = @sql + char(10) + '		,convert(dec(15,5), null)											as formula_preparation_stock_kg_'	+ @sql_name_column -- Остаток после закладки
	
								set @while_dt_Kashira = @while_dt_Kashira + 1;
								set @while_dt_CHMPZ = @while_dt_CHMPZ + 1; 
						end;

								set @sql = @sql + char(10) + 'from #columns as c'
								set @sql = @sql + char(10) + 'left join #data_pivot as d on c.stuffing_id = d.stuffing_id and isnull(c.sap_id, 0) = isnull(d.stuffing_sap_id, 0)' 
								set @sql = @sql + char(10) + 'group by rollup(   c.stuffing_production_name
																				,c.stuffing_type
																				,c.stuffing_id	
																				,c.sap_id_text)'
								set @sql = @sql + char(10) + 'having not (GROUPING_ID(c.stuffing_production_name) = 1)
																 and not (GROUPING_ID(c.sap_id_text) = 0 and c.sap_id_text is null)'

								--print @sql;									
								exec( @sql);

			end;



			-- ДОПОЛНИТЕЛЬНЫЙ ОТЧЕТ
			if @type_report = 'report_for_pivot'
			begin
					select 
						 'Код набивки'				= cl.stuffing_id
						,'MML набивки'				= cl.mml
						,'Название набивки'			= cl.stuffing_name
						,'Площадка'					= cl.stuffing_production_name
						,'Тип набивки'				= cl.stuffing_type
						,'Группа набивки'			= cl.stuffing_group
						,'Цикл'						= cl.maturation_days
						,'Цикл + упаковка'			= cl.maturation_and_packaging_days
						,'Транзит с Площадка'		= cl.transit_from_production_days
						,'Ограничения по камерам'	= cl.count_chamber
						,'Мин замес набивки кг'		= cl.minimum_preparation_materials_kg
						,'Мин объем камеры кг'		= cl.minimum_volume_for_chamber_kg
						,'Дата закладки'			= case when cl.stuffing_production_name = 'Кашира' then c.dt_tm else c.dt_tm - datediff(day, @ProductionDateFrom_CHMPZ, @ProductionDateFrom_Kashira) end					
						,'Дата выхода'				= case when cl.stuffing_production_name = 'Кашира' then c.dt_tm else c.dt_tm - datediff(day, @ProductionDateFrom_CHMPZ, @ProductionDateFrom_Kashira) end + cl.maturation_days
						,'Дата доступности'			= case when cl.stuffing_production_name = 'Кашира' then c.dt_tm else c.dt_tm - datediff(day, @ProductionDateFrom_CHMPZ, @ProductionDateFrom_Kashira) end + cl.maturation_and_packaging_days + cl.transit_from_production_days
												
						,c.dt_tm as for_formula
						,'Закладка (замес)' = convert(dec(15,5), null)
						,'Итог'				= convert(dec(15,5), null)
					from cherkizovo.info.calendar as c
					cross join #columns as cl
					where c.dt_tm between @ProductionDateFrom_Kashira and @report_dt_to
					  and cl.sap_id is null;

			end;


			IF OBJECT_ID('tempdb..#normative_stock','U') is not null	drop table #normative_stock;
			IF OBJECT_ID('tempdb..#stuffing_shipment','U') is not null	drop table #stuffing_shipment;
			IF OBJECT_ID('tempdb..#shipments','U') is not null			drop table #shipments;
			IF OBJECT_ID('tempdb..#columns','U') is not null			drop table #columns;	
			IF OBJECT_ID('tempdb..#data','U') is not null				drop table #data;	

end;

























