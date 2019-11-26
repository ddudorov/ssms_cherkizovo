use project_plan_production_finished_products 

go

-- exec project_plan_production_finished_products.report.for_marking_plan @type_report = 'report_main'

ALTER PROCEDURE report.for_marking_plan @type_report varchar(50) = 'report_main'
as
BEGIN
			SET NOCOUNT ON;
			
			-- для теста
			--declare @type_report varchar(50); set @type_report = 'report_main'
			
			declare @report_dt_from datetime;	set @report_dt_from =	(select top 1 dt 
																		 from (	select  isnull(min(stuffing_production_date_to),'29990101') as dt  from project_plan_production_finished_products.data_import.stuffing_fact union all
																				select  isnull(min(stuffing_production_date_to),'29990101') as dt  from project_plan_production_finished_products.data_import.stuffing_plan ) as s 
																		 order by dt);																 
			declare @report_dt_to datetime;		set @report_dt_to =		(select			max(shipment_date)						from project_plan_production_finished_products.data_import.shipments_sales_plan);																 
			declare @dt_while as datetime;

			declare @sql varchar(max);
			
			declare @sql_iif			varchar(500);
			declare @sql_name_column	varchar(500);
			--------------------
			-- ПОДГОТОВКА ДАННЫХ
			--------------------

			-- НАБИВКИ
			begin
					IF OBJECT_ID('tempdb..#stuffing','U') is not null drop table #stuffing;
					select 
							 s.sap_id
							,s.stuffing_id
							--,s.stuffing_production_name
							,s.stuffing_production_date_to
							--,s.stuffing_available_date
							,sum(s.stuffing_surplus_kg) as stuffing_surplus_kg
							,sum(s.stuffing_marking_kg) as stuffing_marking_kg
					into #stuffing
					from (
							select 
									 s.sap_id
									,s.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_to
									,s.stuffing_available_date
									,s.stuffing_surplus_kg
									,case when not s.sap_id is null then   nullif( isnull( s.stuffing_marking_kg, 0) + isnull( s.stuffing_shipment_kg, 0) , 0)   end as stuffing_marking_kg
							from project_plan_production_finished_products.data_import.stuffing_fact as s
							union all
							select 
									 s.sap_id
									,s.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_to
									,s.stuffing_available_date
									,s.stuffing_surplus_kg
									,case when not s.sap_id is null then   nullif( isnull( s.stuffing_marking_kg, 0) + isnull( s.stuffing_shipment_kg, 0) , 0)   end as stuffing_marking_kg
							from project_plan_production_finished_products.data_import.stuffing_plan as s
						 ) as s
					group by 
							 s.sap_id
							,s.stuffing_id
							--,s.stuffing_production_name
							,s.stuffing_production_date_to;
							--,s.stuffing_available_date

			end;

			-- ПОТРЕБНОСТЬ К ОТГРУЗКЕ
			begin

						IF OBJECT_ID('tempdb..#shipments','U') is not null drop table #shipments;

						select
								 p.sap_id
								,p.sap_id_expiration_date_in_days
								,p.stuffing_id
								,p.shipment_min_KOS
								,p.shipment_date - p.sap_id_expiration_date_in_days * p.shipment_min_KOS as stuffing_production_date_to_min
								,p.shipment_date - st.transit_from_production_days - st.maturation_and_packaging_days + st.maturation_days as stuffing_production_date_to_max
								,p.shipment_date as stuffing_available_date
								,sum(p.shipment_kg) as shipment_kg
								,sum(p.stock_net_need_kg) as stock_net_need_kg
						into #shipments
						from (
								select 
										 p.sap_id
										,p.sap_id_expiration_date_in_days
										,p.stuffing_id
										,p.shipment_min_KOS
										,p.shipment_date
										,p.shipment_kg
										,p.stock_net_need_kg
								from project_plan_production_finished_products.data_import.shipments_SAP as p 
								where p.stuffing_id_box_type in (0, 1)
								 --and p.stock_net_need_kg > 0 
								 and p.shipment_delete = 0		
								 and not p.sap_id is null 
								 and not p.stuffing_id is null 
								 and not p.sap_id_expiration_date_in_days is null						 
								 and not isnull(p.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')

								union all

								select 
										 p.sap_id
										,p.sap_id_expiration_date_in_days
										,p.stuffing_id
										,p.shipment_min_KOS
										,p.shipment_date
										,p.shipment_kg
										,p.stock_net_need_kg
								from project_plan_production_finished_products.data_import.shipments_1C as p
								where p.stuffing_id_box_type in (0, 1)
								 --and p.stock_net_need_kg > 0 
								 and not p.sap_id is null 
								 and not p.stuffing_id is null 
								 and not p.sap_id_expiration_date_in_days is null						 
								 and not isnull(p.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')

								union all

								select 
										 p.sap_id
										,p.sap_id_expiration_date_in_days
										,p.stuffing_id
										,p.shipment_min_KOS
										,p.shipment_date
										,p.shipment_kg
										,p.stock_net_need_kg
								from project_plan_production_finished_products.data_import.shipments_sales_plan as p
								where p.stuffing_id_box_type in (0, 1)
								 --and p.stock_net_need_kg > 0 
								 and not p.sap_id is null 
								 and not p.stuffing_id is null 
								 and not p.sap_id_expiration_date_in_days is null						 
								 and not isnull(p.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')

							) as p join project_plan_production_finished_products.info.stuffing as st on p.stuffing_id = st.stuffing_id
						where not st.transit_from_production_days is null and not st.maturation_and_packaging_days is null and not st.maturation_days is null
						group by  
								 p.sap_id
								,p.sap_id_expiration_date_in_days
								,p.stuffing_id
								,p.shipment_min_KOS
								,p.shipment_date - st.transit_from_production_days - st.maturation_and_packaging_days + st.maturation_days
								,p.shipment_date;


			end;
		
			-- СТОЛБЦЫ ДЛЯ ОТЧЕТА
			begin

						IF OBJECT_ID('tempdb..#columns','U') is not null drop table #columns;				

						with clm as 
						(
								select distinct stuffing_id, sap_id from #stuffing
								union 
								select distinct stuffing_id, sap_id from #shipments
								union 
								select 
										 sm2.stuffing_id
										,s2.sap_id 
								from cherkizovo.info.products_sap													as s1
								join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm1 on s1.sap_id = sm1.sap_id
								join cherkizovo.info.products_sap													as s2  on isnull(sm1.SAP_id_correct_manual, sm1.SAP_id) = s2.sap_id 
								join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm2 on s2.sap_id = sm2.sap_id
								where not isnull(s2.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
								  and ISNUMERIC(LEFT(sm2.stuffing_id, 5)) = 1
								 
						)

						select 
								 c.stuffing_id
								,t.mml
								,t.stuffing_name
								,t.production_name as stuffing_production_name
								,t.stuffing_type
								,t.stuffing_group	

								,t.maturation_days
								,t.maturation_and_packaging_days
								,t.transit_from_production_days

								,t.count_chamber
								,t.minimum_preparation_materials_kg
								,t.minimum_volume_for_chamber_kg
								
								,t.minimum_volume_for_marking_kg
								,t.step_marking_kg
								,t.marking_line_productivity_kg
								,t.marking_line_type

								,c.sap_id
								,p.position_dependent_id
								,p.product_1C_full_name
								,p.product_SAP_full_name
								,p.production_name as product_production_name
								,p.expiration_date_in_days

						into #columns
						from (
								select stuffing_id, sap_id	from clm						
								union
								select stuffing_id, null	from clm
							 ) as c
						left join project_plan_production_finished_products.info.stuffing as t on c.stuffing_id = t.stuffing_id
						left join cherkizovo.info.products_sap as p on c.sap_id = p.SAP_id
			end;
			
			-- ОСНОВНОЙ ОТЧЕТ
			if @type_report = 'report_main'
			begin

						begin -- создаем временную таблицу и добавляем туда данные

									-- СОЗДАЕМ ТАБЛИЦУ ДЛЯ НАБИВОК
									IF OBJECT_ID('tempdb..#stuffing_pivot','U') is not null drop table #stuffing_pivot;

									create table #stuffing_pivot
									(	
											 stuffing_id	VARCHAR(40)	NOT NULL		
											,sap_id			BIGINT			NULL
									);

									-- СОЗДАЕМ СТОЛБЦЫ
									set @dt_while = @report_dt_from;
									while @dt_while <= @report_dt_to
									begin

											set @sql = 'alter table #stuffing_pivot add stuffing_surplus_kg_'	+ format(@dt_while, 'yyyyMMdd') + ' dec(11, 5) null;'
											exec (@sql);

											set @sql = 'alter table #stuffing_pivot add stuffing_marking_kg_'	+ format(@dt_while, 'yyyyMMdd') + ' dec(11, 5) null;'
											exec (@sql);

											set @dt_while = @dt_while + 1;
									end;
									
									-- НАПОЛНЯЕМ ДАННЫМИ
											set @sql = ''
											set @sql = @sql + char(10) + 'insert into #stuffing_pivot
																		  select st.stuffing_id, st.sap_id'
									set @dt_while = @report_dt_from;
									while @dt_while <= @report_dt_to
									begin
											set @sql = @sql + char(10) + '		,sum(iif(st.stuffing_production_date_to = ''' + format(@dt_while, 'yyyyMMdd') + ''', st.stuffing_surplus_kg, null) ) as stuffing_surplus_kg_' +  format(@dt_while, 'yyyyMMdd')
											set @sql = @sql + char(10) + '		,sum(iif(st.stuffing_production_date_to = ''' + format(@dt_while, 'yyyyMMdd') + ''', st.stuffing_marking_kg, null) ) as stuffing_marking_kg_' +  format(@dt_while, 'yyyyMMdd')

											set @dt_while = @dt_while + 1;
									end;
											set @sql = @sql + char(10) + 'from #stuffing as st 
																		  group by st.stuffing_id, st.sap_id'
											exec (@sql);
											
									IF OBJECT_ID('tempdb..#stuffing','U') is not null drop table #stuffing;
									 -- select * from #stuffing_pivot;
						end;


						begin -- создаем временную таблицу и добавляем туда данные

									-- СОЗДАЕМ ТАБЛИЦУ ДЛЯ НАБИВОК
									IF OBJECT_ID('tempdb..#shipments_pivot','U') is not null drop table #shipments_pivot;
									create table #shipments_pivot
									(	
											 stuffing_id	VARCHAR(40)	NOT NULL		
											,sap_id			BIGINT		NOT NULL

									);

									-- СОЗДАЕМ СТОЛБЦЫ
									set @dt_while = @report_dt_from;
									while @dt_while <= @report_dt_to
									begin

											set @sql = 'alter table #shipments_pivot add shipment_kg_'			+ format(@dt_while, 'yyyyMMdd') + ' dec(11, 5) null;';	exec (@sql);
											set @sql = 'alter table #shipments_pivot add stock_net_need_kg_'	+ format(@dt_while, 'yyyyMMdd') + ' dec(11, 5) null;';	exec (@sql);
											set @dt_while = @dt_while + 1;
									end;
									
									-- НАПОЛНЯЕМ ДАННЫМИ
											set @sql = ''
											set @sql = @sql + char(10) + 'insert into #shipments_pivot
																		  select st.stuffing_id, st.sap_id'
									set @dt_while = @report_dt_from;
									while @dt_while <= @report_dt_to
									begin
											set @sql = @sql + char(10) + '		,sum(iif(st.stuffing_production_date_to_max = ''' + format(@dt_while, 'yyyyMMdd') + ''', st.shipment_kg			, null) ) as shipment_kg_' +  format(@dt_while, 'yyyyMMdd')
											set @sql = @sql + char(10) + '		,sum(iif(st.stuffing_production_date_to_max = ''' + format(@dt_while, 'yyyyMMdd') + ''', st.stock_net_need_kg	, null) ) as stock_net_need_kg_' +  format(@dt_while, 'yyyyMMdd')

											set @dt_while = @dt_while + 1;
									end;
											set @sql = @sql + char(10) + 'from #shipments as st 
																		  group by st.stuffing_id, st.sap_id'
											exec (@sql);
											
									IF OBJECT_ID('tempdb..#shipments','U') is not null drop table #shipments;
									--select * from #shipments_pivot;
						end;
									
								IF OBJECT_ID('tempdb..#report_main','U')  is not null drop table #report_main;
						
								set @sql = ''
								set @sql = @sql + char(10) + 'select' 
								set @sql = @sql + char(10) + '			 case		
																				when GROUPING_ID(c.stuffing_production_name) = 1	then 0
																				when GROUPING_ID(c.stuffing_type) = 1				then 1
																				when GROUPING_ID(c.stuffing_id) = 1					then 2
																				when GROUPING_ID(c.sap_id) = 1						then 3
																				when GROUPING_ID(c.sap_id) = 0						then 4
																		 end as frm_id'
								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_production_name) = 1	then null
																				when GROUPING_ID(c.stuffing_type) = 1				then isnull(c.stuffing_production_name, ''Завод не указан'')
																				when GROUPING_ID(c.stuffing_id) = 1					then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'')
																				when GROUPING_ID(c.sap_id) = 1						then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|'' + isnull(c.stuffing_id , ''Код набивки не указан'')
																				when GROUPING_ID(c.sap_id) = 0						then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|'' + isnull(c.stuffing_id , ''Код набивки не указан'') + ''|'' + FORMAT(c.sap_id, ''000000000000000000000000'')
																		 end as data_hierarchy'

								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_production_name) = 1	then ''Общий итог''
																				when GROUPING_ID(c.stuffing_type) = 1				then isnull(c.stuffing_production_name, ''Завод не указан'')
																				when GROUPING_ID(c.stuffing_id) = 1					then isnull(c.stuffing_type , ''Тип набивки не указан'')
																				when GROUPING_ID(c.sap_id) = 1						then isnull(c.stuffing_id , ''Код набивки не указан'')
																				when GROUPING_ID(c.sap_id) = 0						then FORMAT(c.sap_id, ''000000000000000000000000'')
																		 end as data_id'	
																		 	
								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_production_name) = 1	then null
																				when GROUPING_ID(c.stuffing_type) = 1				then null
																				when GROUPING_ID(c.stuffing_id) = 1					then null
																				when GROUPING_ID(c.sap_id) = 1						then (select top 1 s.stuffing_name from #columns as s where c.stuffing_id = s.stuffing_id)
																				when GROUPING_ID(c.sap_id) = 0						then (select top 1 s.product_1C_full_name from #columns as s where c.sap_id = s.sap_id)
																		 end as data_name'

								set @sql = @sql + char(10) + '			,(select top 1 s.mml										from #columns as s where c.stuffing_id = s.stuffing_id) as mml
																		,(select top 1 s.product_production_name					from #columns as s where c.sap_id	   = s.sap_id	  ) as product_production_name
																		,(select top 1 s.stuffing_group								from #columns as s where c.stuffing_id = s.stuffing_id) as stuffing_group
																		,(select top 1 s.maturation_days							from #columns as s where c.stuffing_id = s.stuffing_id) as maturation_days
																		,(select top 1 s.maturation_and_packaging_days				from #columns as s where c.stuffing_id = s.stuffing_id) as maturation_and_packaging_days
																		,(select top 1 s.transit_from_production_days				from #columns as s where c.stuffing_id = s.stuffing_id) as transit_from_production_days
							
																		,(select top 1 s.count_chamber								from #columns as s where c.stuffing_id = s.stuffing_id) as count_chamber
																		,(select top 1 s.minimum_preparation_materials_kg			from #columns as s where c.stuffing_id = s.stuffing_id) as minimum_preparation_materials_kg
																		,(select top 1 s.minimum_volume_for_chamber_kg				from #columns as s where c.stuffing_id = s.stuffing_id) as minimum_volume_for_chamber_kg

																		,(select top 1 s.minimum_volume_for_marking_kg				from #columns as s where c.stuffing_id = s.stuffing_id) as minimum_volume_for_marking_kg
																		,(select top 1 s.step_marking_kg							from #columns as s where c.stuffing_id = s.stuffing_id) as step_marking_kg
																		,(select top 1 s.marking_line_type							from #columns as s where c.stuffing_id = s.stuffing_id) as marking_line_type
																		,(select top 1 s.marking_line_productivity_kg				from #columns as s where c.stuffing_id = s.stuffing_id) as marking_line_productivity_kg'


						--set @report_dt_to = @dt_while + 3; -- для теста
						set @dt_while = @report_dt_from;
						while @dt_while <= @report_dt_to
						begin
								-- дата доступности
								set @sql = @sql + char(10) + ',DATEADD(day, max(c.maturation_and_packaging_days - c.maturation_days + transit_from_production_days), ''' + format(@dt_while, 'yyyyMMdd') + ''') as stuffing_available_date_' + format(@dt_while, 'yyyyMMdd')
					
								-- выход набивки
								set @sql = @sql + char(10) + ',sum(nullif(
																			isnull(st.stuffing_surplus_kg_' + format(@dt_while, 'yyyyMMdd') + ', 0) +
																			isnull(st.stuffing_marking_kg_' + format(@dt_while, 'yyyyMMdd') + ', 0)
																		 , 0)) as stuffing_kg_'	+ format(@dt_while, 'yyyyMMdd')
								-- нераспределенная набивка
								set @sql = @sql + char(10) + ',sum(st.stuffing_surplus_kg_'		+ format(@dt_while, 'yyyyMMdd') + ') as stuffing_surplus_kg_'	+ format(@dt_while, 'yyyyMMdd')
					
								-- маркировка набивки
								set @sql = @sql + char(10) + ',sum(st.stuffing_marking_kg_'		+ format(@dt_while, 'yyyyMMdd') + ') as stuffing_marking_kg_'	+ format(@dt_while, 'yyyyMMdd')					
					
								-- потребность изначальная
								set @sql = @sql + char(10) + ',sum(sh.shipment_kg_'				+ format(@dt_while, 'yyyyMMdd') + ') as shipment_kg_'			+ format(@dt_while, 'yyyyMMdd')

								-- потребность после остатков
								set @sql = @sql + char(10) + ',sum(sh.stock_net_need_kg_'		+ format(@dt_while, 'yyyyMMdd') + ') as stock_net_need_kg_'		+ format(@dt_while, 'yyyyMMdd')
								
								-- маркировка
								set @sql = @sql + char(10) + ',null as marking_kg_'				+ format(@dt_while, 'yyyyMMdd')

								-- остаток после маркировки и отгрузки
								set @sql = @sql + char(10) + ',null as stock_marking_kg_'		+ format(@dt_while, 'yyyyMMdd')

								set @dt_while = @dt_while + 1;
						end;

																	-- into cherkizovo.temp.fff  
								set @sql = @sql + char(10) + 'from #columns as c'
			

								---- НАБИВКИ ----------------------------------------------------------------------
								set @sql = @sql + char(10) + 'left join #stuffing_pivot as st on c.stuffing_id = st.stuffing_id and isnull(c.sap_id, 0) = isnull(st.sap_id, 0)'

								-- ПОТРЕБНОСТЬ ----------------------------------------------------------------------
								set @sql = @sql + char(10) + 'left join #shipments_pivot as sh on c.stuffing_id = sh.stuffing_id and isnull(c.sap_id, 0) = isnull(sh.sap_id, 0)'

								set @sql = @sql + char(10) + 'group by rollup(   c.stuffing_production_name
																				,c.stuffing_type
																				,c.stuffing_id	
																				,c.sap_id)'

								set @sql = @sql + char(10) + 'having not (GROUPING_ID(c.sap_id) = 0 and c.sap_id is null)'
																		--and not (GROUPING_ID(c.stuffing_production_name) = 1)'
																 
					
								--print  (@sql);
								exec (@sql);

								IF OBJECT_ID('tempdb..#stuffing_pivot','U')  is not null drop table #stuffing_pivot;
								IF OBJECT_ID('tempdb..#shipments_pivot','U') is not null drop table #shipments_pivot;



			end;

			-- ДОПОЛНИТЕЛЬНЫЙ ОТЧЕТ
			if @type_report = 'report_for_pivot'
			begin
					
					select 
							 'для впр' = isnull(c.stuffing_production_name, 'Завод не указан') + '|' + isnull(c.stuffing_type , 'Тип набивки не указан') + '|' + isnull(c.stuffing_id , 'Код набивки не указан') + '|' + FORMAT(c.sap_id, '000000000000000000000000') 
							,'Код набивки' = c.stuffing_id
							,'MML' = c.mml
							,'Название набивки' = c.stuffing_name
							,'Производитель' = c.stuffing_production_name
							,'Тип набивки' = c.stuffing_type
							,'Цикл созревания' = c.maturation_days
							,'Цикл созревания + упаковка' = c.maturation_and_packaging_days
							,'Транзит с производства' = c.transit_from_production_days
							,'Минимальный квант маркировки' = c.minimum_volume_for_marking_kg
							,'Кратность маркировки' = c.step_marking_kg
							,'Упаковочная линия кг в час' = c.marking_line_productivity_kg
							,'Тип упаковочной линии' = c.marking_line_type
							,'SAP ID' = '''' + FORMAT(c.sap_id, '000000000000000000000000')
							,'Код зависимой позиции' = c.position_dependent_id
							,'Название SKU 1С' = c.product_1C_full_name
							,'Общий срок годности' = c.expiration_date_in_days
							,'Дата производства' = cl.dt_tm
							,'Дата доступности' = cl.dt_tm + transit_from_production_days + 1
							,'Годен до' = cl.dt_tm + c.expiration_date_in_days
							,'Закладка (замес)' = convert(dec(15,5), null)
							,'Итог' = convert(dec(15,5), null)
					from #columns as c
					cross join cherkizovo.info.calendar as cl
					where cl.dt_tm between @report_dt_from and @report_dt_to
					  and not c.sap_id is null;
			end;						 

end;

		











































