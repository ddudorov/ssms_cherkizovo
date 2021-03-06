﻿use project_plan_production_finished_products 

go

-- exec .report.for_marking_plan @type_report = 'report_main'
-- exec .report.for_marking_plan @type_report = 'report_for_pivot'

ALTER PROCEDURE report.for_marking_plan @type_report varchar(50) = 'report_main'
as
BEGIN
			SET NOCOUNT ON;
			
			-- для теста
			--declare @type_report varchar(50); set @type_report = 'report_main'
			
			declare @report_dt_from datetime;	set @report_dt_from =	(select top 1 dt 
																		 from (	select  isnull(min(stuffing_production_date_to),'29990101') as dt  from .data_import.stuffing_fact union all
																				select  isnull(min(stuffing_production_date_to),'29990101') as dt  from .data_import.stuffing_plan ) as s 
																		 order by dt);																 
			declare @report_dt_to datetime;		set @report_dt_to =		(select			max(shipment_date)						from .data_import.shipment);																 
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
							 s.stuffing_sap_id as sap_id
							,s.stuffing_id
							,s.stuffing_production_date_to
							,sum(s.stuffing_surplus_kg) as stuffing_surplus_kg
							,sum(s.stuffing_marking_kg) as stuffing_marking_kg
					into #stuffing
					from (
							select 
									 s.stuffing_sap_id
									,s.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_to
									,s.stuffing_available_date
									--,s.stuffing_kg
									--,s.stuffing_marking_kg
									--,s.stuffing_shipment_kg
									,s.stuffing_surplus_kg
									,case when not s.stuffing_sap_id is null then   nullif( isnull( s.stuffing_marking_kg, 0) + isnull( s.stuffing_shipment_kg, 0) , 0)   end as stuffing_marking_kg
							from .data_import.stuffing_fact as s
							
							union all
							select 
									 s.stuffing_sap_id
									,s.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_to
									,s.stuffing_available_date
									--,s.stuffing_kg
									--,s.stuffing_marking_kg
									--,s.stuffing_shipment_kg
									,s.stuffing_surplus_kg
									,case when not s.stuffing_sap_id is null then   nullif( isnull( s.stuffing_marking_kg, 0) + isnull( s.stuffing_shipment_kg, 0) , 0)   end as stuffing_marking_kg
							from .data_import.stuffing_plan as s
						 ) as s
					group by 
							 s.stuffing_sap_id
							,s.stuffing_id
							,s.stuffing_production_date_to;

			end;

			-- ПОТРЕБНОСТЬ К ОТГРУЗКЕ
			begin

						IF OBJECT_ID('tempdb..#shipments','U') is not null drop table #shipments;

						select
								 p.sap_id
								,p.stuffing_id	
								,p.stuffing_id_box	
								,p.stuffing_production_date_to
								,p.stuffing_available_date
								,sum(p.shipment_kg) as shipment_kg
								,sum(p.net_need_kg) as net_need_kg
						into #shipments
						from (

								-- потребность которая не распределилась
								select 
										 p.shipment_sap_id					as sap_id
										--,p.shipment_stuffing_id				as stuffing_id
										,case 
												when p.shipment_stuffing_id_box_type = 0 then p.shipment_stuffing_id
												when p.shipment_stuffing_id_box_type = 1 then p.shipment_stuffing_id
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_stuffing_id_box
										 end as stuffing_id
										,case 
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_stuffing_id
										 end as stuffing_id_box
										,p.shipment_date - sf.transit_days - sf.packaging_days as stuffing_production_date_to
										,p.shipment_date as stuffing_available_date
										,case 
												when p.shipment_stuffing_id_box_type = 0 then p.shipment_after_stuffing_plan_kg
												--when p.shipment_stuffing_id_box_type = 1 then p.shipment_after_stock_kg
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_kg
										 end as shipment_kg
										,case 
												when p.shipment_stuffing_id_box_type = 0 then p.shipment_after_stuffing_plan_kg
												--when p.shipment_stuffing_id_box_type = 1 then p.shipment_after_stuffing_plan_kg
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_after_stock_kg
										 end as net_need_kg
								from .data_import.shipment as p 
								join .info.stuffing as sf on p.shipment_stuffing_id = sf.stuffing_id
								where p.shipment_delete = 0		
									and p.shipment_stuffing_id_box_type in (0 ,2)
									and not p.shipment_sap_id is null 
									and not p.shipment_stuffing_id is null 					 
									and not isnull(p.shipment_product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
									and ISNUMERIC(left(isnull(p.shipment_stuffing_id,''), 5)) = 1	
								    --and p.shipment_kg is not null
									
									
								union all
								 
								-- потребность из остатков она то и будет чистой после отгрузки остатков, есть аналог в группе, потребность закрылась из другого артикула
								select 
										 st.stock_sap_id
										,st.stock_stuffing_id
										,null as stuffing_id_box
										,l.shipment_date - sf.transit_days - sf.packaging_days as stuffing_production_date_to
										,l.shipment_date as stuffing_available_date
										,l.stock_shipment_kg as shipment_kg
										,null as net_need_kg
								from .data_import.stock_log_calculation as l
								join .data_import.stock					as st on l.stock_row_id = st.stock_row_id
								join .info.stuffing						as sf on st.stock_stuffing_id = sf.stuffing_id
								join .info_view.sap_id					as ps on st.stock_sap_id = ps.sap_id_for_join 
																	and not isnull(ps.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
																	and ISNUMERIC(left(isnull(st.stock_stuffing_id,''), 5)) = 1	
								where not l.shipment_row_id in (select s.shipment_row_id
																from .data_import.shipment as s
																where not s.shipment_stuffing_id_box_row_id is null) -- исключаем коробки

								union all
								 
								-- потребность из набивок факт, так как есть приоритеты и артикул может изменить
								select 
										 l.stuffing_sap_id
										,st.stuffing_id
										,null as stuffing_id_box
										,l.shipment_date - DATEDIFF(day,st.stuffing_production_date_to, st.stuffing_available_date) as stuffing_production_date_to
										,l.shipment_date as stuffing_available_date
										,l.stuffing_shipment_kg as shipment_kg
										,l.stuffing_shipment_kg as net_need_kg
								from .data_import.stuffing_fact_log_calculation as l
								join .data_import.stuffing_fact	as st on l.stuffing_sap_id = st.stuffing_sap_id and l.stuffing_row_id = st.stuffing_sap_id_row_id
								where not l.shipment_row_id in (select s.shipment_row_id
																from .data_import.shipment as s
																where not s.shipment_stuffing_id_box_row_id is null)

								union all
								 
								-- потребность из набивок план, так как есть приоритеты и артикул может изменить
								select 
										 l.stuffing_sap_id
										,st.stuffing_id
										,null as stuffing_id_box
										,l.shipment_date - DATEDIFF(day,st.stuffing_production_date_to, st.stuffing_available_date) as stuffing_production_date_to
										,l.shipment_date as stuffing_available_date
										,l.stuffing_shipment_kg as shipment_kg
										,l.stuffing_shipment_kg as net_need_kg
								from .data_import.stuffing_plan_log_calculation as l
								join .data_import.stuffing_plan	as st on l.stuffing_sap_id = st.stuffing_sap_id and l.stuffing_row_id = st.stuffing_sap_id_row_id
								where not l.shipment_row_id in (select s.shipment_row_id
																from .data_import.shipment as s
																where not s.shipment_stuffing_id_box_row_id is null) -- исключаем коробки


							) as p join .info.stuffing as st on p.stuffing_id = st.stuffing_id
						group by  
								 p.sap_id
								,p.stuffing_id
								,p.stuffing_id_box
								,p.stuffing_production_date_to
								,p.stuffing_available_date;


			end;
		
			-- СТОЛБЦЫ ДЛЯ ОТЧЕТА
			begin

						IF OBJECT_ID('tempdb..#columns','U') is not null drop table #columns;		
						-- select * from #columns where sap_id = 000000001030603716300101 order by 1,2	
						-- select stuffing_id, stuffing_id_box, sap_id  from #columns order by 1,2


						with clm as 
						(
								select distinct stuffing_id, stuffing_id_box, sap_id from #shipments 

								union
								
								select distinct stuffing_id_box, null, sap_id from #shipments where not stuffing_id_box is null

								union 

								select distinct stuffing_id, null,			  sap_id from #stuffing

								union 

								select 
										 sp.stuffing_id
										,null			  
										,sp.sap_id 
								from info_view.sap_id as sp	
								where not isnull(sp.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
								  and sp.sap_id_corrected_need is null
								  and not sp.sap_id_for_join is null
								  and ISNUMERIC(LEFT(sp.stuffing_id, 5)) = 1 
								 
						)

						select 
								 c.stuffing_id
								,c.stuffing_id_box
								,t.mml
								,t.stuffing_name
								,t.production_name as stuffing_production_name
								,t.stuffing_type
								,t.stuffing_group	

								,t.fermentation_and_maturation_days as maturation_days
								,t.fermentation_and_maturation_days + t.packaging_days as maturation_and_packaging_days
								,t.transit_days as transit_from_production_days

								,t.chamber_count as count_chamber
								,t.stuffing_minimum_volume_kg as  minimum_preparation_materials_kg
								,t.chamber_minimum_volume_kg as minimum_volume_for_chamber_kg
								
								,t.marking_minimum_kg as minimum_volume_for_marking_kg
								,t.marking_step_kg as step_marking_kg
								,t.marking_line_productivity_kg
								,t.marking_line_type

								,c.sap_id
								,convert(bigint,p.position_dependent_id) * 100 + p.individual_marking_id as position_dependent_id_and_individual_marking_id
								,p.individual_marking_name
								,p.product_1C_full_name
								,p.production_full_name as product_production_name
								,p.expiration_date_in_days

						into #columns
						from clm as c
						--from (
						--		select stuffing_id, stuffing_id_box, sap_id	from clm						
						--		union
						--		select stuffing_id, null		   , null	from clm
						--	 ) as c
						left join .info.stuffing as t on c.stuffing_id = t.stuffing_id
						left join info_view.sap_id as p on c.sap_id = p.sap_id_for_join
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
											 stuffing_id		VARCHAR(40)	NOT NULL	
											,stuffing_id_box	VARCHAR(40)		NULL	
											,sap_id				BIGINT		NOT NULL

									);

									-- СОЗДАЕМ СТОЛБЦЫ
									set @dt_while = @report_dt_from;
									while @dt_while <= @report_dt_to
									begin

											set @sql = 'alter table #shipments_pivot add shipment_kg_'	+ format(@dt_while, 'yyyyMMdd') + ' dec(11, 5) null;';	exec (@sql);
											set @sql = 'alter table #shipments_pivot add net_need_kg_'	+ format(@dt_while, 'yyyyMMdd') + ' dec(11, 5) null;';	exec (@sql);
											set @dt_while = @dt_while + 1;
									end;
									
									-- НАПОЛНЯЕМ ДАННЫМИ
											set @sql = ''
											set @sql = @sql + char(10) + 'insert into #shipments_pivot
																		  select st.stuffing_id, st.stuffing_id_box, st.sap_id'
									set @dt_while = @report_dt_from;
									while @dt_while <= @report_dt_to
									begin
											set @sql = @sql + char(10) + '		,sum(iif(st.stuffing_production_date_to = ''' + format(@dt_while, 'yyyyMMdd') + ''', st.shipment_kg	 , null) ) as shipment_kg_' +  format(@dt_while, 'yyyyMMdd')
											set @sql = @sql + char(10) + '		,sum(iif(st.stuffing_production_date_to = ''' + format(@dt_while, 'yyyyMMdd') + ''', st.net_need_kg	 , null) ) as net_need_kg_' +  format(@dt_while, 'yyyyMMdd')

											set @dt_while = @dt_while + 1;
									end;
											set @sql = @sql + char(10) + 'from #shipments as st 
																		  group by st.stuffing_id, st.stuffing_id_box, st.sap_id'
											exec (@sql);
											
									IF OBJECT_ID('tempdb..#shipments','U') is not null drop table #shipments;
									--select * from #shipments_pivot;
						end;
									
								IF OBJECT_ID('tempdb..#report_main','U')  is not null drop table #report_main;
						
								set @sql = ''
								set @sql = @sql + char(10) + 'select' 
								set @sql = @sql + char(10) + '			 case		
																				when GROUPING_ID(c.stuffing_production_name) = 1				then 0
																				when GROUPING_ID(c.stuffing_type) = 1							then 1
																				when GROUPING_ID(c.stuffing_id) = 1								then 2
																				when GROUPING_ID(c.sap_id) = 1									then 3
																				when GROUPING_ID(c.sap_id) = 0 and c.stuffing_id_box is null	then 4
																				when GROUPING_ID(c.sap_id) = 0									then 5
																		 end as frm_id'

								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.sap_id) = 0 and c.stuffing_id_box is null	then isnull(c.stuffing_id , ''Код набивки не указан'') + ''|'' + FORMAT(c.sap_id, ''000000000000000000000000'')  
																		 end as stuffing_id_sap_id_for_number_row' 

								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_production_name) = 1				then null
																				when GROUPING_ID(c.stuffing_type) = 1							then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|''
																				when GROUPING_ID(c.stuffing_id) = 1								then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|''
																				when GROUPING_ID(c.sap_id) = 1									then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|'' + isnull(c.stuffing_id , ''Код набивки не указан'') + ''|''
																				when GROUPING_ID(c.sap_id) = 0 and c.stuffing_id_box is null	then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|'' + isnull(c.stuffing_id , ''Код набивки не указан'') + ''|'' + FORMAT(c.sap_id, ''000000000000000000000000'') + ''|''
																				when GROUPING_ID(c.sap_id) = 0									then isnull(c.stuffing_production_name, ''Завод не указан'') + ''|'' + isnull(c.stuffing_type , ''Тип набивки не указан'') + ''|'' + isnull(c.stuffing_id , ''Код набивки не указан'') + ''|'' + FORMAT(c.sap_id, ''000000000000000000000000'') + ''|'' + isnull(c.stuffing_id_box, '''') + ''|''
																		 end as data_hierarchy'

								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_production_name) = 1				then ''Общий итог''
																				when GROUPING_ID(c.stuffing_type) = 1							then isnull(c.stuffing_production_name, ''Завод не указан'')
																				when GROUPING_ID(c.stuffing_id) = 1								then isnull(c.stuffing_type , ''Тип набивки не указан'')
																				when GROUPING_ID(c.sap_id) = 1									then isnull(c.stuffing_id , ''Код набивки не указан'')
																				when GROUPING_ID(c.sap_id) = 0									then FORMAT(c.sap_id, ''000000000000000000000000'')
																		 end as data_id'	
																		 	
								set @sql = @sql + char(10) + '			,case		
																				when GROUPING_ID(c.stuffing_production_name) = 1	then null
																				when GROUPING_ID(c.stuffing_type) = 1				then null
																				when GROUPING_ID(c.stuffing_id) = 1					then null
																				when GROUPING_ID(c.sap_id) = 1						then (select top 1 s.stuffing_name from #columns as s where c.stuffing_id = s.stuffing_id)
																				when GROUPING_ID(c.sap_id) = 0						then (select top 1 s.product_1C_full_name from #columns as s where c.sap_id = s.sap_id)
																		 end as data_name'

								set @sql = @sql + char(10) + '			,(select top 1 s.mml										from #columns as s where c.stuffing_id = s.stuffing_id) as mml
																		,c.stuffing_id_box
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


						--set @report_dt_to = @report_dt_from + 2; -- для теста
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
								set @sql = @sql + char(10) + ',sum(sh.net_need_kg_'				+ format(@dt_while, 'yyyyMMdd') + ') as net_need_kg_'			+ format(@dt_while, 'yyyyMMdd')
								
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
								set @sql = @sql + char(10) + 'left join #shipments_pivot as sh on c.stuffing_id = sh.stuffing_id and isnull(c.stuffing_id_box, ''0'') = isnull(sh.stuffing_id_box, ''0'') and isnull(c.sap_id, 0) = isnull(sh.sap_id, 0)'


								set @sql = @sql + char(10) + 'GROUP BY	GROUPING SETS (()
																					  ,(c.stuffing_production_name)
																					  ,(c.stuffing_production_name, c.stuffing_type)
																					  ,(c.stuffing_production_name, c.stuffing_type, c.stuffing_id)
																					  ,(c.stuffing_production_name, c.stuffing_type, c.stuffing_id, c.stuffing_id_box,c.sap_id)
																					  )'

								set @sql = @sql + char(10) + 'having not (GROUPING_ID(c.sap_id) = 0 and c.sap_id is null)'
																 
					
								--print  (@sql);
								exec (@sql);

								IF OBJECT_ID('tempdb..#stuffing_pivot','U')  is not null drop table #stuffing_pivot;
								IF OBJECT_ID('tempdb..#shipments_pivot','U') is not null drop table #shipments_pivot;



			end;

			-- ДОПОЛНИТЕЛЬНЫЙ ОТЧЕТ
			if @type_report = 'report_for_pivot'
			begin
					
					select 
							 'для впр' = isnull(c.stuffing_production_name, 'Завод не указан') + '|' + isnull(c.stuffing_type , 'Тип набивки не указан') + '|' + isnull(c.stuffing_id , 'Код набивки не указан') + '|' + FORMAT(c.sap_id, '000000000000000000000000') + '|'
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
							,'SAP ID' = FORMAT(c.sap_id, '000000000000000000000000')
							,'Заготовка пф' = (select top 1 'Заготовка пф' from #columns as b where c.stuffing_id = b.stuffing_id_box and c.sap_id = b.sap_id)
							,'Код зависимой позиции + ИМ' = c.position_dependent_id_and_individual_marking_id
							,'Название ИМ' = c.individual_marking_name
							,'Название SKU 1С' = c.product_1C_full_name
							,'Общий срок годности' = c.expiration_date_in_days
							,'Дата производства' = cl.dt_tm
							,'Дата доступности' = cl.dt_tm + transit_from_production_days + 1
							,'Годен до' = cl.dt_tm + c.expiration_date_in_days							
							,'Итог' = convert(dec(15,5), null)
					from #columns as c
					cross join cherkizovo.info.calendar as cl
					where cl.dt_tm between @report_dt_from and @report_dt_to
					  and not c.sap_id is null
					  and c.stuffing_id_box is null;
			end;						 

end;

		




