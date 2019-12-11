use project_plan_production_finished_products

go

-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'fact' 
-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'plan'

ALTER PROCEDURE calc.distribution_stuffing @type_stuffing varchar(15)
as
BEGIN
			
			-- declare @type_stuffing varchar(15); set @type_stuffing = 'fact'

			SET NOCOUNT ON;
			--------------------
			-- ПОДГОТОВКА ДАННЫХ
			--------------------

			-- ОЧИЩАЕМ ЕСЛИ РАНЬШЕ УЖЕ БЫЛ РАСЧЕТ	
			begin 

					if @type_stuffing = 'fact' update project_plan_production_finished_products.data_import.shipment		set shipment_from_stuffing_fact_kg  = null;		
					if @type_stuffing = 'fact' delete project_plan_production_finished_products.data_import.stuffing_fact	where not stuffing_sap_id is null;
					if @type_stuffing = 'fact' update project_plan_production_finished_products.data_import.stuffing_fact	set stuffing_marking_kg = null, stuffing_shipment_kg = null;	

											   update project_plan_production_finished_products.data_import.shipment		set shipment_from_stuffing_plan_kg = null;	
											   delete project_plan_production_finished_products.data_import.stuffing_plan	where not stuffing_sap_id is null;
											   update project_plan_production_finished_products.data_import.stuffing_plan	set stuffing_marking_kg = null, stuffing_shipment_kg = null;

			end;


			-- ОТГРУЗКА ДЛЯ РАСЧЕТОВ | ПОДГОТОВКА
			begin
					

					IF OBJECT_ID('tempdb..#all_sap_id','U') is not null drop table #all_sap_id; 

					select 
							 sm.sap_id
							,sm.sap_id_priority_1
							,sm.sap_id_priority_2
							,sm.sap_id_priority_3
							,sp.expiration_date_in_days
							,sm.stuffing_id
							,sp.product_status
							,iif(ISNUMERIC(left(isnull(sm.stuffing_id,''), 5)) <> 1,							'набивка не номер',	null) as check_stuffing_id
							,iif(isnull(sp.product_status, '') in ('БлокирДляЗаготов/Склада', 'Устаревший'),	'заблокирован',		null) as check_product_status
					into #all_sap_id
					from project_plan_production_finished_products.info.finished_products_sap_id_manual as sm
					join cherkizovo.info.products_sap as sp on sm.sap_id = sp.sap_id;
					

					-- создаем потребность для всех приоритетов, где основной это тот sap id куда повесили потребность, 0 это если коробки и/или нет приоритетов, 1 --> 2 --> 3 --> 4 это приоритет SAP ID 4 = 0
					IF OBJECT_ID('tempdb..#shipment_union_all','U') is not null drop table #shipment_union_all; 
					-- select * from #shipment_union_all 


					select
							 o.sap_id_priority_1
							,o.expiration_date_in_days_priority_1
							,o.stuffing_id_priority_1
							,iif(o.calculation_priority_1 = 'подходит', 1, null) as priority_1

							,o.sap_id_priority_2
							,o.expiration_date_in_days_priority_2
							,o.stuffing_id_priority_2		
							,iif(o.calculation_priority_2 = 'подходит', 2, null) as priority_2

							,o.sap_id_priority_3
							,o.expiration_date_in_days_priority_3
							,o.stuffing_id_priority_3									
							,iif(o.calculation_priority_3 = 'подходит', 3, null) as priority_3

							,o.shipment_sap_id
							,o.shipment_sap_id_expiration_date_in_days
							,o.shipment_stuffing_id
							,o.shipment_stuffing_id_box_type
							,o.shipment_stuffing_id_box_row_id
															
							,case 
								when o.calculation_priority_main = 'подходит' and  o.calculation_priority_1 <> 'подходит' and o.calculation_priority_2 <> 'подходит' and o.calculation_priority_3 <> 'подходит'  then 0
								when o.calculation_priority_main = 'подходит' and (o.calculation_priority_1 =  'подходит' or  o.calculation_priority_2 =  'подходит' or  o.calculation_priority_3 =  'подходит') then 4
							 end as priority_main
							 
							,o.shipment_row_id
							,o.shipment_priority
							,o.shipment_min_KOS
							,o.shipment_date_max							
							,o.shipment_kg

					into #shipment_union_all
					from (
							select  
									 spm.sap_id_priority_1
									,p1.expiration_date_in_days		as expiration_date_in_days_priority_1
									,p1.stuffing_id					as stuffing_id_priority_1
									,case 
											when not o.shipment_stuffing_id_box_row_id is null		then 'коробка'
											when spm.sap_id_priority_1 is null						then 'нет приоритета'
											when not p1.check_stuffing_id is null					then 'набивка не номер'
											when not p1.check_product_status is null				then 'заблокирован'
											else 'подходит'
									 end as calculation_priority_1


									,spm.sap_id_priority_2					
									,p2.expiration_date_in_days		as expiration_date_in_days_priority_2
									,p2.stuffing_id					as stuffing_id_priority_2
									,case 
											when not o.shipment_stuffing_id_box_row_id is null		then 'коробка'
											when spm.sap_id_priority_2 is null						then 'нет приоритета'
											when not p2.check_stuffing_id is null					then 'набивка не номер'
											when not p2.check_product_status is null				then 'заблокирован'
											else 'подходит'
									 end as calculation_priority_2


									,spm.sap_id_priority_3		
									,p3.expiration_date_in_days		as expiration_date_in_days_priority_3
									,p3.stuffing_id					as stuffing_id_priority_3
									,case 
											when not o.shipment_stuffing_id_box_row_id is null		then 'коробка'
											when spm.sap_id_priority_3 is null						then 'нет приоритета'
											when not p3.check_stuffing_id is null					then 'набивка не номер'
											when not p3.check_product_status is null				then 'заблокирован'
											else 'подходит'
									 end as calculation_priority_3

									,o.shipment_sap_id							as shipment_sap_id
									,o.shipment_sap_id_expiration_date_in_days	as shipment_sap_id_expiration_date_in_days
									,o.shipment_stuffing_id						as shipment_stuffing_id
									,o.shipment_stuffing_id_box_type			as shipment_stuffing_id_box_type
									,o.shipment_stuffing_id_box_row_id			as shipment_stuffing_id_box_row_id
									
									,case 
											when not p3.check_stuffing_id is null					then 'набивка не номер'
											when not p3.check_product_status is null				then 'заблокирован'
											--when not o.shipment_stuffing_id_box_row_id is null		then 'коробка'
											else 'подходит'
									 end as calculation_priority_main
									 
									,o.shipment_row_id
									,o.shipment_priority
									,o.shipment_min_KOS
									,o.shipment_date as shipment_date_max
									,iif(@type_stuffing = 'fact', o.shipment_after_stock_kg, o.shipment_after_stuffing_fact_kg) as shipment_kg

							from project_plan_production_finished_products.data_import.shipment as o
							join #all_sap_id as spm on o.shipment_sap_id = spm.sap_id
							left join #all_sap_id as p1 on spm.sap_id_priority_1 = p1.sap_id
							left join #all_sap_id as p2 on spm.sap_id_priority_2 = p2.sap_id
							left join #all_sap_id as p3 on spm.sap_id_priority_3 = p3.sap_id

							where o.shipment_stuffing_id_box_type in (0, 2) -- 0 берем набики не коробки, 2 коробки уже разбиты на набивки
								and o.shipment_delete = 0
								and o.shipment_reason_ignore_in_calculate is null
								and not iif(@type_stuffing = 'fact', o.shipment_after_stock_kg, o.shipment_after_stuffing_fact_kg) is null

					) as o;

					IF OBJECT_ID('tempdb..#all_sap_id','U') is not null drop table #all_sap_id; 

			end;
			

			-- НАБИВКИ | ПОДГОТОВКА
			begin
					
					IF OBJECT_ID('tempdb..#stuffing_without_sap_id','U') is not null drop table #stuffing_without_sap_id;
					-- select * from #stuffing_without_sap_id

					-- создаем таблицу
					create table #stuffing_without_sap_id
					(

						 stuffing_row_id						INT					NOT NULL
						,stuffing_data_type						VARCHAR(40)			not null
						,stuffing_id							VARCHAR(40)			NOT NULL

						,stuffing_available_date				DATETIME				NULL
						,stuffing_before_next_available_date	DATETIME				NULL

						,stuffing_kg							dec(11,5)				NULL

					)
					-- заполняем таблицу
					exec('	
							insert into #stuffing_without_sap_id
							select 
									 stuffing_row_id
									,stuffing_data_type
									,stuffing_id
									,stuffing_available_date
									,stuffing_before_next_available_date
									,stuffing_kg
							from project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '
							where stuffing_sap_id is null;
						 ');
	 
			end;


			-- ОТГРУЗКА ДЛЯ РАСЧЕТОВ | ОСНОВНАЯ ТАБЛИЦА
			begin
					
					IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment;

					--select * from #shipment order by 1

					select
								convert(int, ROW_NUMBER() over (order by o.priority_sap_id
																		,o.stuffing_available_date

																		,o.before_next_shipment_priority
																		,o.before_next_individual_marking_id
																		,o.before_next_shipment_kg desc
																		,o.before_next_shipment_date_max
																		,o.before_next_shipment_min_KOS

							
																		,o.after_next_shipment_date_max
																		,o.after_next_shipment_priority
																		,o.after_next_individual_marking_id
																		,o.after_next_shipment_min_KOS
																		,o.after_next_shipment_kg desc
																)) as shipment_id
								,o.shipment_sap_id	
								,o.shipment_stuffing_id	
								,o.shipment_stuffing_id_box_row_id	
								
								/*
								,o.priority_sap_id	
								,o.stuffing_available_date								
								,o.stuffing_before_next_available_date

								-- before_next
								,o.before_next_shipment_priority
								,o.before_next_shipment_date_max
								,o.before_next_shipment_min_KOS
								,o.before_next_shipment_kg
								,o.before_next_individual_marking_id
																																		
																																		
								-- after_next																						
								,o.after_next_shipment_priority
								,o.after_next_shipment_date_max									
								,o.after_next_shipment_min_KOS
								,o.after_next_shipment_kg
								,o.after_next_individual_marking_id
								*/		
								,o.shipment_row_id	
								--,o.shipment_priority	
								--,o.shipment_min_KOS	
								,o.shipment_date_min
								,o.shipment_date_max		
								,o.shipment_kg
					into #shipment
					from (
							select
									 o.shipment_sap_id	
									,o.shipment_stuffing_id	
									,o.shipment_stuffing_id_box_row_id	
									,o.priority_sap_id	

									,st.stuffing_available_date								
									,st.stuffing_before_next_available_date

									-- before_next
									,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_priority		, 99999			) as before_next_shipment_priority
									,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_date_max		, '29990101'	) as before_next_shipment_date_max
									,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_min_KOS		, 1				) as before_next_shipment_min_KOS
									,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_kg			, 0				) as before_next_shipment_kg
									,iif(st.stuffing_before_next_available_date <> '29990101'											
															and sp.individual_marking_id <> 1, sp.individual_marking_id	, 99999			) as before_next_individual_marking_id
																																		
																																		
									-- after_next																						
									,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_priority		, 99999			) as after_next_shipment_priority
									,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_date_max		, '29990101'	) as after_next_shipment_date_max									
									,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_min_KOS		, 1				) as after_next_shipment_min_KOS
									,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_kg			, 0				) as after_next_shipment_kg
									,iif(st.stuffing_before_next_available_date =  '29990101'
															and sp.individual_marking_id <> 1, sp.individual_marking_id , 99999			) as after_next_individual_marking_id
												
									,o.shipment_row_id
									,o.shipment_priority	
									,o.shipment_min_KOS	
									,o.shipment_date_min
									,o.shipment_date_max	
									,o.shipment_kg
							from (

									select 
											 o.shipment_sap_id	
											,o.shipment_stuffing_id	
											,o.shipment_stuffing_id_box_row_id	
											,o.priority_main as priority_sap_id	
											
											,o.shipment_row_id
											,o.shipment_priority	
											,o.shipment_min_KOS	
											,o.shipment_date_max - FLOOR (o.shipment_sap_id_expiration_date_in_days - o.shipment_sap_id_expiration_date_in_days * o.shipment_min_KOS) as shipment_date_min
											,o.shipment_date_max		
											,o.shipment_kg
									from #shipment_union_all as o
									where not o.priority_main is null

									union all

									select 
											 o.sap_id_priority_1 as shipment_sap_id	
											,o.stuffing_id_priority_1 as shipment_stuffing_id
											,null as shipment_stuffing_id_box_row_id	
											,o.priority_1 as priority_sap_id	
							
											,o.shipment_row_id
											,o.shipment_priority	
											,o.shipment_min_KOS	
											,o.shipment_date_max - FLOOR (o.shipment_sap_id_expiration_date_in_days - o.expiration_date_in_days_priority_1 * o.shipment_min_KOS) as shipment_date_min
											,o.shipment_date_max	
											,o.shipment_kg
									from #shipment_union_all as o
									where not o.priority_1 is null
					
									union all

									select 
											 o.sap_id_priority_2 as shipment_sap_id	
											,o.stuffing_id_priority_2 as shipment_stuffing_id
											,null as shipment_stuffing_id_box_row_id	
											,o.priority_2 as priority_sap_id
											
											,o.shipment_row_id
											,o.shipment_priority	
											,o.shipment_min_KOS	
											,o.shipment_date_max - FLOOR (o.expiration_date_in_days_priority_2 - o.expiration_date_in_days_priority_2 * o.shipment_min_KOS) as shipment_date_min
											,o.shipment_date_max	
											,o.shipment_kg
									from #shipment_union_all as o
									where not o.priority_2 is null
					
									union all

									select 
											 o.sap_id_priority_3 as shipment_sap_id	
											,o.stuffing_id_priority_3 as shipment_stuffing_id
											,null as shipment_stuffing_id_box_row_id	
											,o.priority_3 as priority_sap_id
											
											,o.shipment_row_id
											,o.shipment_priority	
											,o.shipment_min_KOS	
											,o.shipment_date_max - FLOOR (o.expiration_date_in_days_priority_3 - o.expiration_date_in_days_priority_3 * o.shipment_min_KOS) as shipment_date_min
											,o.shipment_date_max	
											,o.shipment_kg
									from #shipment_union_all as o
									where not o.priority_3 is null

									) as o
							join #stuffing_without_sap_id as st on o.shipment_stuffing_id = st.stuffing_id
							 and o.shipment_date_max between st.stuffing_available_date and st.stuffing_before_next_available_date
							join cherkizovo.info.products_sap as sp on o.shipment_sap_id = sp.SAP_id 
							
						) as o;-- where not o.shipment_stuffing_id_box_row_id is null;

					IF OBJECT_ID('tempdb..#shipment_union_all','U') is not null drop table #shipment_union_all; 
					


					CREATE NonCLUSTERED INDEX NonCl_shipment_id ON #shipment (shipment_id asc);

					CREATE NonCLUSTERED INDEX NonCl_shipment_row_id ON #shipment (shipment_row_id asc)
					INCLUDE(shipment_kg);
					--CREATE NonCLUSTERED INDEX NonCl_shipment_stuffing_id_box_row_id ON #shipment (shipment_stuffing_id_box_row_id desc);
	
	
					--CREATE CLUSTERED INDEX Cl_id ON #shipment (shipment_id, shipment_row_id, shipment_stuffing_id_box_row_id);
	
					----CREATE NonCLUSTERED INDEX NonCl_shipment_id ON #shipment (shipment_id asc, shipment_row_id asc , shipment_stuffing_id_box_row_id desc)
					----INCLUDE(shipment_kg, shipment_check_kg);

					--CREATE NONCLUSTERED INDEX NonCl_shipment_kg ON #shipment (shipment_id, shipment_row_id, shipment_stuffing_id_box_row_id)
					--INCLUDE(shipment_kg);

					--CREATE NONCLUSTERED INDEX NonCl_stuffing_id_box_row_id
					--ON #shipment (shipment_stuffing_id_box_row_id)
					--INCLUDE ([shipment_sap_id],[shipment_stuffing_id],[shipment_date_min],[shipment_date_max],[shipment_kg]);

					--CREATE NONCLUSTERED INDEX NonCl_shipment_row_id ON #shipment (shipment_row_id )
					--INCLUDE(shipment_kg, shipment_check_kg);

					---- создаем индексы
					--CREATE NoNCLUSTERED INDEX NoCl_shipment_id_select ON #shipment (shipment_id)
					--INCLUDE(shipment_sap_id, shipment_stuffing_id, shipment_date_min, shipment_date_max, shipment_kg);


					--CREATE NONCLUSTERED INDEX NonCl_stuffing_id_box_row_id ON #shipment (shipment_stuffing_id_box_row_id)
					--INCLUDE (shipment_id, shipment_sap_id, shipment_stuffing_id, shipment_row_id, shipment_date_min, shipment_date_max, shipment_kg);
			end;
			

			-- НАБИВКИ | КОМБИНИРУЕМ ВСЕ НАБИВКИ С SAP ID
			begin 

					IF OBJECT_ID('tempdb..#stuffing','U') is not null drop table #stuffing;  -- select * from #stuffing_fact

					select 
							 o.stuffing_sap_id	
							,o.stuffing_id
							,st.stuffing_row_id
							,st.stuffing_available_date
							,st.stuffing_before_next_available_date
							,ist.minimum_volume_for_marking_kg as stuffing_min_marking_kg
							,ist.step_marking_kg as stuffing_step_marking_kg
							,st.stuffing_kg as stuffing_initial_kg
							,st.stuffing_kg
							,convert(dec(11,5),null) as stuffing_marking_kg  -- если null значит не маркировался, если 0 то маркировался уже sap id
							,convert(dec(11,5),null) as stuffing_shipment_kg -- кол-во которое уже отгружено из набивки включая маркировку
					into #stuffing
					from (select distinct o.shipment_stuffing_id as stuffing_id, o.shipment_sap_id as stuffing_sap_id from #shipment as o) as o
					join #stuffing_without_sap_id as st on o.stuffing_id = st.stuffing_id
					join project_plan_production_finished_products.info.stuffing as ist on st.stuffing_id = ist.stuffing_id
					where not ist.minimum_volume_for_marking_kg is null;

					-- создаем индексы
					--CREATE CLUSTERED INDEX Cl_stuffing_id ON #stuffing (sap_id, stuffing_id, stuffing_available_date asc, stuffing_before_next_available_date);

					CREATE NONCLUSTERED INDEX NonCl_sap_id_available_date ON #stuffing (stuffing_sap_id,stuffing_available_date)
					INCLUDE (stuffing_row_id, stuffing_min_marking_kg, stuffing_step_marking_kg, stuffing_kg, stuffing_marking_kg);

					CREATE NONCLUSTERED INDEX NonCl_sap_id_row_id ON #stuffing (stuffing_sap_id, stuffing_row_id)
					INCLUDE (stuffing_marking_kg, stuffing_shipment_kg);

					CREATE NONCLUSTERED INDEX NonCl_row_id ON #stuffing (stuffing_row_id)
					INCLUDE (stuffing_sap_id, stuffing_kg, stuffing_marking_kg);

					IF OBJECT_ID('tempdb..#stuffing_without_sap_id','U') is not null drop table #stuffing_without_sap_id;
			end;
				
						
			-- LOG РАСПРЕДЕЛЕНИЕ НАБИВКИ
			begin 

					-- ЛОГ НАБИВОК
					exec('TRUNCATE TABLE project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '_log_calculation;');

					IF OBJECT_ID('tempdb..#stuffing_log_calculation','U') is not null drop table #stuffing_log_calculation;
					-- select * from #stuffing_log_calculation

					create table #stuffing_log_calculation
					( 
							 sort_id				INT				NOT NULL IDENTITY(1,1)
							,shipment_row_id		INT				NOT NULL
							,shipment_date			datetime		NOT NULL
							,shipment_kg			dec(11,5)		NOT NULL
							,stuffing_row_id		INT					NULL	
							,stuffing_sap_id		bigint				NULL	
							,stuffing_kg			dec(11,5)			NULL	
							,stuffing_marking_kg	dec(11,5)			NULL	
							,stuffing_shipment_kg	dec(11,5)			NULL			
					);

			end;


			-- select * from #shipment order by 1
			-- select * from #stuffing
			-- select * from #stuffing_log_calculation

			------------------------
			-- РАСПРЕДЕЛЕНИЕ НАБИВОК
			------------------------
			
			-- переменные для отгрузки
			declare @shipment_id						int;			set @shipment_id = 1;
			declare @shipment_sap_id					bigint;		
			declare @shipment_stuffing_id_box_row_id	int;	
			declare @shipment_date_min					date; 
			declare @shipment_date_max					date; 
			declare @shipment_kg						dec(11,5);
			declare @shipment_row_id					int;	

			while not @shipment_id is null
			begin
						-- заполняем переменные по отгрузке
						select
								 @shipment_id						= max(o.shipment_id)
								,@shipment_sap_id					= max(o.shipment_sap_id)
								,@shipment_stuffing_id_box_row_id	= max(o.shipment_stuffing_id_box_row_id)
								,@shipment_date_min					= max(o.shipment_date_min)
								,@shipment_date_max					= max(o.shipment_date_max)
								,@shipment_kg						= max(o.shipment_kg)
								,@shipment_row_id					= max(o.shipment_row_id)		
						from #shipment as o
						where o.shipment_id = @shipment_id;


						-- ==================== --
						-- распределяем набивки --
						-- ==================== --
						while isnull(@shipment_kg, 0) > 0.0
						begin

									-- НЕ КОРОБКА
									if @shipment_stuffing_id_box_row_id is null
									begin

											--выбираем какую набивку использовать --> (внутрик вызывается хранимка которая маркирует и отгружает набивку
											exec project_plan_production_finished_products.calc.distribution_stuffing_select_for_no_box  @shipment_id			= @shipment_id			
																																		,@shipment_sap_id		= @shipment_sap_id		-- log
																																		,@shipment_date_min		= @shipment_date_min	
																																		,@shipment_date_max		= @shipment_date_max	
																																		,@shipment_kg			= @shipment_kg						
																																		,@shipment_row_id		= @shipment_row_id		-- log
																																		,@shipment_kg_output	= @shipment_kg out
									end;
									
									-- АРТИКУЛ СОСТОИТ ИЗ КОРОБОК (2 ИЛИ БОЛЕЕ НАБИВОК В ОДНОМ АРТИКУЛЕ)
									if not @shipment_stuffing_id_box_row_id is null -- переключить на 2, что бы распределять коробки
									begin
											--выбираем какие набивки использовать --> (внутрик вызывается хранимка которая маркирует набивку --> (внутрик вызывается хранимка которая отгружает))
											exec project_plan_production_finished_products.calc.distribution_stuffing_select_for_box  @shipment_stuffing_id_box_row_id	= @shipment_stuffing_id_box_row_id
																																	 ,@shipment_kg_output				= @shipment_kg out;
									end;

						end;

						set @shipment_id = @shipment_id + 1; 

						--if @shipment_id % 10 = 0
						--begin
						--	select @shipment_id
						--end

							--select @shipment_id
					

			end;
			


			-------------------------------------------------------------
			-- РАСКИДЫВАЕМ ОСТАТОК НАБИВОК ГДЕ ШАГ МАРКИРОВКИ ОТСУТСТВУЕТ
			-------------------------------------------------------------
			begin
					if @type_stuffing = 'fact'
					begin

							insert into project_plan_production_finished_products.data_import.stuffing_fact
							(

									 stuffing_sap_id_row_id
									,stuffing_sap_id
									,stuffing_id
									,stuffing_production_name
									,stuffing_production_date_from
									,stuffing_production_date_to
									,stuffing_available_date
									,stuffing_before_next_available_date
									,stuffing_expiration_date
									,stuffing_marking_kg
									,stuffing_shipment_kg
							)
					
							select 
									st.stuffing_row_id as stuffing_sap_id_row_id
									,st.stuffing_sap_id
									,st.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_from
									,s.stuffing_production_date_to
									,st.stuffing_available_date
									,st.stuffing_before_next_available_date
									,s.stuffing_expiration_date
									--,st.stuffing_initial_kg
									--,st.stuffing_kg
									,nullif(
											 isnull(st.stuffing_marking_kg, 0) +
											 isnull(case when st.stuffing_step_marking_kg is null and st.stuffing_kg <> 0 then st.stuffing_kg * (st.stuffing_shipment_kg / sum(st.stuffing_shipment_kg) over (partition by st.stuffing_row_id)) end , 0) 
										   ,0) as stuffing_marking_kg
									,st.stuffing_shipment_kg
									--,st.stuffing_step_marking_kg
							from #stuffing as st
							join project_plan_production_finished_products.data_import.stuffing_fact as s on st.stuffing_row_id = s.stuffing_row_id
							where isnull(st.stuffing_marking_kg, 0) <> 0 or isnull(st.stuffing_shipment_kg, 0) <> 0;


							 
					end;

					if @type_stuffing = 'plan'
					begin

							insert into project_plan_production_finished_products.data_import.stuffing_plan
							(

									 stuffing_sap_id_row_id
									,stuffing_sap_id
									,stuffing_id
									,stuffing_production_name
									,stuffing_production_date_from
									,stuffing_production_date_to
									,stuffing_available_date
									,stuffing_before_next_available_date
									,stuffing_expiration_date
									,stuffing_marking_kg
									,stuffing_shipment_kg
							)
					
							select 
									st.stuffing_row_id as stuffing_sap_id_row_id
									,st.stuffing_sap_id
									,st.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_from
									,s.stuffing_production_date_to
									,st.stuffing_available_date
									,st.stuffing_before_next_available_date
									,s.stuffing_expiration_date
									--,st.stuffing_initial_kg
									--,st.stuffing_kg
									,nullif(
											 isnull(st.stuffing_marking_kg, 0) +
											 isnull(case when st.stuffing_step_marking_kg is null and st.stuffing_kg <> 0 then st.stuffing_kg * (st.stuffing_shipment_kg / sum(st.stuffing_shipment_kg) over (partition by st.stuffing_row_id)) end , 0) 
										   ,0) as stuffing_marking_kg
									,st.stuffing_shipment_kg
									--,st.stuffing_step_marking_kg
							from #stuffing as st
							join project_plan_production_finished_products.data_import.stuffing_plan as s on st.stuffing_row_id = s.stuffing_row_id
							where isnull(st.stuffing_marking_kg, 0) <> 0 or isnull(st.stuffing_shipment_kg, 0) <> 0;

					end;
								
			end;

			-------------------------------------------
			-- ЛОГ НАБИВКИ ДОБАВЛЯЕМ В ОСНОВНУЮ ТАБЛИЦУ
			-------------------------------------------
			exec('insert into project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '_log_calculation
				  select * from #stuffing_log_calculation');
				 

			-----------------------------------------
			-- ДОБАВЛЯЕМ ИНФОРМАЦИЮ НА УРОВНЕ НАБИВКИ
			-----------------------------------------
			begin
					exec('
							update s
							set s.stuffing_marking_kg = st.stuffing_marking_kg
							   ,s.stuffing_shipment_kg = st.stuffing_shipment_kg
							from project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + ' as s
							join (
									select stuffing_sap_id_row_id, sum(stuffing_marking_kg) as stuffing_marking_kg, sum(stuffing_shipment_kg) as stuffing_shipment_kg
									from project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '
									where not stuffing_sap_id is null and isnull(stuffing_marking_kg, 0) + isnull(stuffing_shipment_kg, 0) > 0
									group by stuffing_sap_id_row_id
								 ) as st on s.stuffing_row_id = st.stuffing_sap_id_row_id

						');

			end;


			----------------------------------------------
			-- ДОБАВЛЯЕМ ИНФОРМАЦИЮ В ТАБЛИЦУ С ОТГРУЗКАМИ
			----------------------------------------------
			begin
						exec('
								update o
								set o.shipment_from_stuffing_' + @type_stuffing + '_kg = l.stuffing_shipment_kg
								from project_plan_production_finished_products.data_import.shipment as o
								join (
										select l.shipment_row_id, sum(l.stuffing_shipment_kg) as stuffing_shipment_kg
										from #stuffing_log_calculation as l
										where not l.stuffing_shipment_kg is null
										group by l.shipment_row_id
									 ) as l
									on o.shipment_row_id = l.shipment_row_id
							
								');

						exec('
								update s
								set s.shipment_from_stuffing_' + @type_stuffing + '_kg = a.shipment_from_stuffing_kg
								from project_plan_production_finished_products.data_import.shipment as s
								join (
										SELECT shipment_stuffing_id_box_row_id, sum(shipment_from_stuffing_' + @type_stuffing + '_kg) as shipment_from_stuffing_kg
										FROM project_plan_production_finished_products.data_import.shipment
										where shipment_stuffing_id_box_type in (2) and not shipment_from_stuffing_' + @type_stuffing + '_kg is null
										group by shipment_stuffing_id_box_row_id
									 ) as a on s.shipment_row_id = a.shipment_stuffing_id_box_row_id
								');



			end;	



end;

