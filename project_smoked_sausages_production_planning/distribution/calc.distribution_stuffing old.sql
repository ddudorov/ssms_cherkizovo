use project_plan_production_finished_products

go

-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'fact' 
-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'plan'

ALTER PROCEDURE calc.distribution_stuffing @type_stuffing varchar(15)
as
BEGIN
			declare @type_stuffing varchar(15); set @type_stuffing = 'fact'

			SET NOCOUNT ON;
			--------------------
			-- ПОДГОТОВКА ДАННЫХ
			--------------------

			-- ОЧИЩАЕМ ЕСЛИ РАНЬШЕ УЖЕ БЫЛ РАСЧЕТ	
			begin 

					if @type_stuffing = 'fact'
					begin

							update project_plan_production_finished_products.data_import.shipments_SAP			set stuffing_fact_shipment_kg = null;			
							update project_plan_production_finished_products.data_import.shipments_1C			set stuffing_fact_shipment_kg = null;	
							update project_plan_production_finished_products.data_import.shipments_sales_plan	set stuffing_fact_shipment_kg = null;	

							delete project_plan_production_finished_products.data_import.stuffing_fact			where not sap_id is null;
							update project_plan_production_finished_products.data_import.stuffing_fact			set stuffing_marking_kg = null, stuffing_shipment_kg = null;	

					end;

					update project_plan_production_finished_products.data_import.shipments_SAP			set stuffing_plan_shipment_kg = null;			
					update project_plan_production_finished_products.data_import.shipments_1C			set stuffing_plan_shipment_kg = null;	
					update project_plan_production_finished_products.data_import.shipments_sales_plan	set stuffing_plan_shipment_kg = null;	
					
					delete project_plan_production_finished_products.data_import.stuffing_plan			where not sap_id is null;
					update project_plan_production_finished_products.data_import.stuffing_plan			set stuffing_marking_kg = null, stuffing_shipment_kg = null;

			end;
			
			-- ОТГРУЗКА ОБЪЕДИНЯЕМ ДАННЫЕ ИЗ ТАБЛИЦ
			begin

					IF OBJECT_ID('tempdb..#shipment_union_all','U') is not null drop table #shipment_union_all; 

					select
							 o.row_id as shipment_row_id
							,o.name_table as shipment_name_table
							,o.shipment_priority
							,o.shipment_min_KOS
							,o.sap_id
							,o.stuffing_id
							,o.stuffing_id_box_type
							,o.stuffing_id_box_row_id
							,convert(date, o.shipment_date - o.sap_id_expiration_date_in_days * (1.0 - o.shipment_min_KOS)) as shipment_date_min
							,convert(date, o.shipment_date) as shipment_date_max
							,iif(@type_stuffing = 'fact', o.stock_net_need_kg, o.stuffing_fact_net_need_kg ) as shipment_kg
							,convert(tinyint, iif(not o.reason_ignore_in_calculate is null, 1, 0)) as error_yn
					into #shipment_union_all
					from (

							select   o.name_table
									,o.row_id
									,o.sap_id
									,o.stuffing_id
									,o.stuffing_id_box_type
									,o.stuffing_id_box_row_id
									,o.shipment_priority
									,o.shipment_date
									,o.shipment_min_KOS
									,o.sap_id_expiration_date_in_days
									,o.stock_net_need_kg
									,o.stuffing_fact_net_need_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_SAP as o
							where o.stuffing_id_box_type in (0, 2) -- 0 берем набики не коробки, 2 коробки уже разбиты на набивки
							  and o.shipment_delete = 0

							union all

							select   o.name_table
									,o.row_id
									,o.sap_id
									,o.stuffing_id
									,o.stuffing_id_box_type
									,o.stuffing_id_box_row_id
									,o.shipment_priority
									,o.shipment_date
									,o.shipment_min_KOS
									,o.sap_id_expiration_date_in_days
									,o.stock_net_need_kg
									,o.stuffing_fact_net_need_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_1C as o
							where o.stuffing_id_box_type in (0, 2) -- 0 берем набики не коробки, 2 коробки уже разбиты на набивки
							
							union all

							select   o.name_table
									,o.row_id
									,o.sap_id
									,o.stuffing_id
									,o.stuffing_id_box_type
									,o.stuffing_id_box_row_id
									,o.shipment_priority
									,o.shipment_date
									,o.shipment_min_KOS
									,o.sap_id_expiration_date_in_days
									,o.stock_net_need_kg
									,o.stuffing_fact_net_need_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_sales_plan as o
							where o.stuffing_id_box_type in (0, 2) -- 0 берем набики не коробки, 2 коробки уже разбиты на набивки
							  and o.shipment_delete = 0

						) as o
					where not iif(@type_stuffing = 'fact', o.stock_net_need_kg, o.stuffing_fact_net_need_kg) is null;

			end;
			
			-- НАБИВКИ ДЛЯ ОТГРУЗКИ
			begin
					
					IF OBJECT_ID('tempdb..#stuffing_for_shipment','U') is not null drop table #stuffing_for_shipment;

					-- создаем таблицу
					select top 0 
								 stuffing_row_id
								,name_table as stuffing_name_table
								,stuffing_id
								,convert(date, stuffing_available_date) as stuffing_available_date
								,convert(date, stuffing_before_next_available_date) as stuffing_before_next_available_date
								,stuffing_kg
					into #stuffing_for_shipment
					from project_plan_production_finished_products.data_import.stuffing_fact;
	
					-- заполняем таблицу
					exec('	
							insert into #stuffing_for_shipment
							select 
									 stuffing_row_id
									,name_table as stuffing_name_table
									,stuffing_id
									,stuffing_available_date
									,stuffing_before_next_available_date
									,stuffing_kg
							from project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '
							where sap_id is null;
						 ');
	 
			end;

			-- ОТГРУЗКА ДЛЯ РАСЧЕТОВ
			begin
					
					IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment;

					select 
							convert(int, ROW_NUMBER() over (order by o.stuffing_id
																	,st.stuffing_available_date
																	,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_priority		, null)
																	,iif(st.stuffing_before_next_available_date <> '29990101', shipment_kg				, null) desc
																	,iif(st.stuffing_before_next_available_date <> '29990101' 
																							and sp.individual_marking_id <> 1, sp.individual_marking_id	, null) desc
																	,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_date_max		, null)
																	,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_min_KOS		, null)

																	
																	,iif(st.stuffing_before_next_available_date <> '29990101', null						, o.shipment_date_max)
																	,iif(st.stuffing_before_next_available_date <> '29990101', null						, o.shipment_priority)
																	,iif(st.stuffing_before_next_available_date <> '29990101', null						, o.shipment_min_KOS)
																	,iif(st.stuffing_before_next_available_date <> '29990101', null						, o.shipment_kg) desc

															)) as shipment_id

							--,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_priority, null) as shipment_priority
							--,iif(st.stuffing_before_next_available_date <> '29990101', shipment_kg, null)  as shipment_kg
							--,iif(st.stuffing_before_next_available_date <> '29990101' and sp.individual_marking_id <> 1, sp.individual_marking_id, null)  as individual_marking_id
							--,iif(st.stuffing_before_next_available_date <> '29990101', shipment_date_max, null) as shipment_date_max
							--,iif(st.stuffing_before_next_available_date <> '29990101', shipment_min_KOS, null) as shipment_min_KOS
							--	
							--,o.shipment_priority
							--,sp.individual_marking_id
							--,st.stuffing_before_next_available_date

							,o.shipment_row_id
							,o.shipment_name_table
							,o.sap_id
							,o.stuffing_id
							,o.stuffing_id_box_type
							,o.stuffing_id_box_row_id
							,o.shipment_date_min
							,o.shipment_date_max
							,o.shipment_kg
							,o.error_yn
					into #shipment
					from #shipment_union_all as o 
					join #stuffing_for_shipment as st 
						on o.stuffing_id = st.stuffing_id
						and o.shipment_date_max between st.stuffing_available_date and st.stuffing_before_next_available_date
					join cherkizovo.info.products_sap as sp on o.sap_id = sp.SAP_id; 
					
					-- создаем индексы
					CREATE NoNCLUSTERED INDEX NoCl_shipment_id ON #shipment (shipment_id)
					INCLUDE(sap_id, stuffing_id, shipment_date_min, shipment_date_max, shipment_kg)

			end;
			
			-- НАБИВКИ С SAP ID
			begin 
					IF OBJECT_ID('tempdb..#stuffing','U') is not null drop table #stuffing;  -- select * from #stuffing_fact
					select 
							 sh.sap_id	
							,sh.stuffing_id
							,sh.stuffing_id_box_type
							,st.stuffing_row_id
							,st.stuffing_name_table
							,convert(date, st.stuffing_available_date) as stuffing_available_date
							,convert(date, st.stuffing_before_next_available_date) as stuffing_before_next_available_date
							,ist.minimum_volume_for_marking_kg as stuffing_min_marking_kg
							,ist.step_marking_kg as stuffing_step_marking_kg
							,st.stuffing_kg as stuffing_initial_kg
							,st.stuffing_kg
							,convert(dec(11,5),null) as stuffing_marking_kg  -- если null значит не маркировался, если 0 то маркировался уже sap id
							,convert(dec(11,5),null) as stuffing_shipment_kg -- кол-во которое уже отгружено из набивки включая маркировку
					into #stuffing
					from (
							select distinct sh.sap_id, sh.stuffing_id, sh.stuffing_id_box_type
							from #shipment as sh	
						 ) as sh
					join #stuffing_for_shipment as st on sh.stuffing_id = st.stuffing_id
					join project_plan_production_finished_products.info.stuffing as ist on st.stuffing_id = ist.stuffing_id;

					-- создаем индексы
					CREATE CLUSTERED INDEX Cl_stuffing_id ON #stuffing (sap_id, stuffing_id, stuffing_available_date asc, stuffing_before_next_available_date);

			end;
						
			-- LOG распределение набивки
			begin 

					-- ЛОГ ОСТАТКОВ
					exec('TRUNCATE TABLE project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '_log_calculation;');

					IF OBJECT_ID('tempdb..#stuffing_log_calculation','U') is not null drop table #stuffing_log_calculation;

					create table #stuffing_log_calculation
					( 
							 sort_id				INT				NOT NULL IDENTITY(1,1)
							,shipment_row_id		INT				NOT NULL	
							,shipment_name_table	varchar(40)			NULL	
							,shipment_sap_id		bigint			NOT NULL
							,shipment_kg			dec(11,5)		NOT NULL
							,stuffing_row_id		INT					NULL
							,stuffing_kg			dec(11,5)			NULL	
							,stuffing_marking_kg	dec(11,5)			NULL	
							,stuffing_shipment_kg	dec(11,5)			NULL			
					);

			end;


			--select * from #stuffing
			select * 
			from #shipment
			where stuffing_id_box_type = 2
			order by shipment_id


			--select *
			--from #stuffing_log_calculation


			------------------------
			-- РАСПРЕДЕЛЕНИЕ НАБИВОК
			------------------------
			
			-- переменные для отгрузки
			declare @shipment_id						int;			set @shipment_id = 1;
			declare @shipment_sap_id					bigint; -- log
			declare @shipment_stuffing_id				VARCHAR(40); 
			declare @shipment_stuffing_id_box_row_id	int;	
			declare @shipment_date_min					date; 
			declare @shipment_date_max					date; 
			declare @shipment_kg						dec(11,5);

			declare @shipment_row_id					int;			-- log
			declare @shipment_name_table				varchar(40);	-- log
			declare @shipment_error_yn					tinyint;		-- log

			-- набивки
			declare @stuffing_row_id					int;			-- log
			declare @stuffing_id_box_type				tinyint;		-- коробочка
			declare @stuffing_min_marking_kg			dec(11,5);
			declare @stuffing_step_marking_kg			dec(11,5);
			declare @stuffing_kg						dec(11,5);		-- log
			declare @stuffing_marking_kg				dec(11,5);		-- log
			declare @stuffing_shipment_kg				dec(11,5);		-- log
			
			



			while not @shipment_id is null
			begin
						-- заполняем переменные по отгрузке
						select
								 @shipment_id						= max(o.shipment_id)
								,@shipment_sap_id					= max(o.sap_id)
								,@shipment_stuffing_id				= max(o.stuffing_id)
								,@shipment_stuffing_id_box_row_id	= max(o.stuffing_id_box_row_id)
								,@shipment_date_min					= max(o.shipment_date_min)
								,@shipment_date_max					= max(o.shipment_date_max)
								,@shipment_kg						= max(o.shipment_kg)

								,@shipment_row_id					= max(o.shipment_row_id)		-- log
								,@shipment_name_table				= max(o.shipment_name_table)	-- log
								,@shipment_error_yn					= max(o.error_yn)				-- log
						from #shipment as o
						where o.shipment_id = @shipment_id;


						-- ==================== --
						-- распределяем набивки --
						-- ==================== --
						set @stuffing_row_id = 0
						while not @stuffing_row_id is null and not @shipment_id is null -- @stuffing_row_id = 0 для входа в цикл, если остатки null, то выходим из цикла -- 
						begin
														 
									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ НАБИВКИ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #stuffing_log_calculation (  shipment_row_id,  shipment_name_table,  shipment_sap_id, shipment_kg)	
																	values( @shipment_row_id, @shipment_name_table, @shipment_sap_id, @shipment_kg);

									-- если есть ошибки
									if @shipment_error_yn = 1
									begin
											set @stuffing_row_id = null
											CONTINUE
									end;




									select 
											 @stuffing_row_id			= max(f.stuffing_row_id)
											,@stuffing_min_marking_kg	= max(f.stuffing_min_marking_kg)
											,@stuffing_step_marking_kg	= max(f.stuffing_step_marking_kg)
											,@stuffing_kg				= max(f.stuffing_kg)
											,@stuffing_marking_kg		= max(f.stuffing_marking_kg)
									from (
											select top 1
													 f.stuffing_row_id
													,f.stuffing_name_table
													,f.stuffing_min_marking_kg
													,f.stuffing_step_marking_kg
													,f.stuffing_kg
													,f.stuffing_marking_kg
											from #stuffing as f
											where f.stuffing_id = @shipment_stuffing_id
												and f.sap_id = @shipment_sap_id
												and isnull(f.stuffing_kg, 0) + isnull(f.stuffing_marking_kg, 0) > 0
												and f.stuffing_available_date between @shipment_date_min and @shipment_date_max
											order by f.stuffing_available_date
										 ) as f;

										 
									-- ПРОВЕРКА: есть ли набивка
									if @stuffing_row_id is null
									begin
											CONTINUE
									end;

									-- маркируем набивку
									if isnull(@stuffing_marking_kg, 0) < @shipment_kg and @stuffing_kg > 0
									begin

												-- получаем кол-во для маркировки
												set @stuffing_marking_kg = case	
																				-- НАБИВКА C ШАГОМ МАРКИРОВКИ 			
																				when not @stuffing_step_marking_kg is null then 																					
																					case 
																						when FLOOR(@stuffing_kg / @stuffing_min_marking_kg) = 0 -- остаток набивке меньше шага маркироки -- не должно быть такого, набивка должна маркировкаться полностью
																						then @stuffing_kg

																						when FLOOR(@stuffing_kg / @stuffing_min_marking_kg) - FLOOR((@shipment_kg - isnull(@stuffing_marking_kg, 0)) / @stuffing_min_marking_kg) <= 1	-- когда набивки мало или равна заказу и остается остаток меньше мин маркироки
																						then @stuffing_kg

																						when FLOOR(@stuffing_kg / @stuffing_min_marking_kg) - FLOOR((@shipment_kg - isnull(@stuffing_marking_kg, 0)) / @stuffing_min_marking_kg) >  1	-- когда набивки больше и остаток меньше мин маркироки
																						then @stuffing_min_marking_kg * CEILING((@shipment_kg - isnull(@stuffing_marking_kg, 0)) / @stuffing_min_marking_kg)
																					end		
																					

																				-- НАБИВКА БЕЗ ШАГА МАРКИРОВКИ - НЕ МАРКИРОВАЛАСЬ
																				when @stuffing_marking_kg is null and @stuffing_kg < @stuffing_min_marking_kg then  -- остатко < мин шаг маркировки  - может быть остаток набивки
																					@stuffing_kg

																				when @stuffing_marking_kg is null and @stuffing_kg >= @stuffing_min_marking_kg then  -- остатко >= мин шаг маркировки 
																					case 
																						when @shipment_kg < @stuffing_kg and @shipment_kg <= @stuffing_min_marking_kg	-- отгрузка <= остаток набивки
																						then @stuffing_min_marking_kg

																						when @shipment_kg < @stuffing_kg and @shipment_kg >  @stuffing_min_marking_kg	-- отгрузка <= остаток набивки
																						then @shipment_kg

																						when @shipment_kg >= @stuffing_kg -- отгрузка > остаток набивки
																						then @stuffing_kg
																					end

																				-- НАБИВКА БЕЗ ШАГА МАРКИРОВКИ - МАРКИРОВАЛАСЬ		
																				when not @stuffing_marking_kg is null then 	
																					case 																														
																						when @stuffing_kg + @stuffing_marking_kg >= @shipment_kg -- остаток набивки + маркировка >= отгрузки
																						then @shipment_kg - @stuffing_marking_kg

																						when @stuffing_kg + @stuffing_marking_kg <  @shipment_kg -- если артикул маркировался, и остаток набивки + маркировка <  отгрузки
																						then @stuffing_kg	
																					end		 																		
																			end;

												if isnull(@stuffing_marking_kg, 0) = 0 -- выход если набивка 0
												begin
														set @stuffing_row_id = null;
														CONTINUE;
												end;
												
												-- кол-во для маркировки вычитаем из остатка набивки и добавляем в маркировку
												update #stuffing
												set stuffing_kg			= stuffing_kg - @stuffing_marking_kg
												   ,stuffing_marking_kg = iif(sap_id = @shipment_sap_id, isnull(stuffing_marking_kg, 0) + @stuffing_marking_kg, stuffing_marking_kg)
												where stuffing_row_id = @stuffing_row_id;


												-- записываем сколько всего кг для маркировки
												select @stuffing_kg = stuffing_kg
													  ,@stuffing_marking_kg = stuffing_marking_kg
												from #stuffing
												where sap_id = @shipment_sap_id
												  and stuffing_row_id = @stuffing_row_id;	

									end;
									
									-- записываем кол-во для отгрузки
									set @stuffing_shipment_kg = iif(@stuffing_marking_kg >= @shipment_kg, @shipment_kg, @stuffing_marking_kg)

									-- РАСПРЕДЕЛЯЕМ: отгружаем из маркировки для набивки
									update #stuffing
									set  stuffing_marking_kg	= stuffing_marking_kg				- @stuffing_shipment_kg
										,stuffing_shipment_kg	= isnull(stuffing_shipment_kg, 0)	+ @stuffing_shipment_kg
									where stuffing_row_id = @stuffing_row_id
									  and sap_id = @shipment_sap_id;


									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ НАБИВКИ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #stuffing_log_calculation (  shipment_row_id,  shipment_name_table,  shipment_sap_id,  shipment_kg,  stuffing_row_id,  stuffing_kg,  stuffing_marking_kg,  stuffing_shipment_kg)	
																	values( @shipment_row_id, @shipment_name_table, @shipment_sap_id, @shipment_kg, @stuffing_row_id, @stuffing_kg, @stuffing_marking_kg, @stuffing_shipment_kg)	
																	  

									-- РАСПРЕДЕЛЯЕМ: отгружаем из маркировки для заявки / плана
									update #shipment
									set shipment_kg = shipment_kg - @stuffing_shipment_kg
									where shipment_id = @shipment_id;


									-- ПРОВЕРКА: кол-во отгруженного = 0
									select @shipment_kg = shipment_kg
									from #shipment
									where shipment_id = @shipment_id;

									if @shipment_kg = 0.0
									begin
										set @stuffing_row_id = null
										CONTINUE
									end;

								
						
						end;

						
						set @shipment_id = @shipment_id + 1; 

			end;
			

			-------------------------------------------------------------
			-- РАСКИДЫВАЕМ ОСТАТОК НАБИВОК ГДЕ ШАГ МАРКИРОВКИ ОТСУТСТВУЕТ
			-------------------------------------------------------------
			begin
					if @type_stuffing = 'fact'
					begin

							insert into project_plan_production_finished_products.data_import.stuffing_fact
							(
								 stuffing_row_id	
								,sap_id			
								,stuffing_id							
								,stuffing_production_name				
								,stuffing_production_date_from			
								,stuffing_production_date_to			
								,stuffing_available_date				
								,stuffing_before_next_available_date				
		
								,stuffing_marking_kg					
								,stuffing_shipment_kg		
							)

							SELECT
								 st.stuffing_row_id
								,st.sap_id			
								,s.stuffing_id
								,s.stuffing_production_name				
								,s.stuffing_production_date_from			
								,s.stuffing_production_date_to			
								,s.stuffing_available_date				
								,s.stuffing_before_next_available_date	

								,nullif(st.stuffing_marking_kg, 0) as stuffing_marking_kg
								,st.stuffing_shipment_kg	

							FROM (
									select 
											 st.stuffing_row_id
											,st.sap_id				
											,st.stuffing_initial_kg
											,st.stuffing_marking_kg	+ (stuffing_kg * (stuffing_shipment_kg / sum(stuffing_shipment_kg) over (partition by stuffing_row_id))) as stuffing_marking_kg		
											,st.stuffing_shipment_kg	
									from #stuffing as st
									where st.stuffing_initial_kg <> st.stuffing_kg and isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0) > 0
								 ) as st
							join project_plan_production_finished_products.data_import.stuffing_fact as s on st.stuffing_row_id = s.stuffing_row_id;

					end;


					if @type_stuffing = 'plan'
					begin

							insert into project_plan_production_finished_products.data_import.stuffing_plan
							(
								 stuffing_row_id	
								,sap_id			
								,stuffing_id							
								,stuffing_production_name				
								,stuffing_production_date_from			
								,stuffing_production_date_to			
								,stuffing_available_date				
								,stuffing_before_next_available_date				
		
								,stuffing_marking_kg					
								,stuffing_shipment_kg		
							)

							SELECT
								 st.stuffing_row_id
								,st.sap_id			
								,s.stuffing_id
								,s.stuffing_production_name				
								,s.stuffing_production_date_from			
								,s.stuffing_production_date_to			
								,s.stuffing_available_date				
								,s.stuffing_before_next_available_date	

								,nullif(st.stuffing_marking_kg, 0) as stuffing_marking_kg
								,st.stuffing_shipment_kg	

								-- проверка
								--,st.stuffing_initial_kg
								--,st.stuffing_kg
								--,sum(isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0)) over (partition by st.stuffing_row_id)
								--,st.stuffing_initial_kg - sum(isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0)) over (partition by st.stuffing_row_id)
							FROM (
									select 
											 st.stuffing_row_id
											,st.sap_id				
											,st.stuffing_initial_kg
											,st.stuffing_marking_kg	+ (stuffing_kg * (stuffing_shipment_kg / sum(stuffing_shipment_kg) over (partition by stuffing_row_id))) as stuffing_marking_kg		
											,st.stuffing_shipment_kg	
									from #stuffing as st
									where st.stuffing_initial_kg <> st.stuffing_kg and isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0) > 0

									--		,st.stuffing_marking_kg				
									--		 +					 
									--		 isnull( st.stuffing_kg 
									--				 *
									--				 case 
									--					when st.stuffing_kg > 0 and st.stuffing_step_marking_kg is null and isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0) > 0
									--					then (isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0)) 
									--						 / 
									--						 sum(isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0)) over (partition by st.stuffing_row_id)
									--				 end, 0) as stuffing_marking_kg
									--		,st.stuffing_shipment_kg	
									--from #stuffing as st
									--where st.stuffing_initial_kg <> st.stuffing_kg and isnull(st.stuffing_marking_kg, 0) + isnull(st.stuffing_shipment_kg, 0) > 0
								 ) as st
							join project_plan_production_finished_products.data_import.stuffing_plan as s on st.stuffing_row_id = s.stuffing_row_id;

					end;
								
			end;

			-- ЛОГ НАБИВКИ ДОБАВЛЯЕМ В ОСНОВНУЮ ТАБЛИЦУ
			exec('insert into project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '_log_calculation
				  select *
				  from #stuffing_log_calculation');
				 

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
									select stuffing_row_id, sum(stuffing_marking_kg) as stuffing_marking_kg, sum(stuffing_shipment_kg) as stuffing_shipment_kg
									from project_plan_production_finished_products.data_import.stuffing_' + @type_stuffing + '
									where not sap_id is null and isnull(stuffing_marking_kg, 0) + isnull(stuffing_shipment_kg, 0) > 0
									group by stuffing_row_id
								 ) as st on s.row_id = st.stuffing_row_id

						');

			end;

			----------------------------------------------
			-- ДОБАВЛЯЕМ ИНФОРМАЦИЮ В ТАБЛИЦУ С ОТГРУЗКАМИ
			----------------------------------------------
			begin
						exec('
								update o
								set o.stuffing_' + @type_stuffing + '_shipment_kg = l.stuffing_shipment_kg
								from project_plan_production_finished_products.data_import.shipments_SAP as o
								join (
										select l.shipment_row_id, sum(l.stuffing_shipment_kg) as stuffing_shipment_kg
										from #stuffing_log_calculation as l
										where not l.stuffing_shipment_kg is null
										  and l.shipment_name_table = ''shipments_SAP''
										group by l.shipment_row_id
									 ) as l
									on o.row_id = l.shipment_row_id
							
							');


						exec('
								update o
								set o.stuffing_' + @type_stuffing + '_shipment_kg = l.stuffing_shipment_kg
								from project_plan_production_finished_products.data_import.shipments_1C as o
								join (
										select l.shipment_row_id, sum(l.stuffing_shipment_kg) as stuffing_shipment_kg
										from #stuffing_log_calculation as l
										where not l.stuffing_shipment_kg is null
										  and l.shipment_name_table = ''shipments_1C''
										group by l.shipment_row_id
									 ) as l
									on o.row_id = l.shipment_row_id
							
							');


						exec('
								update o
								set o.stuffing_' + @type_stuffing + '_shipment_kg = l.stuffing_shipment_kg
								from project_plan_production_finished_products.data_import.shipments_sales_plan as o
								join (
										select l.shipment_row_id, sum(l.stuffing_shipment_kg) as stuffing_shipment_kg
										from #stuffing_log_calculation as l
										where not l.stuffing_shipment_kg is null
										  and l.shipment_name_table = ''shipments_sales_plan''
										group by l.shipment_row_id
									 ) as l
									on o.row_id = l.shipment_row_id
							
							');

			end;	


end;




							
						


