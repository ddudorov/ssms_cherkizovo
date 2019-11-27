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
							from project_plan_production_finished_products.data_import.shipments_SAP as o
							where o.stuffing_id_box_type in (0, 2) -- 0 берем набики не коробки, 2 коробки уже разбиты на набивки
								and o.shipment_delete = 0
								and o.reason_ignore_in_calculate is null
								and not isnull(o.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')

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
							from project_plan_production_finished_products.data_import.shipments_1C as o
							where o.stuffing_id_box_type in (0, 2) -- 0 берем набики не коробки, 2 коробки уже разбиты на набивки
								and o.reason_ignore_in_calculate is null
								and not isnull(o.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
							
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
							from project_plan_production_finished_products.data_import.shipments_sales_plan as o
							where o.stuffing_id_box_type in (0, 2) -- 0 берем набики не коробки, 2 коробки уже разбиты на набивки
								and o.shipment_delete = 0
								and o.reason_ignore_in_calculate is null
								and not isnull(o.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')

						) as o
					where not iif(@type_stuffing = 'fact', o.stock_net_need_kg, o.stuffing_fact_net_need_kg) is null;

					--delete  
					--from #shipment_union_all
					--where stuffing_id_box_type <> 2
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
							convert(int, ROW_NUMBER() over (order by 
																	 o.stuffing_available_date

																	,o.shipment_priority_before_next
																	,o.individual_marking_id_before_next
																	,o.shipment_kg_before_next desc
																	,o.shipment_date_max_before_next
																	,o.shipment_min_KOS_before_next

							
																	,o.shipment_date_max_after_next
																	,o.shipment_priority_after_next
																	,o.individual_marking_id_after_next
																	,o.shipment_min_KOS_after_next
																	,o.shipment_kg_after_next desc
															)) as shipment_id

							,o.shipment_row_id
							,o.shipment_name_table
							,o.sap_id
							,o.stuffing_id
							,o.stuffing_id_box_type
							,o.stuffing_id_box_row_id
							,o.shipment_date_min
							,o.shipment_date_max
							,o.shipment_kg as shipment_initial_kg
							,o.shipment_kg
					into #shipment
					from (

								select 

											st.stuffing_available_date								
										,st.stuffing_before_next_available_date

										-- before_next
										,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_priority		, 99999			) as shipment_priority_before_next
										,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_date_max		, '29990101'	) as shipment_date_max_before_next
										,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_min_KOS		, 1				) as shipment_min_KOS_before_next
										,iif(st.stuffing_before_next_available_date <> '29990101', o.shipment_kg			, 0				) as shipment_kg_before_next
										,iif(st.stuffing_before_next_available_date <> '29990101'											
																and sp.individual_marking_id <> 1, sp.individual_marking_id	, 99999			) as individual_marking_id_before_next
																																		
																																		
										-- after_next																						
										,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_priority		, 99999			) as shipment_priority_after_next
										,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_date_max		, '29990101'	) as shipment_date_max_after_next									
										,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_min_KOS		, 1				) as shipment_min_KOS_after_next
										,iif(st.stuffing_before_next_available_date =  '29990101', o.shipment_kg			, 0				) as shipment_kg_after_next
										,iif(st.stuffing_before_next_available_date =  '29990101'
																and sp.individual_marking_id <> 1, sp.individual_marking_id , 99999			) as individual_marking_id_after_next
																			
										,o.shipment_row_id
										,o.shipment_name_table
										,o.sap_id
										,o.stuffing_id
										,o.stuffing_id_box_type
										,o.stuffing_id_box_row_id
										,o.shipment_date_min
										,o.shipment_date_max
										,o.shipment_kg as shipment_initial_kg
										,o.shipment_kg
								from #shipment_union_all as o 
								join #stuffing_for_shipment as st 
									on o.stuffing_id = st.stuffing_id
									and o.shipment_date_max between st.stuffing_available_date and st.stuffing_before_next_available_date
								join cherkizovo.info.products_sap as sp on o.sap_id = sp.SAP_id 
						) as o;


					-- создаем индексы
					CREATE NoNCLUSTERED INDEX NoCl_shipment_id ON #shipment (shipment_id)
					INCLUDE(sap_id, stuffing_id, shipment_date_min, shipment_date_max, shipment_kg);

			end;
			
			-- НАБИВКИ С SAP ID
			begin 
					IF OBJECT_ID('tempdb..#stuffing','U') is not null drop table #stuffing;  -- select * from #stuffing_fact
					select 
								sh.sap_id	
							,sh.stuffing_id
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

					-- ЛОГ НАБИВОК
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


		
			--select * from #stuffing_for_shipment
			IF OBJECT_ID('tempdb..#shipment_union_all','U') is not null drop table #shipment_union_all; -- select * from #shipment_union_all where stuffing_id_box_type = 0
			IF OBJECT_ID('tempdb..#stuffing_for_shipment','U') is not null drop table #stuffing_for_shipment; -- select * from #stuffing_for_shipment
			

			-- select * from #shipment order by 1
			--IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment;
			--IF OBJECT_ID('tempdb..#stuffing_log_calculation','U') is not null drop table #stuffing_log_calculation;

			------------------------
			-- РАСПРЕДЕЛЕНИЕ НАБИВОК
			------------------------
			
			-- переменные для отгрузки
			declare @shipment_id						int;			set @shipment_id = 1;
			declare @shipment_sap_id					bigint;			-- log
			declare @shipment_stuffing_id				VARCHAR(40); 
			declare @shipment_stuffing_id_box_type		TINYINT; 
			declare @shipment_stuffing_id_box_row_id	int;	
			declare @shipment_date_min					date; 
			declare @shipment_date_max					date; 
			declare @shipment_kg						dec(11,5);

			declare @shipment_row_id					int;			-- log
			declare @shipment_name_table				varchar(40);	-- log


			while not @shipment_id is null
			begin
						-- заполняем переменные по отгрузке
						select
								 @shipment_id						= max(o.shipment_id)
								,@shipment_sap_id					= max(o.sap_id)
								,@shipment_stuffing_id				= max(o.stuffing_id)
								,@shipment_stuffing_id				= max(o.stuffing_id)
								,@shipment_stuffing_id_box_type		= max(o.stuffing_id_box_type)
								,@shipment_stuffing_id_box_row_id	= max(o.stuffing_id_box_row_id)
								,@shipment_date_min					= max(o.shipment_date_min)
								,@shipment_date_max					= max(o.shipment_date_max)
								,@shipment_kg						= max(o.shipment_kg)

								,@shipment_row_id					= max(o.shipment_row_id)		-- log
								,@shipment_name_table				= max(o.shipment_name_table)	-- log
						from #shipment as o
						where o.shipment_id = @shipment_id;


						-- ==================== --
						-- распределяем набивки --
						-- ==================== --
						while isnull(@shipment_kg, 0) <> 0.0
						begin
										
									-- артикул не состоит из коробок
									if @shipment_stuffing_id_box_type = 0 
									begin

											--выбираем какую набивку использовать --> (внутрик вызывается хранимка которая маркирует набивку --> (внутрик вызывается хранимка которая отгружает))
											exec project_plan_production_finished_products.calc.distribution_stuffing_select_for_no_box  @shipment_id			= @shipment_id			
																																		,@shipment_sap_id		= @shipment_sap_id		-- log
																																		,@shipment_stuffing_id	= @shipment_stuffing_id	
																																		,@shipment_date_min		= @shipment_date_min	
																																		,@shipment_date_max		= @shipment_date_max	
																																		,@shipment_kg			= @shipment_kg			
																																		,@shipment_row_id		= @shipment_row_id		-- log
																																		,@shipment_name_table	= @shipment_name_table	-- log
									end;

									
									-- артикул состоит из коробок (2 или более набивок в одном артикуле)
									if @shipment_stuffing_id_box_type = 2 -- переключить на 2, что бы распределять коробки
									begin

											--выбираем какие набивки использовать --> (внутрик вызывается хранимка которая маркирует набивку --> (внутрик вызывается хранимка которая отгружает))
											exec project_plan_production_finished_products.calc.distribution_stuffing_select_for_box  @stuffing_id_box_row_id	= @shipment_stuffing_id_box_row_id;
									end;
									


									-- ПРОВЕРКА: кол-во отгруженного = 0
									select @shipment_kg = iif(@shipment_kg <> shipment_kg, shipment_kg, 0) -- если отгрузки не было, то пишем 0, что бы выйти из цикла
									from #shipment
									where shipment_id = @shipment_id;

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
									--where st.stuffing_initial_kg 0) 0
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




							
						


