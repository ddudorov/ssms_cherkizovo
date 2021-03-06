﻿use project_plan_production_finished_products

go

-- exec .calc.distribution_marking

ALTER PROCEDURE calc.distribution_marking
as
BEGIN
			SET NOCOUNT ON;
			
			-- ПОДГОТОВКА ДАННЫХ
			begin 

					-- ОЧИЩАЕМ ЕСЛИ РАНЬШЕ УЖЕ БЫЛ РАСЧЕТ
					update .data_import.marking		set marking_shipment_kg = null;			
					update .data_import.shipment	set shipment_from_marking_kg = null;	

					begin -- СОЗДАЕМ ГРУППУ АРТИКУЛОВ, ЧТО БЫ НЕ ЗАВИСИТЬ ОТ ПЛОЩАДКИ 

							IF OBJECT_ID('tempdb..#sap_id_group','U') is not null drop table #sap_id_group;

							select 
								 sp.sap_id
								,DENSE_RANK() over (order by isnull(sp.product_name_analog, sp.product_clean_full_name), sp.individual_marking_id) as sap_id_group		
								--,sp.product_name_analog
								--,sp.product_clean_full_name
								--,sp.individual_marking_id														
							into #sap_id_group
							from info_view.sap_id as sp
							where sp.sap_id_type = 'Основной';


					end;


					begin -- МАРКИРОВКА 

							IF OBJECT_ID('tempdb..#marking','U') is not null drop table #marking; 
				
							select convert(int,   ROW_NUMBER() over (order by s.marking_sap_id, s.marking_on_date, s.marking_current_KOS, s.marking_kg)   ) as marking_id
								  ,s.marking_row_id
								  ,s.marking_sap_id
								  ,sg.sap_id_group
								  ,s.marking_on_date
								  ,s.marking_current_KOS
								  ,s.marking_KOS_in_day
								  ,s.marking_kg
							into #marking
							from .data_import.marking as s
							join #sap_id_group as sg on s.marking_sap_id = sg.sap_id
							where s.marking_reason_ignore_in_calculate is null;
							
							CREATE CLUSTERED INDEX Cl_marking_id ON #marking ( marking_id);  
					end;	

					
					begin -- ПОТРЕБНОСТЬ 


							IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment; 

							select 
									convert(int,   ROW_NUMBER() over (order by o.sort_1, o.sort_2, o.sort_3, o.sort_4, o.sort_5, o.shipment_min_KOS)   ) as shipment_id
									,o.shipment_row_id
									,o.shipment_sap_id
									,o.sap_id_group
									,o.shipment_min_KOS
									,o.shipment_date
									,o.shipment_kg
							into #shipment
							from (

									select   
											 o.shipment_row_id
											,o.shipment_sap_id
											,sg.sap_id_group
									
											,DENSE_RANK() over (order by o.shipment_sap_id, m.marking_on_date) as sort_1

											,iif(m.marking_next_available_date <> '29990101', o.shipment_priority	, 99999)		as sort_2
											,iif(m.marking_next_available_date <> '29990101', o.shipment_date		,'29990101')	as sort_3
						  
											,iif(m.marking_next_available_date =  '29990101', o.shipment_date		,'29990101')	as sort_4
											,iif(m.marking_next_available_date =  '29990101', o.shipment_priority	,99999)			as sort_5
											
											,o.shipment_min_KOS
											,o.shipment_date
											,o.shipment_after_stock_kg as shipment_kg

									from .data_import.shipment as o
									join #sap_id_group as sg on o.shipment_sap_id = sg.sap_id
									left join (
													select 
															 marking_sap_id
															,marking_on_date
															,isnull(LEAD(marking_on_date) OVER (PARTITION BY marking_sap_id ORDER BY marking_on_date)  - 1 , '29990101') AS marking_next_available_date
													from .data_import.marking
													group by  marking_sap_id, marking_on_date
											  ) as m on o.shipment_sap_id = m.marking_sap_id and o.shipment_date between m.marking_on_date and m.marking_next_available_date
									where o.shipment_stuffing_id_box_type in (0, 1)
										and o.shipment_delete = 0
										and not o.shipment_after_stock_kg is null
										and o.shipment_reason_ignore_in_calculate is null
										and not isnull(o.shipment_product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
								) as o;



							-- индекс
							CREATE CLUSTERED INDEX Cl_shipment_id ON #shipment (shipment_id); 
					
					end;		
						
					
					begin -- ЛОГ 
					
							TRUNCATE TABLE .data_import.marking_log_calculation;

							IF OBJECT_ID('tempdb..#marking_log_calculation','U') is not null drop table #marking_log_calculation;

							create table #marking_log_calculation
							( 
									 sort_id				INT				NOT NULL IDENTITY(1,1)  
									,shipment_row_id		INT				NOT NULL		
									,shipment_date			datetime			NULL	
									,shipment_kg			dec(11,5)		NOT NULL
									,marking_row_id			INT					NULL
									,marking_kg				dec(11,5)			NULL	
									,marking_shipment_kg	dec(11,5)			NULL		
							);	

					end;	
					
			end;


			--------------------
			-- расчет маркировки
			--------------------

			-- переменные для отгрузки
			declare @shipment_id			int;			set @shipment_id = 1;
			declare @shipment_sap_id		bigint; 
			declare @shipment_sap_id_group	smallint; 
			declare @shipment_date			datetime;		-- log
			declare @shipment_min_KOS		DEC(7,6)
			declare @shipment_kg			dec(11,5);			
			declare @shipment_row_id		int;			-- for log

			-- переменные для остатков
			declare @marking_id				int;
			declare @marking_row_id			int;			-- for log
			declare @marking_kg				dec(11,5);
			declare @marking_shipment_kg	dec(11,5);
			
			
			while not @shipment_id is null
			begin

						-- заполняем переменные по маркировки
						select
								 @shipment_id						= max(o.shipment_id)
								,@shipment_sap_id					= max(o.shipment_sap_id)
								,@shipment_sap_id_group				= max(o.sap_id_group)
								,@shipment_date						= max(o.shipment_date)
								,@shipment_min_KOS					= max(o.shipment_min_KOS)
								,@shipment_kg						= max(o.shipment_kg)
								,@shipment_row_id					= max(o.shipment_row_id)
						from #shipment as o
						where o.shipment_id = @shipment_id;

						-- ==================== --
						-- распределяем остатки --
						-- ==================== --
						set @marking_id = 0;
						while isnull(@shipment_kg, 0) > 0 
						begin

									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #marking_log_calculation
									(		shipment_row_id,  shipment_date,  shipment_kg)	
									values(@shipment_row_id, @shipment_date, @shipment_kg);

									-- БЕРЕМ ОСТАТКИ
									select
											 @marking_id		= max(s.marking_id)
											,@marking_kg		= max(s.marking_kg)
											,@marking_row_id	= max(s.marking_row_id)
									from (
											select top 1 s.marking_id, s.marking_kg, s.marking_row_id
											from #marking as s
											where s.sap_id_group = @shipment_sap_id_group	
											  --and s.marking_sap_id = @shipment_sap_id
											  and s.marking_kg > 0.0
											  and s.marking_id > @marking_id
											  and s.marking_on_date <= @shipment_date
											  and @shipment_min_KOS < s.marking_current_KOS - (s.marking_KOS_in_day * DATEDIFF(day, s.marking_on_date, @shipment_date))  --ПРОВЕРКА: КОС остатков больше мин КОС на отгрузку
											order by s.marking_id
										 ) as s;


									-- ПРОВЕРКА: если остатков нет
									if @marking_id is null BREAK;
									
									set @marking_shipment_kg = iif(@shipment_kg > @marking_kg, @marking_kg, @shipment_kg);


									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ
									insert into #marking_log_calculation
										  ( shipment_row_id,  shipment_date,  shipment_kg,  marking_row_id,  marking_kg,  marking_shipment_kg)	
									values(@shipment_row_id, @shipment_date, @shipment_kg, @marking_row_id, @marking_kg, @marking_shipment_kg);
												
									-- РАСПРЕДЕЛЯЕМ: вычитаем из остатков и потребности 
									update #shipment	set shipment_kg		= shipment_kg	- @marking_shipment_kg	where shipment_id = @shipment_id;
									update #marking		set marking_kg		= marking_kg	- @marking_shipment_kg	where marking_id = @marking_id;
														set @shipment_kg	= @shipment_kg	- @marking_shipment_kg;
						
						end;

						-- следующая отгрузка
						set @shipment_id = @shipment_id + 1; 
			end;





			-- добавляем в основную таблицу
			-- логи маркировка
			insert into .data_import.marking_log_calculation
			select * from #marking_log_calculation;

			-- маркировка
			update s
			set s.marking_shipment_kg = l.marking_shipment_kg
			from .data_import.marking as s
			join (
					select l.marking_row_id, sum(l.marking_shipment_kg) as marking_shipment_kg
					from #marking_log_calculation as l
					where not marking_shipment_kg is null
					group by l.marking_row_id
				 ) as l 
				on s.marking_row_id = l.marking_row_id;


			-- отгрузка
			update o
			set o.shipment_from_marking_kg = l.shipment_from_marking_kg
			from .data_import.shipment as o
			join (
					select l.shipment_row_id, sum(l.marking_shipment_kg) as shipment_from_marking_kg
					from #marking_log_calculation as l
					where not marking_shipment_kg is null
					group by l.shipment_row_id
				 ) as l
				on o.shipment_row_id = l.shipment_row_id;
			
			-- добавляем данные в общию таблицу, которую выводим на форму
			exec report.for_form

			-- не имеет смысла заполнять коробки разбитые на набивки, так как они не используются


end;




							
						


