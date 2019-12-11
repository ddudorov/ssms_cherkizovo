use project_plan_production_finished_products

go

-- exec project_plan_production_finished_products.calc.distribution_stock

ALTER PROCEDURE calc.distribution_stock
as
BEGIN
			SET NOCOUNT ON;
			
			-- подготовка данных
			begin 


					-- ОЧИЩАЕМ ЕСЛИ РАНЬШЕ УЖЕ БЫЛ РАСЧЕТ
					update project_plan_production_finished_products.data_import.stock		set stock_shipment_kg = null;		
					update project_plan_production_finished_products.data_import.shipment	set shipment_from_stock_kg = null, shipment_from_stuffing_fact_kg = null, shipment_from_stuffing_plan_kg = null, shipment_from_marking_kg = null;		

					-- ЛОГ
					TRUNCATE TABLE project_plan_production_finished_products.data_import.stock_log_calculation;
					TRUNCATE TABLE project_plan_production_finished_products.data_import.stuffing_fact_log_calculation;
					TRUNCATE TABLE project_plan_production_finished_products.data_import.stuffing_plan_log_calculation;
					TRUNCATE TABLE project_plan_production_finished_products.data_import.marking_log_calculation;
					
					IF OBJECT_ID('tempdb..#stock_log_calculation','U') is not null drop table #stock_log_calculation;

					create table #stock_log_calculation
					( 
							 sort_id				INT				NOT NULL IDENTITY(1,1)  
							,shipment_row_id		INT				NOT NULL		
							,shipment_date			datetime			NULL	
							,shipment_kg			dec(11,5)		NOT NULL
							,stock_row_id			INT					NULL
							,stock_kg				dec(11,5)			NULL	
							,stock_shipment_kg		dec(11,5)			NULL			
					);



					-- СОЗДАЕМ ГРУППУ АРТИКУЛОВ, ЧТО БЫ НЕ ЗАВИСИТЬ ОТ ПЛОЩАДКИ
					IF OBJECT_ID('tempdb..#sap_id_group','U') is not null drop table #sap_id_group;
					select 
						 sp.sap_id
						,DENSE_RANK() over (order by isnull(p.sap_id_group_name, sp.product_clean_full_name), sp.individual_marking_id) as sap_id_group
						,sp.production_attribute
						,isnull(p.sap_id_group_name, sp.product_clean_full_name) as product_clean_full_name
						,sp.individual_marking_id
					into #sap_id_group
					from cherkizovo.info.products_sap as sp
					left join project_plan_production_finished_products.info.finished_products_sap_id_manual as p on sp.sap_id = p.sap_id;


					
					-- ОСТАТКИ
					IF OBJECT_ID('tempdb..#stock','U') is not null drop table #stock; 
				
					select convert(int, ROW_NUMBER() over (order by s.stock_sap_id, s.stock_on_date, s.stock_current_KOS, s.stock_kg)) as stock_id
							,s.stock_row_id
							,s.stock_sap_id
							,sg.sap_id_group
							,sg.production_attribute
							,s.stock_on_date
							,s.stock_current_KOS
							,s.stock_KOS_in_day
							,s.stock_kg
					into #stock
					from project_plan_production_finished_products.data_import.stock as s
					join #sap_id_group as sg on s.stock_sap_id = sg.sap_id
					where s.stock_reason_ignore_in_calculate is null;


					-- индекс
					CREATE NONCLUSTERED INDEX NoCl_stock ON #stock (sap_id_group, stock_id asc, stock_on_date desc)
					include(stock_current_KOS, stock_KOS_in_day, production_attribute); 
					
					CREATE CLUSTERED INDEX Cl_stock_id ON #stock (stock_id);  
							

					-- ОТГРУЗКА
					IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment; 

					select convert(int,   ROW_NUMBER() over (order by o.shipment_sap_id, o.shipment_date, o.shipment_priority, o.shipment_min_KOS, o.shipment_kg desc)   ) as shipment_id
						  ,o.shipment_row_id
						  ,o.sap_id
						  ,sg.sap_id_group
						  ,sg.production_attribute
						  ,o.shipment_min_KOS
						  ,o.shipment_date
						  ,o.shipment_kg
					into #shipment
					from project_plan_production_finished_products.data_import.shipment as o
					join #sap_id_group as sg on o.shipment_sap_id = sg.sap_id
					where o.shipment_stuffing_id_box_type in (0, 1)
					  and o.shipment_delete = 0
					  and o.shipment_reason_ignore_in_calculate is null;
					--where o.sap_id in (select s.sap_id from #stock as s)

					
					-- индекс
					--CREATE NONCLUSTERED INDEX NoCl_shipment_id ON #shipment (shipment_id); 
					CREATE CLUSTERED INDEX Cl_shipment_id ON #shipment (shipment_id); 

					
			
			end;



			------------------
			-- расчет остатков
			------------------

			-- переменные для отгрузки
			declare @shipment_id						int;			set @shipment_id = 1;
			declare @shipment_sap_id					bigint; 
			declare @shipment_sap_id_group				smallint; 
			declare @shipment_production_attribute		varchar(4);
			declare @shipment_date						datetime;		-- log
			declare @shipment_min_KOS					DEC(7,6);
			declare @shipment_kg						dec(11,5);
			declare @shipment_row_id					int;			-- log
			
			-- переменные для остатков
			declare @stock_id							int;
			declare @stock_row_id						int;			-- log
			declare @stock_kg							dec(11,5);
			declare @stock_shipment_kg					dec(11,5);
			




			while not @shipment_id is null
			begin
						-- заполняем переменные по отгрузке
						select
								 @shipment_id						= max(o.shipment_id)
								,@shipment_sap_id					= max(o.sap_id)
								,@shipment_sap_id_group				= max(o.sap_id_group)
								,@shipment_production_attribute		= max(o.production_attribute)
								,@shipment_date						= max(o.shipment_date)
								,@shipment_min_KOS					= max(o.shipment_min_KOS)
								,@shipment_kg						= max(o.shipment_kg)
								,@shipment_row_id					= max(o.shipment_row_id)
						from #shipment as o
						where o.shipment_id = @shipment_id;


						-- ==================== --
						-- распределяем остатки --
						-- ==================== --
						set @stock_id = 0;

						while not @stock_id is null and not @shipment_id is null -- 0 для входа в цикл, если остатки null, то выходим из цикла -- 
						begin
									 
									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #stock_log_calculation
									(		shipment_row_id,  shipment_date,  shipment_kg)	
									values(@shipment_row_id, @shipment_date, @shipment_kg);


									-- БЕРЕМ ОСТАТКИ
									select
											 @stock_id			= max(s.stock_id)
											,@stock_kg			= max(s.stock_kg)
											,@stock_row_id		= max(s.stock_row_id)
									from (
											select top 1 s.stock_id, s.stock_kg, s.stock_row_id
											from #stock as s
											where s.stock_kg > 0.0
											  and s.sap_id_group = @shipment_sap_id_group		
											  and s.stock_id > @stock_id
											  and s.stock_on_date <= @shipment_date
											  and @shipment_min_KOS < s.stock_current_KOS - (s.stock_KOS_in_day * DATEDIFF(day, s.stock_on_date, @shipment_date))  --ПРОВЕРКА: КОС остатков больше мин КОС на отгрузку
											/*
											order by s.stock_id
												 ,case @shipment_production_attribute
														when 'П1' then 
																		case s.production_attribute 
																				when 'П4' then 1
																				when 'П7' then 2
																				when 'П1' then 3
																		 end
														when 'П4' then 
																		case s.production_attribute 
																				when 'П4' then 4
																				when 'П7' then 5
																				when 'П1' then 6
																		 end
																		 
														when 'П7' then 
																		case s.production_attribute 
																				when 'П4' then 7
																				when 'П1' then 8
																				when 'П7' then 9
																		 end
												  end
															--П1	ОАО ЧМПЗ Москва
															--П4	ОАО ЧМПЗ Калининград 
															--П7	ЗАО Черкизово-Кашира
											*/
										 ) as s;


									-- ПРОВЕРКА: если остатков нет
									if @stock_id is null CONTINUE;

									set @stock_shipment_kg = iif(@shipment_kg > @stock_kg, @stock_kg, @shipment_kg);
															
									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #stock_log_calculation
										  ( shipment_row_id, shipment_date,  shipment_kg,   stock_row_id,  stock_kg,  stock_shipment_kg)	
									values(@shipment_row_id, @shipment_date, @shipment_kg, @stock_row_id, @stock_kg, @stock_shipment_kg);
									
									-- РАСПРЕДЕЛЯЕМ: если заказали больше чем на остатках, но берем кол-во на остатках или кол-во заказанного	
									update #shipment	set shipment_kg		= shipment_kg	- @stock_shipment_kg	where shipment_id = @shipment_id;
									update #stock		set stock_kg		= stock_kg		- @stock_shipment_kg	where stock_id = @stock_id;
														set @shipment_kg	= @shipment_kg	- @stock_shipment_kg;
										
						
									-- если заказ больше 0, значит не все покрыли 
									if @shipment_kg = 0
									begin
										set @stock_id = null
										CONTINUE
									end;

						end;

						-- следующая отгрузка
						set @shipment_id = @shipment_id + 1; 
			end;





			-- добавляем в основную таблицу
			-- СОХРАНЯЕМ ЛОГИ
			insert into project_plan_production_finished_products.data_import.stock_log_calculation
			select * from #stock_log_calculation;

			-- ДОБАВЛЯЕМ ОТГРУЗКИ В ОСТАТКИ
			update s
			set s.stock_shipment_kg = l.stock_shipment_kg
			from project_plan_production_finished_products.data_import.stock as s
			join (
					select l.stock_row_id, sum(l.stock_shipment_kg) as stock_shipment_kg
					from #stock_log_calculation as l
					group by l.stock_row_id
				 ) as l 
				on s.stock_row_id = l.stock_row_id;



			-- ДОБАВЛЯЕМ ОТГРУЗКИ В ПОТРЕБНОСТЬ
			update o
			set o.shipment_from_stock_kg = l.shipment_from_stock_kg
			from project_plan_production_finished_products.data_import.shipment as o
			join (
					select l.shipment_row_id, sum(l.stock_shipment_kg) as shipment_from_stock_kg
					from #stock_log_calculation as l
					group by l.shipment_row_id
				 ) as l
				on o.shipment_row_id = l.shipment_row_id;
			
			-- заполняем коробки
			update s
			set s.shipment_from_stock_kg = ss.shipment_from_stock_kg
			from project_plan_production_finished_products.data_import.shipment as s
			join (
					select
							 ss.shipment_row_id
							,max(ss.shipment_from_stock_kg) over (partition by ss.shipment_stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.shipment_stuffing_id_box_row_id, ss.shipment_stuffing_id_box_type) as shipment_from_stock_kg
					from project_plan_production_finished_products.data_import.shipment as ss
					where ss.shipment_stuffing_id_box_type in (1, 2)
				 ) as ss on s.shipment_row_id = ss.shipment_row_id and s.shipment_stuffing_id_box_type in (2);


end;




							
						


