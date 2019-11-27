use project_plan_production_finished_products

go

-- exec project_plan_production_finished_products.calc.distribution_marking

ALTER PROCEDURE calc.distribution_marking
as
BEGIN
			SET NOCOUNT ON;
			
			-- подготовка данных
			begin 

					-- ОЧИЩАЕМ ЕСЛИ РАНЬШЕ УЖЕ БЫЛ РАСЧЕТ
					update project_plan_production_finished_products.data_import.marking				set marking_shipment_kg = null;			
					update project_plan_production_finished_products.data_import.shipments_SAP			set marking_shipment_kg = null;			
					update project_plan_production_finished_products.data_import.shipments_1C			set marking_shipment_kg = null;	
					update project_plan_production_finished_products.data_import.shipments_sales_plan	set marking_shipment_kg = null;	

					-- ЛОГ маркировки
					TRUNCATE TABLE project_plan_production_finished_products.data_import.marking_log_calculation;

					IF OBJECT_ID('tempdb..#marking_log_calculation','U') is not null drop table #marking_log_calculation;

					create table #marking_log_calculation
					( 
							 sort_id				INT				NOT NULL IDENTITY(1,1)  
							,marking_row_id			INT					NULL
							,shipment_row_id		INT				NOT NULL	
							,shipment_name_table	varchar(40)			NULL	
							,shipment_kg			dec(11,5)		NOT NULL
							,marking_kg				dec(11,5)			NULL	
							,marking_shipment_kg	dec(11,5)			NULL				
					);


					-- маркировка
					IF OBJECT_ID('tempdb..#marking','U') is not null drop table #marking; 
				
					select ROW_NUMBER() over (order by s.sap_id, s.marking_on_date, s.marking_current_KOS, s.marking_kg) as marking_sort_id
						  ,s.row_id as marking_row_id
						  ,s.sap_id
						  ,s.marking_on_date
						  ,s.marking_current_KOS
						  ,s.marking_KOS_in_day
						  ,s.marking_kg
					into #marking
					from project_plan_production_finished_products.data_import.marking as s
					where s.reason_ignore_in_calculate is null;
					

					-- индекс
					CREATE NONCLUSTERED INDEX NoCl_stock_id ON #marking (marking_sort_id asc)
					include(marking_on_date, marking_current_KOS, marking_KOS_in_day, marking_kg); 
							
							
								

					-- ОТГРУЗКА
					IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment; 

					select ROW_NUMBER() over (order by o.sap_id, o.shipment_date, o.shipment_priority, o.shipment_min_KOS, o.shipment_kg) as shipment_id
						  ,o.name_table as shipment_name_table
						  ,o.row_id as shipment_row_id
						  ,o.sap_id
						  ,o.shipment_min_KOS
						  ,o.shipment_date
						  ,o.shipment_kg
					into #shipment
					from (
							select   o.row_id
									,o.name_table
									,o.sap_id
									,o.shipment_priority
									,o.shipment_min_KOS
									,o.shipment_date
									,o.stock_net_need_kg as shipment_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_SAP as o
							where o.stuffing_id_box_type in (0, 1)
							  and o.shipment_delete = 0
							  and o.reason_ignore_in_calculate is null
							  and not isnull(o.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')

							union all
							
							select   o.row_id
									,o.name_table
									,o.sap_id
									,o.shipment_priority
									,o.shipment_min_KOS
									,o.shipment_date
									,o.stock_net_need_kg as shipment_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_1C as o
							where o.stuffing_id_box_type in (0, 1)
							  and o.reason_ignore_in_calculate is null
							  and not isnull(o.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
							
							union all
							
							select   o.row_id
									,o.name_table
									,o.sap_id
									,o.shipment_priority
									,o.shipment_min_KOS
									,o.shipment_date
									,o.stock_net_need_kg as shipment_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_sales_plan as o
							where o.stuffing_id_box_type in (0, 1)
							  and o.shipment_delete = 0
							  and o.reason_ignore_in_calculate is null
							  and not isnull(o.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')

						 ) as o
					where not o.shipment_kg is null;
					
					-- индекс
					CREATE NONCLUSTERED INDEX NoCl_shipment_id ON #shipment (shipment_id); 
			
			end;



			--------------------
			-- расчет маркировки
			--------------------

			-- переменные для отгрузки
			declare @shipment_id			int; set @shipment_id = 1;
			declare @shipment_sap_id		bigint; 
			declare @shipment_date			datetime; 
			declare @shipment_min_KOS		DEC(7,6)
			declare @shipment_kg			dec(11,5);			
			declare @shipment_row_id		int;			-- for log
			declare @shipment_name_table	varchar(40);	-- for log

			-- переменные для остатков
			declare @marking_sort_id		int;
			declare @marking_row_id			int;			-- for log
			declare @marking_kg				dec(11,5);
			declare @marking_shipment_kg	dec(11,5);
			
			
			while not @shipment_id is null
			begin

						-- заполняем переменные по маркировки
						select
								 @shipment_id			= max(o.shipment_id)
								,@shipment_sap_id		= max(o.sap_id)
								,@shipment_date			= max(o.shipment_date)
								,@shipment_min_KOS		= max(o.shipment_min_KOS)
								,@shipment_kg			= max(o.shipment_kg)
								,@shipment_row_id		= max(o.shipment_row_id)
								,@shipment_name_table	= max(o.shipment_name_table)
						from #shipment as o
						where o.shipment_id = @shipment_id;

						-- ==================== --
						-- распределяем остатки --
						-- ==================== --
						set @marking_sort_id = 0;

						while not @marking_sort_id is null and not @shipment_id is null -- 0 для входа в цикл, если остатки null, то выходим из цикла -- 
						begin


									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #marking_log_calculation
									(		shipment_row_id,  shipment_name_table,  shipment_kg)	
									values(@shipment_row_id, @shipment_name_table, @shipment_kg);

									-- БЕРЕМ ОСТАТКИ
									select
											 @marking_sort_id	= max(s.marking_sort_id)
											,@marking_kg		= max(s.marking_kg)
											,@marking_row_id	= max(s.marking_row_id)
									from (
											select top 1 s.marking_sort_id, s.marking_kg, s.marking_row_id
											from #marking as s
											where s.sap_id = @shipment_sap_id
											  and s.marking_kg > 0.0
											  and s.marking_sort_id > @marking_sort_id
											  and s.marking_on_date <= @shipment_date
											  and @shipment_min_KOS < s.marking_current_KOS - (s.marking_KOS_in_day * DATEDIFF(day, s.marking_on_date, @shipment_date))  --ПРОВЕРКА: КОС остатков больше мин КОС на отгрузку
											order by s.marking_sort_id
										 ) as s;


									-- ПРОВЕРКА: если остатков нет
									if @marking_sort_id is null CONTINUE;
									
									set @marking_shipment_kg = iif(@shipment_kg > @marking_kg, @marking_kg, @shipment_kg);

									
									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ
									insert into #marking_log_calculation
									(		marking_row_id,  shipment_row_id,  shipment_name_table,  shipment_kg,  marking_kg,  marking_shipment_kg)	
									values(@marking_row_id, @shipment_row_id, @shipment_name_table, @shipment_kg, @marking_kg, @marking_shipment_kg);
																		
									-- РАСПРЕДЕЛЯЕМ: если заказали больше чем на остатках, но берем кол-во на остатках или кол-во заказанного
									update #shipment	set shipment_kg		= shipment_kg	- @marking_shipment_kg	where shipment_id = @shipment_id;
									update #marking		set marking_kg		= marking_kg	- @marking_shipment_kg	where marking_sort_id = @marking_sort_id;
														set @shipment_kg	= @shipment_kg	- @marking_shipment_kg;
						

									-- если заказ больше 0, значит не все покрыли 
									if @shipment_kg = 0
									begin
										set @marking_sort_id = null
										CONTINUE
									end;

						end;

						-- следующая отгрузка
						set @shipment_id = @shipment_id + 1; 
			end;





			-- добавляем в основную таблицу
			-- логи маркировка
			insert into project_plan_production_finished_products.data_import.marking_log_calculation
			select * from #marking_log_calculation;

			-- остатки
			update s
			set s.marking_shipment_kg = l.marking_shipment_kg
			from project_plan_production_finished_products.data_import.marking as s
			join (
					select l.marking_row_id, sum(l.marking_shipment_kg) as marking_shipment_kg
					from #marking_log_calculation as l
					where not marking_shipment_kg is null
					group by l.marking_row_id
				 ) as l 
				on s.row_id = l.marking_row_id;


			-- отгрузка
			update o
			set o.marking_shipment_kg = l.marking_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_SAP as o
			join (
					select l.shipment_row_id, sum(l.marking_shipment_kg) as marking_shipment_kg
					from #marking_log_calculation as l
					where not marking_shipment_kg is null
					  and l.shipment_name_table = 'shipments_SAP'
					group by l.shipment_row_id
				 ) as l
				on o.row_id = l.shipment_row_id;
			
			
			-- заполняем коробки
			update s
			set s.marking_shipment_kg = ss.marking_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_SAP as s
			join (
					select
							 ss.row_id
							,max(ss.marking_shipment_kg) over (partition by ss.stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.stuffing_id_box_row_id, ss.stuffing_id_box_type) as marking_shipment_kg
					from project_plan_production_finished_products.data_import.shipments_SAP as ss
					where ss.stuffing_id_box_type in (1, 2)
				 ) as ss on s.row_id = ss.row_id and s.stuffing_id_box_type in (2);




				

			update o
			set o.marking_shipment_kg = l.marking_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_1C as o
			join (
					select l.shipment_row_id, sum(l.marking_shipment_kg) as marking_shipment_kg
					from #marking_log_calculation as l
					where not marking_shipment_kg is null
					  and l.shipment_name_table = 'shipments_1C'
					group by l.shipment_row_id
				 ) as l
				on o.row_id = l.shipment_row_id;
			
			-- заполняем коробки
			update s
			set s.marking_shipment_kg = ss.marking_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_1C as s
			join (
					select
							 ss.row_id
							,max(ss.marking_shipment_kg) over (partition by ss.stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.stuffing_id_box_row_id, ss.stuffing_id_box_type) as marking_shipment_kg
					from project_plan_production_finished_products.data_import.shipments_1C as ss
					where ss.stuffing_id_box_type in (1, 2)
				 ) as ss on s.row_id = ss.row_id and s.stuffing_id_box_type in (2);




			update o
			set o.marking_shipment_kg = l.marking_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_sales_plan as o
			join (
					select l.shipment_row_id, sum(l.marking_shipment_kg) as marking_shipment_kg
					from #marking_log_calculation as l
					where not marking_shipment_kg is null
					  and l.shipment_name_table = 'shipments_sales_plan'
					group by l.shipment_row_id
				 ) as l
				on o.row_id = l.shipment_row_id;
			

			-- заполняем коробки
			update s
			set s.marking_shipment_kg = ss.marking_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_sales_plan as s
			join (
					select
							 ss.row_id
							,max(ss.marking_shipment_kg) over (partition by ss.stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.stuffing_id_box_row_id, ss.stuffing_id_box_type) as marking_shipment_kg
					from project_plan_production_finished_products.data_import.shipments_sales_plan as ss
					where ss.stuffing_id_box_type in (1, 2)
				 ) as ss on s.row_id = ss.row_id and s.stuffing_id_box_type in (2);





end;




							
						


