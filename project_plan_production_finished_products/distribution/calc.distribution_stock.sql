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
					update project_plan_production_finished_products.data_import.stock					set stock_shipment_kg = null;
					update project_plan_production_finished_products.data_import.transits				set stock_shipment_kg = null;			
					update project_plan_production_finished_products.data_import.shipments_SAP			set stock_shipment_kg = null, stuffing_fact_shipment_kg = null, stuffing_plan_shipment_kg = null;			
					update project_plan_production_finished_products.data_import.shipments_1C			set stock_shipment_kg = null, stuffing_fact_shipment_kg = null, stuffing_plan_shipment_kg = null;	
					update project_plan_production_finished_products.data_import.shipments_sales_plan	set stock_shipment_kg = null, stuffing_fact_shipment_kg = null, stuffing_plan_shipment_kg = null;	


					-- ЛОГ ОСТАТКОВ
					TRUNCATE TABLE project_plan_production_finished_products.data_import.stock_log_calculation;

					IF OBJECT_ID('tempdb..#stock_log_calculation','U') is not null drop table #stock_log_calculation;

					create table #stock_log_calculation
					( 
							 sort_id				INT				NOT NULL IDENTITY(1,1)
							,stock_row_id			INT					NULL
							,stock_name_table		varchar(40)			NULL
							,shipment_row_id		INT				NOT NULL	
							,shipment_name_table	varchar(40)			NULL	
							,shipment_kg			dec(11,5)		NOT NULL
							,stock_kg				dec(11,5)			NULL	
							,stock_shipment_kg		dec(11,5)			NULL	
							
					);

					
					-- ОСТАТКИ
					IF OBJECT_ID('tempdb..#stock','U') is not null drop table #stock; 
				

					select ROW_NUMBER() over (order by s.sap_id, s.stock_on_date, s.stock_current_KOS, s.stock_kg) as stock_id
							,s.stock_row_id
							,s.stock_name_table
							,s.sap_id
							,s.stock_on_date
							,s.stock_current_KOS
							,s.stock_KOS_in_day
							,s.stock_kg
					into #stock
					from (
							select 
								   s.row_id as stock_row_id
								  ,s.name_table as stock_name_table
								  ,s.sap_id
								  ,s.stock_on_date
								  ,s.stock_current_KOS
								  ,s.stock_KOS_in_day
								  ,s.stock_kg
							from project_plan_production_finished_products.data_import.stock as s
							where s.reason_ignore_in_calculate is null

							union all
				
							select 
								   s.row_id as stock_row_id
								  ,s.name_table as stock_name_table
								  ,s.sap_id
								  ,s.stock_on_date
								  ,s.stock_current_KOS
								  ,s.stock_KOS_in_day
								  ,s.stock_kg
							from project_plan_production_finished_products.data_import.transits as s
							where s.reason_ignore_in_calculate is null
						 ) as s

					-- индекс
					CREATE NONCLUSTERED INDEX NoCl_stock_id ON #stock (stock_id asc)
					include(stock_on_date, stock_current_KOS, stock_KOS_in_day, stock_kg); 
							
							


								

					-- ОТГРУЗКА
					IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment; 

					select ROW_NUMBER() over (order by o.sap_id, o.shipment_date, o.shipment_priority, o.shipment_min_KOS, o.shipment_kg desc) as shipment_id
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
									,o.shipment_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_SAP as o
							where o.stuffing_id_box_type in (0, 1)
							  and o.shipment_delete = 0
							  and o.reason_ignore_in_calculate is null
							  
							union all

							select   o.row_id
									,o.name_table
									,o.sap_id
									,o.shipment_priority
									,o.shipment_min_KOS
									,o.shipment_date
									,o.shipment_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_1C as o
							where o.stuffing_id_box_type in (0, 1)
							  and o.reason_ignore_in_calculate is null

							union all

							select   o.row_id
									,o.name_table
									,o.sap_id
									,o.shipment_priority
									,o.shipment_min_KOS
									,o.shipment_date
									,o.shipment_kg
									,o.reason_ignore_in_calculate
							from project_plan_production_finished_products.data_import.shipments_sales_plan as o
							where o.stuffing_id_box_type in (0, 1)
							  and o.shipment_delete = 0
							  and o.reason_ignore_in_calculate is null




						 ) as o
					where o.sap_id in (select s.sap_id from #stock as s);

					
					-- индекс
					CREATE NONCLUSTERED INDEX NoCl_shipment_id ON #shipment (shipment_id); 

					
			
			end;



			------------------
			-- расчет остатков
			------------------


			-- переменные для отгрузки
			declare @shipment_id			int; set @shipment_id = 1;
			declare @shipment_sap_id		bigint; 
			declare @shipment_date			datetime; 
			declare @shipment_min_KOS		DEC(7,6);
			declare @shipment_kg			dec(11,5);
			declare @shipment_row_id		int;			-- log
			declare @shipment_name_table	varchar(40);	-- log
			
			-- переменные для остатков
			declare @stock_id				int;
			declare @stock_kg				dec(11,5);
			declare @stock_shipment_kg		dec(11,5);
			declare @stock_row_id			int;			-- log
			declare @stock_name_table		varchar(40);	-- log
			




			while not @shipment_id is null
			begin
						-- заполняем переменные по отгрузке
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
						set @stock_id = 0;

						while not @stock_id is null and not @shipment_id is null -- 0 для входа в цикл, если остатки null, то выходим из цикла -- 
						begin
									 
									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #stock_log_calculation
									(		shipment_row_id,  shipment_name_table,  shipment_kg)	
									values(@shipment_row_id, @shipment_name_table, @shipment_kg);


									-- БЕРЕМ ОСТАТКИ
									select
											 @stock_id			= max(s.stock_id)
											,@stock_kg			= max(s.stock_kg)
											,@stock_row_id		= max(s.stock_row_id)
											,@stock_name_table	= max(s.stock_name_table)
									from (
											select top 1 s.stock_id, s.stock_kg, s.stock_row_id, s.stock_name_table
											from #stock as s
											where s.sap_id = @shipment_sap_id
											  and s.stock_kg > 0.0
											  and s.stock_id > @stock_id
											  and s.stock_on_date <= @shipment_date
											  and @shipment_min_KOS < s.stock_current_KOS - (s.stock_KOS_in_day * DATEDIFF(day, s.stock_on_date, @shipment_date))  --ПРОВЕРКА: КОС остатков больше мин КОС на отгрузку
											order by s.stock_id
										 ) as s;


									-- ПРОВЕРКА: если остатков нет
									if @stock_id is null CONTINUE;

	
									set @stock_shipment_kg = iif(@shipment_kg > @stock_kg, @stock_kg, @shipment_kg);
															
									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #stock_log_calculation
									(		stock_row_id,  stock_name_table,  shipment_row_id,  shipment_name_table,  shipment_kg,  stock_kg,  stock_shipment_kg)	
									values(@stock_row_id, @stock_name_table, @shipment_row_id, @shipment_name_table, @shipment_kg, @stock_kg, @stock_shipment_kg);
									
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
			-- логи
			insert into project_plan_production_finished_products.data_import.stock_log_calculation
			select * from #stock_log_calculation;

			-- остатки
			update s
			set s.stock_shipment_kg = l.stock_shipment_kg
			from project_plan_production_finished_products.data_import.stock as s
			join (
					select l.stock_row_id, sum(l.stock_shipment_kg) as stock_shipment_kg
					from #stock_log_calculation as l
					where not stock_shipment_kg is null
					  and l.stock_name_table = 'stock'
					group by l.stock_row_id
				 ) as l 
				on s.row_id = l.stock_row_id;

			update s
			set s.stock_shipment_kg = l.stock_shipment_kg
			from project_plan_production_finished_products.data_import.transits as s
			join (
					select l.stock_row_id, sum(l.stock_shipment_kg) as stock_shipment_kg
					from #stock_log_calculation as l
					where not stock_shipment_kg is null
					  and l.stock_name_table = 'transits'
					group by l.stock_row_id
				 ) as l 
				on s.row_id = l.stock_row_id;


			-- shipments_SAP --------------------------------------------------------------------------------------------------
			update o
			set o.stock_shipment_kg = l.stock_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_SAP as o
			join (
					select l.shipment_row_id, sum(l.stock_shipment_kg) as stock_shipment_kg
					from #stock_log_calculation as l
					where not stock_shipment_kg is null
					  and l.shipment_name_table = 'shipments_SAP'
					group by l.shipment_row_id
				 ) as l
				on o.row_id = l.shipment_row_id;
			
			-- заполняем коробки
			update s
			set s.stock_shipment_kg = ss.stock_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_SAP as s
			join (
					select
							 ss.row_id
							,max(ss.stock_shipment_kg) over (partition by ss.stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.stuffing_id_box_row_id, ss.stuffing_id_box_type) as stock_shipment_kg
					from project_plan_production_finished_products.data_import.shipments_SAP as ss
					where ss.stuffing_id_box_type in (1, 2)
				 ) as ss on s.row_id = ss.row_id and s.stuffing_id_box_type in (2);


				 
			-- shipments_1C --------------------------------------------------------------------------------------------------
			update o
			set o.stock_shipment_kg = l.stock_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_1C as o
			join (
					select l.shipment_row_id, sum(l.stock_shipment_kg) as stock_shipment_kg
					from #stock_log_calculation as l
					where not stock_shipment_kg is null
					  and l.shipment_name_table = 'shipments_1C'
					group by l.shipment_row_id
				 ) as l
				on o.row_id = l.shipment_row_id;
			
			-- заполняем коробки
			update s
			set s.stock_shipment_kg = ss.stock_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_1C as s
			join (
					select
							 ss.row_id
							,max(ss.stock_shipment_kg) over (partition by ss.stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.stuffing_id_box_row_id, ss.stuffing_id_box_type) as stock_shipment_kg
					from project_plan_production_finished_products.data_import.shipments_1C as ss
					where ss.stuffing_id_box_type in (1, 2)
				 ) as ss on s.row_id = ss.row_id and s.stuffing_id_box_type in (2);



				 
			-- shipments_sales_plan --------------------------------------------------------------------------------------------------
			update o
			set o.stock_shipment_kg = l.stock_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_sales_plan as o
			join (
					select l.shipment_row_id, sum(l.stock_shipment_kg) as stock_shipment_kg
					from #stock_log_calculation as l
					where not stock_shipment_kg is null
					  and l.shipment_name_table = 'shipments_sales_plan'
					group by l.shipment_row_id
				 ) as l
				on o.row_id = l.shipment_row_id;
			

			
			-- заполняем коробки
			update s
			set s.stock_shipment_kg = ss.stock_shipment_kg
			from project_plan_production_finished_products.data_import.shipments_sales_plan as s
			join (
					select
							 ss.row_id
							,max(ss.stock_shipment_kg) over (partition by ss.stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.stuffing_id_box_row_id, ss.stuffing_id_box_type) as stock_shipment_kg
					from project_plan_production_finished_products.data_import.shipments_sales_plan as ss
					where ss.stuffing_id_box_type in (1, 2)
				 ) as ss on s.row_id = ss.row_id and s.stuffing_id_box_type in (2);

			-- проверка
			/*
			select sum(stock_kg), sum(stock_shipment_kg),  sum(stock_after_shipment_kg),  sum(stock_kg)- sum(stock_shipment_kg)
			from project_plan_production_finished_products.import_data.stock

			select sum(o.shipment_kg) as shipment_kg, sum(o.stock_shipment_kg) as stock_shipment_kg, sum(o.stock_net_need) as stock_net_need, sum(o.shipment_kg) - sum(o.stock_net_need)
			from (
					select shipment_kg, stock_shipment_kg, stock_net_need from project_plan_production_finished_products.import_data.shipments_SAP
					union all
					select shipment_kg, stock_shipment_kg, stock_net_need from project_plan_production_finished_products.import_data.shipments_1C
					union all
					select shipment_kg, stock_shipment_kg, stock_net_need from project_plan_production_finished_products.import_data.shipments_sales_plan
				 ) as o
			*/
end;




							
						


