use project_plan_production_finished_products

go

-- exec project_plan_production_finished_products.calc.distribution_stock

ALTER PROCEDURE calc.distribution_stock
as
BEGIN
			SET NOCOUNT ON;
			
			-- подготовка данных
			BEGIN 

					BEGIN  -- ОЧИЩАЕМ ЕСЛИ РАНЬШЕ УЖЕ БЫЛ РАСЧЕТ + ЛОГИ 
					
							update data_import.stock	set stock_shipment_kg = null;		
							update data_import.shipment	set shipment_from_stock_kg = null, shipment_from_stuffing_fact_kg = null, shipment_from_stuffing_plan_kg = null, shipment_from_marking_kg = null;		

							-- ЛОГ
							TRUNCATE TABLE data_import.stock_log_calculation;
							TRUNCATE TABLE data_import.stuffing_fact_log_calculation;
							TRUNCATE TABLE data_import.stuffing_plan_log_calculation;
							TRUNCATE TABLE data_import.marking_log_calculation;
					
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

					end;
					
					BEGIN -- СОЗДАЕМ ГРУППУ АРТИКУЛОВ, ЧТО БЫ НЕ ЗАВИСИТЬ ОТ ПЛОЩАДКИ 

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
										
					BEGIN -- ОСТАТКИ 

							IF OBJECT_ID('tempdb..#stock','U') is not null drop table #stock; 
				
							select convert(int, ROW_NUMBER() over (order by s.stock_sap_id, s.stock_on_date, s.stock_current_KOS, s.stock_kg)) as stock_id
									,s.stock_row_id
									,s.stock_sap_id
									,sg.sap_id_group
									,s.stock_on_date
									,s.stock_current_KOS
									,s.stock_KOS_in_day
									,s.stock_kg
							into #stock
							from data_import.stock as s
							join #sap_id_group as sg on s.stock_sap_id = sg.sap_id
							where s.stock_reason_ignore_in_calculate is null;

							-- индекс
							CREATE NONCLUSTERED INDEX NoCl_stock ON #stock (sap_id_group, stock_id asc, stock_on_date desc)
							include(stock_current_KOS, stock_KOS_in_day); 
					
							CREATE CLUSTERED INDEX Cl_stock_id ON #stock (stock_id);  

					end;		
					
					BEGIN -- ПРИОРИТЕТ ОТГРУЗКИ КАК У НАБИВКИ, МЕЖДУ ВЫХОДАМИ В ПЕРВУЮ ОЧЕРЕДЬ ОТГРУЖАЕМ 1 ПРИОРИТЕТ ИГНОРИРУЯ ДАТУ ОТГРУЗКИ 
							
							BEGIN -- ПОДГОТОВКА ДАННЫХ

									IF OBJECT_ID('tempdb..#for_prioritization','U') is not null drop table #for_prioritization;

									select 
											 ROW_NUMBER() over (order by sh.shipment_sap_id, st.stuffing_available_date) as id
											,sh.shipment_sap_id
											,sh.shipment_stuffing_id
											,sh.count_stuffing_for_sap_id
											,st.stuffing_available_date
											,st.stuffing_before_next_available_date
											,convert(bit, null) as stuffing_true
									into #for_prioritization
									from (
											select distinct sh.shipment_sap_id, sh.shipment_stuffing_id, sh.count_stuffing_for_sap_id
											from (
													select 
														 sh.shipment_sap_id
														,sh.shipment_stuffing_id
														,iif(sh.shipment_stuffing_id_box_type = 2, count(1) over (partition by sh.shipment_stuffing_id_box_row_id), 1) as count_stuffing_for_sap_id
													from data_import.shipment as sh					
													where sh.shipment_delete = 0
													  and sh.shipment_reason_ignore_in_calculate is null 
													  and sh.shipment_stuffing_id_box_type in (0, 2)
												 ) as sh							
										 ) as sh
									left join (
													select distinct st.stuffing_id, st.stuffing_available_date, st.stuffing_before_next_available_date
													from data_import.stuffing_fact as st
											  ) as st on sh.shipment_stuffing_id = st.stuffing_id;


									-- ТАБЛИЦА ДЛЯ ПРОВЕРКИ ДОСТУПНЫХ НАБИВОК
									IF OBJECT_ID('tempdb..#check_prioritization','U') is not null drop table #check_prioritization;

									create table #check_prioritization
									(
										 shipment_sap_id			bigint			not null	
										,shipment_stuffing_id		VARCHAR(40)		not null
										,count_stuffing_for_sap_id	int				not null
										,count_stuffing_available	int					null
									);

									insert into #check_prioritization
									select distinct shipment_sap_id, shipment_stuffing_id, count_stuffing_for_sap_id , 0
									from #for_prioritization;

							END;
							
							BEGIN -- РАСЧЕТ ДОСТУПНЫХ НАБИВОК + КОРОБОК

									declare @id_pr int; set @id_pr = 1;

									while  not (select max(id) from #for_prioritization where id = @id_pr) is null
									begin

											-- stuffing_true = 1 это означает выход набивки, теперь добавляем набивку
											update cp
											set count_stuffing_available = count_stuffing_available + 1
											from #check_prioritization as cp
											where exists (select *
														  from #for_prioritization as fp
														  where cp.shipment_sap_id = fp.shipment_sap_id
															and cp.shipment_stuffing_id = fp.shipment_stuffing_id
															and fp.id = @id_pr);

											if 1 = (  
													select iif(cp.count_stuffing_for_sap_id <= cp.count_stuffing_available, 1, 0) as check_stuffing_available
													from (
															select cp.count_stuffing_for_sap_id, sum(iif(cp.count_stuffing_available > 0, 1, 0)) as count_stuffing_available
															from #check_prioritization as cp
															where cp.shipment_sap_id in (select shipment_sap_id from #for_prioritization where id = @id_pr)
															group by cp.count_stuffing_for_sap_id
														 ) as cp  
													)
											begin

													-- доступные набивки есть, тогда проставляем статус stuffing_true = 1
													update #for_prioritization
													set stuffing_true = 1
													where id = @id_pr;

													-- вычитаем -1 доступная набивка
													update #check_prioritization
													set count_stuffing_available = count_stuffing_available - 1
													where shipment_sap_id in (select shipment_sap_id from #for_prioritization where id = @id_pr);
					
											end;
													
											set @id_pr = @id_pr + 1;

									end;


							END;
							
							BEGIN -- итоговая таблица

									IF OBJECT_ID('tempdb..#prioritization','U') is not null drop table #prioritization;
									
									select 
											 fp.shipment_sap_id
											,fp.stuffing_available_date
											,isnull(   lead(stuffing_available_date)  OVER ( partition by shipment_sap_id order by stuffing_available_date ) - 1, '29990101') as stuffing_before_next_available_date
									into #prioritization
									from (
											select shipment_sap_id, stuffing_available_date
											from #for_prioritization
											where stuffing_true = 1

											union

											select shipment_sap_id, '19000101'
											from #for_prioritization
											where stuffing_true = 1
										 ) as fp
									where not fp.stuffing_available_date is null;

							end;

							IF OBJECT_ID('tempdb..#check_prioritization','U') is not null drop table #check_prioritization;
							IF OBJECT_ID('tempdb..#for_prioritization','U') is not null drop table #for_prioritization;
					END;

					BEGIN -- ПОДГОТОВКА ДАННЫХ ОТГРУЗКА

							-- ОТГРУЗКА
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
										  --,p.stuffing_available_date
										  --,p.stuffing_before_next_available_date
										  --,o.shipment_priority
										  ,DENSE_RANK() over (order by o.shipment_sap_id, p.stuffing_available_date) as sort_1

										  ,iif(p.stuffing_before_next_available_date <> '29990101', o.shipment_priority	, 99999)		as sort_2
										  ,iif(p.stuffing_before_next_available_date <> '29990101', o.shipment_date		,'29990101')	as sort_3
						  
										  ,iif(p.stuffing_before_next_available_date =  '29990101', o.shipment_date		,'29990101')	as sort_4
										  ,iif(p.stuffing_before_next_available_date =  '29990101', o.shipment_priority	,99999)			as sort_5
								  
										  ,o.shipment_min_KOS
										  ,o.shipment_date
										  ,o.shipment_kg

					
									from data_import.shipment as o
									join #sap_id_group as sg on o.shipment_sap_id = sg.sap_id
									join #prioritization as p on o.shipment_sap_id = p.shipment_sap_id and o.shipment_date between p.stuffing_available_date and p.stuffing_before_next_available_date
									where o.shipment_stuffing_id_box_type in (0, 1)
									  and o.shipment_delete = 0
									  and o.shipment_reason_ignore_in_calculate is null
								) as o;

							-- индекс
							CREATE CLUSTERED INDEX Cl_shipment_id ON #shipment (shipment_id); 

					end;

			
			end;



			------------------
			-- расчет остатков
			------------------

			-- переменные для отгрузки
			declare @shipment_id						int;			set @shipment_id = 1;
			declare @shipment_sap_id					bigint; 
			declare @shipment_sap_id_group				smallint; 
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
						set @stock_id = 0;
						while isnull(@shipment_kg, 0) > 0.0
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
											order by s.stock_id
										 ) as s;


									-- ПРОВЕРКА: если остатков нет
									if @stock_id is null BREAK; 


									set @stock_shipment_kg = iif(@shipment_kg > @stock_kg, @stock_kg, @shipment_kg);
															
									-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ ОСТАТКОВ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
									insert into #stock_log_calculation
										  ( shipment_row_id, shipment_date,  shipment_kg,   stock_row_id,  stock_kg,  stock_shipment_kg)	
									values(@shipment_row_id, @shipment_date, @shipment_kg, @stock_row_id, @stock_kg, @stock_shipment_kg);
									
									-- РАСПРЕДЕЛЯЕМ: вычитаем из остатков и потребности
									update #shipment	set shipment_kg		= shipment_kg	- @stock_shipment_kg	where shipment_id = @shipment_id;
									update #stock		set stock_kg		= stock_kg		- @stock_shipment_kg	where stock_id = @stock_id;
														set @shipment_kg	= @shipment_kg	- @stock_shipment_kg;
									
						end;

						-- следующая отгрузка
						set @shipment_id = @shipment_id + 1; 
			end;





			-- добавляем в основную таблицу
			-- СОХРАНЯЕМ ЛОГИ
			insert into data_import.stock_log_calculation
			select * from #stock_log_calculation;

			-- ДОБАВЛЯЕМ ОТГРУЗКИ В ОСТАТКИ
			update s
			set s.stock_shipment_kg = l.stock_shipment_kg
			from data_import.stock as s
			join (
					select l.stock_row_id, sum(l.stock_shipment_kg) as stock_shipment_kg
					from #stock_log_calculation as l
					group by l.stock_row_id
				 ) as l 
				on s.stock_row_id = l.stock_row_id;



			-- ДОБАВЛЯЕМ ОТГРУЗКИ В ПОТРЕБНОСТЬ
			update o
			set o.shipment_from_stock_kg = l.shipment_from_stock_kg
			from data_import.shipment as o
			join (
					select l.shipment_row_id, sum(l.stock_shipment_kg) as shipment_from_stock_kg
					from #stock_log_calculation as l
					group by l.shipment_row_id
				 ) as l
				on o.shipment_row_id = l.shipment_row_id;
			
			-- заполняем коробки
			update s
			set s.shipment_from_stock_kg = ss.shipment_from_stock_kg
			from data_import.shipment as s
			join (
					select
							 ss.shipment_row_id
							,max(ss.shipment_from_stock_kg) over (partition by ss.shipment_stuffing_id_box_row_id) *	
							 ss.shipment_kg / 
							 sum(ss.shipment_kg) over (partition by ss.shipment_stuffing_id_box_row_id, ss.shipment_stuffing_id_box_type) as shipment_from_stock_kg
					from data_import.shipment as ss
					where ss.shipment_stuffing_id_box_type in (1, 2)
				 ) as ss on s.shipment_row_id = ss.shipment_row_id and s.shipment_stuffing_id_box_type in (2);


			-- добавляем данные в общию таблицу, которую выводим на форму
			exec report.for_form
			

end;




							
						


