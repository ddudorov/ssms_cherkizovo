use project_plan_production_finished_products

go


ALTER PROCEDURE calc.distribution_stuffing_select_for_box @shipment_stuffing_id_box_row_id	int
														 ,@shipment_kg_output 				dec(11,5) OUTPUT   			
as
BEGIN

			SET NOCOUNT ON;

			-----------------------------------------------------------------------------------------
			-- ЕСЛИ КОРОБКА, НУЖНО ОПРЕДЕЛИТЬ КАКИЕ НАБИВКИ БРАТЬ, С МИН РАСХОЖДЕНИЯМИ В ДНЯХ ВЫХОДА
			-----------------------------------------------------------------------------------------

			-- select * from #shipment where not shipment_stuffing_id_box_row_id is null
			-- Выгружаем потребность для коробок 
			begin 

					IF OBJECT_ID('tempdb..#shipment_check_marking','U') is not null drop table #shipment_check_marking;
		
					select 
							 ROW_NUMBER() over(order by sh.shipment_id) as id
							,sh.shipment_id
							,sh.shipment_row_id
							,sh.shipment_sap_id
							,sh.shipment_stuffing_id
							,sh.shipment_date_min
							,sh.shipment_date_max
							,sh.shipment_kg
					into #shipment_check_marking
					from #shipment as sh
					where sh.shipment_stuffing_id_box_row_id = @shipment_stuffing_id_box_row_id; -- 1305203 --1377242 
					
					-- ПИШЕМ ЛОГИ РАСПРЕДЕЛЕНИЯ НАБИВКИ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ | ПОТРЕБНОСТЬ
					insert into #stuffing_log_calculation 
					(	   shipment_row_id, stuffing_sap_id, shipment_date,		shipment_kg)	
					select shipment_row_id, shipment_sap_id, shipment_date_max,	shipment_kg from #shipment_check_marking;

			end;


			-- выбираем набивки которые нам пдойдут
			begin  
			
					IF OBJECT_ID('tempdb..#union_data','U') is not null drop table #union_data;
					-- select * from #union_data
					select 
							 st.stuffing_id
							,st.stuffing_row_id
							,st.stuffing_available_date
							,st.stuffing_min_marking_kg
							,st.stuffing_step_marking_kg
							,st.stuffing_kg
							,st.stuffing_marking_kg
							,sh.shipment_id
							,sh.shipment_row_id
							,sh.shipment_sap_id
							,sh.shipment_date_max
							,sh.shipment_kg
					into #union_data
					from #stuffing as st
					join #shipment_check_marking as sh on st.stuffing_sap_id = sh.shipment_sap_id 
													  and st.stuffing_id = sh.shipment_stuffing_id 
													  and st.stuffing_available_date between sh.shipment_date_min and sh.shipment_date_max
													  and st.stuffing_kg + isnull(st.stuffing_marking_kg, 0) >= sh.shipment_kg
					where st.stuffing_kg + isnull(st.stuffing_marking_kg, 0) > 0;
					
			end;


			-- создаем для каждой набивки отдельную таблицу считаем дни
			begin

					IF OBJECT_ID('tempdb..#analysis','U')   is not null drop table #analysis;

					select top 1 *
					into #analysis
					from (
							select *	
									,case 
										when s.count_stuffing = 3 and not s.stuffing_available_date_1 is null and not s.stuffing_available_date_2 is null and not s.stuffing_available_date_3 is null
										then (abs( DATEDIFF(day, s.stuffing_available_date_1 , s.stuffing_available_date_2)) +
												abs( DATEDIFF(day, s.stuffing_available_date_2 , s.stuffing_available_date_3)) + 
												abs( DATEDIFF(day, s.stuffing_available_date_3 , s.stuffing_available_date_1))) / 3

										when s.count_stuffing = 2 and not s.stuffing_available_date_1 is null and not s.stuffing_available_date_2 is null
										then abs( DATEDIFF(day, s.stuffing_available_date_1 , s.stuffing_available_date_2))

									end as avg_between_in_days
								
									,datediff(day, case 
														when s.count_stuffing = 3 and not s.stuffing_available_date_1 is null and not s.stuffing_available_date_2 is null and not s.stuffing_available_date_3 is null
														then convert(datetime,   floor(   convert(int,   (s.stuffing_available_date_1 + s.stuffing_available_date_2 + s.stuffing_available_date_3)) / 3   )   )

														when s.count_stuffing = 2 and not s.stuffing_available_date_1 is null and not s.stuffing_available_date_2 is null
														then convert(datetime,   floor(   convert(int,   (s.stuffing_available_date_1 + s.stuffing_available_date_2							     )) / 2   )   )
													end, s.shipment_date_max) as between_available_and_shipment
								
							from (
									select 
											 s1.shipment_sap_id
											,s1.shipment_date_max
											,s1.shipment_kg
											,(select count(1) from #shipment_check_marking) as count_stuffing

											,s1.shipment_id						as shipment_id_1					,s2.shipment_id					as shipment_id_2					,s3.shipment_id					as shipment_id_3		
											,s1.shipment_row_id					as shipment_row_id_1				,s2.shipment_row_id				as shipment_row_id_2				,s3.shipment_row_id				as shipment_row_id_3
											,s1.stuffing_available_date			as stuffing_available_date_1		,s2.stuffing_available_date		as stuffing_available_date_2		,s3.stuffing_available_date		as stuffing_available_date_3
											,s1.stuffing_id						as stuffing_id_1					,s2.stuffing_id					as stuffing_id_2					,s3.stuffing_id					as stuffing_id_3
											,s1.stuffing_row_id					as stuffing_row_id_1				,s2.stuffing_row_id				as stuffing_row_id_2				,s3.stuffing_row_id				as stuffing_row_id_3
											,s1.stuffing_min_marking_kg			as stuffing_min_marking_kg_1		,s2.stuffing_min_marking_kg		as stuffing_min_marking_kg_2		,s3.stuffing_min_marking_kg		as stuffing_min_marking_kg_3
											,s1.stuffing_step_marking_kg		as stuffing_step_marking_kg_1		,s2.stuffing_step_marking_kg	as stuffing_step_marking_kg_2		,s3.stuffing_step_marking_kg	as stuffing_step_marking_kg_3
											,s1.stuffing_kg						as stuffing_kg_1					,s2.stuffing_kg					as stuffing_kg_2					,s3.stuffing_kg					as stuffing_kg_3
											,s1.stuffing_marking_kg				as stuffing_marking_kg_1			,s2.stuffing_marking_kg			as stuffing_marking_kg_2			,s3.stuffing_marking_kg			as stuffing_marking_kg_3
									
									from (select * from #union_data where stuffing_id in (select shipment_stuffing_id from #shipment_check_marking where id = 1)) as s1  
									full -- cross join не работает когда таблицы 0 строк
									join (select * from #union_data where stuffing_id in (select shipment_stuffing_id from #shipment_check_marking where id = 2)) as s2 on 1 = 1
									full -- cross join не работает когда таблицы 0 строк
									join (select * from #union_data where stuffing_id in (select shipment_stuffing_id from #shipment_check_marking where id = 3)) as s3 on 1 = 1 
							
								 ) as s

						) as s
					where isnull(s.avg_between_in_days, 99) < 8 
					  and isnull(s.between_available_and_shipment, 99) < 30
					  and ((s.count_stuffing = 2 and isnull(s.stuffing_marking_kg_1, 0) = isnull(s.stuffing_marking_kg_2, 0)) or
						   (s.count_stuffing = 3 and isnull(s.stuffing_marking_kg_1, 0) = isnull(s.stuffing_marking_kg_2, 0) and isnull(s.stuffing_marking_kg_1, 0) = isnull(s.stuffing_marking_kg_3, 0)))
					order by (s.avg_between_in_days + s.between_available_and_shipment) / 2.0;


					IF OBJECT_ID('tempdb..#union_data','U') is not null drop table #union_data;

			end;



			-- проверяем 
			begin

					if (select count(1) from #analysis) <> 1
					begin
							update #shipment
							set shipment_kg = 0
							where shipment_id in (select shipment_id from #shipment_check_marking);

							set @shipment_kg_output = 0

							return(0);
					end;

			end;


 
			begin -- отправляем на маркировку и отгрузку 
	
					declare @shipment_id				int
					declare @shipment_sap_id			bigint			-- log
					declare	@shipment_date_max			datetime
					declare @shipment_kg				dec(11,5)
					declare @shipment_row_id			int				-- log
					
					declare @stuffing_row_id			int			
					declare @stuffing_min_marking_kg	dec(11,5)
					declare @stuffing_step_marking_kg	dec(11,5)
					declare @stuffing_kg				dec(11,5)	
					declare @stuffing_marking_kg		dec(11,5)
					


					-- 1 набивка
					if (select count_stuffing from #analysis) in (2 ,3)
					begin
							select 
									 @shipment_id				= shipment_id_1				
									,@shipment_sap_id			= shipment_sap_id			-- log
									,@shipment_date_max			= shipment_date_max					
									,@shipment_kg				= shipment_kg					
									,@shipment_row_id			= shipment_row_id_1			-- log
																		
									,@stuffing_row_id			= stuffing_row_id_1			
									,@stuffing_min_marking_kg	= stuffing_min_marking_kg_1		
									,@stuffing_step_marking_kg	= stuffing_step_marking_kg_1	
									,@stuffing_kg				= stuffing_kg_1					
									,@stuffing_marking_kg		= stuffing_marking_kg_1			
							from #analysis;

							exec .calc.distribution_stuffing_marking_and_shipment  
									 @shipment_id				= @shipment_id					
									,@shipment_sap_id			= @shipment_sap_id																  								
									,@shipment_date				= @shipment_date_max																  								
									,@shipment_kg				= @shipment_kg					
									,@shipment_row_id			= @shipment_row_id				
																																				  								
									,@stuffing_row_id			= @stuffing_row_id				
									,@stuffing_min_marking_kg	= @stuffing_min_marking_kg		
									,@stuffing_step_marking_kg	= @stuffing_step_marking_kg		
									,@stuffing_kg				= @stuffing_kg					
									,@stuffing_marking_kg		= @stuffing_marking_kg
									,@shipment_kg_output		= @shipment_kg_output out;;

					end;

					-- 2 набивка
					if (select count_stuffing from #analysis) in (2 ,3)
					begin

							select 
									 @shipment_id				= shipment_id_2				
									,@shipment_sap_id			= shipment_sap_id			-- log
									,@shipment_date_max			= shipment_date_max					
									,@shipment_kg				= shipment_kg				
									,@shipment_row_id			= shipment_row_id_2			-- log
																		
									,@stuffing_row_id			= stuffing_row_id_2			
									,@stuffing_min_marking_kg	= stuffing_min_marking_kg_2		
									,@stuffing_step_marking_kg	= stuffing_step_marking_kg_2	
									,@stuffing_kg				= stuffing_kg_2				
									,@stuffing_marking_kg		= stuffing_marking_kg_2		
							from #analysis;

							exec .calc.distribution_stuffing_marking_and_shipment  
									 @shipment_id				= @shipment_id					
									,@shipment_sap_id			= @shipment_sap_id																  								
									,@shipment_date				= @shipment_date_max																  								
									,@shipment_kg				= @shipment_kg					
									,@shipment_row_id			= @shipment_row_id				
																																				  								
									,@stuffing_row_id			= @stuffing_row_id				
									,@stuffing_min_marking_kg	= @stuffing_min_marking_kg		
									,@stuffing_step_marking_kg	= @stuffing_step_marking_kg		
									,@stuffing_kg				= @stuffing_kg					
									,@stuffing_marking_kg		= @stuffing_marking_kg
									,@shipment_kg_output		= @shipment_kg_output out;;

					end;
					
					-- 3 набивка
					if (select count_stuffing from #analysis) in (3)
					begin

							select 
									 @shipment_id				= shipment_id_3				
									,@shipment_sap_id			= shipment_sap_id			-- log
									,@shipment_date_max			= shipment_date_max					
									,@shipment_kg				= shipment_kg				
									,@shipment_row_id			= shipment_row_id_3			-- log
																		
									,@stuffing_row_id			= stuffing_row_id_3			
									,@stuffing_min_marking_kg	= stuffing_min_marking_kg_3	
									,@stuffing_step_marking_kg	= stuffing_step_marking_kg_3	
									,@stuffing_kg				= stuffing_kg_3				
									,@stuffing_marking_kg		= stuffing_marking_kg_3		
							from #analysis;

							exec .calc.distribution_stuffing_marking_and_shipment  
									 @shipment_id				= @shipment_id					
									,@shipment_sap_id			= @shipment_sap_id																  								
									,@shipment_date				= @shipment_date_max																  								
									,@shipment_kg				= @shipment_kg					
									,@shipment_row_id			= @shipment_row_id				
																																				  								
									,@stuffing_row_id			= @stuffing_row_id				
									,@stuffing_min_marking_kg	= @stuffing_min_marking_kg		
									,@stuffing_step_marking_kg	= @stuffing_step_marking_kg		
									,@stuffing_kg				= @stuffing_kg					
									,@stuffing_marking_kg		= @stuffing_marking_kg
									,@shipment_kg_output		= @shipment_kg_output out;;

					end;

			end;


end;























