use project_plan_production_finished_products

go


ALTER PROCEDURE calc.distribution_stuffing_select_for_box @stuffing_id_box_row_id int
as
BEGIN

			SET NOCOUNT ON;

			declare @count_stuffing tinyint; -- кол-во набивок в коробке
			declare @analysis_id_1  smallint; 
			declare @analysis_id_2  smallint; 
			declare @analysis_id_3  smallint; 

			-----------------------------------------------------------------------------------------
			-- ЕСЛИ КОРОБКА, НУЖНО ОПРЕДЕЛИТЬ КАКИЕ НАБИВКИ БРАТЬ, С МИН РАСХОЖДЕНИЯМИ В ДНЯХ ВЫХОДА
			-----------------------------------------------------------------------------------------
			begin -- Выгружаем потребность для коробок 

					IF OBJECT_ID('tempdb..#shipment_check_marking','U') is not null drop table #shipment_check_marking;
					
					-- select * from #shipment where not  stuffing_id_box_row_id is null order by stuffing_id_box_row_id
					-- select * from #shipment where stuffing_id_box_row_id = 4927

					-- select * from #shipment_check_marking where 

					select 
							 sh.shipment_id
							,sh.shipment_row_id
							,sh.shipment_name_table
							,sh.sap_id
							,sh.stuffing_id
							,sh.shipment_date_min
							,sh.shipment_date_max
							,sh.shipment_kg
					into #shipment_check_marking
					from #shipment as sh
					where sh.stuffing_id_box_row_id = @stuffing_id_box_row_id;
					
					-- ПИШЕМ ЛОГИ РАСПРЕДЕЛЕНИЯ НАБИВКИ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ | ПОТРЕБНОСТЬ
					insert into #stuffing_log_calculation 
					(	   shipment_row_id,  shipment_name_table,  shipment_sap_id, shipment_kg)	
					select shipment_row_id,  shipment_name_table,			sap_id, shipment_kg from #shipment_check_marking;
					
					set @count_stuffing = (select count(1) from #shipment_check_marking);
					


			end;

			begin -- Считаем разницу между мин датой доступности | подготовка данных для анализа 1 
			
					IF OBJECT_ID('tempdb..#stuffing_for_analysis_1','U') is not null drop table #stuffing_for_analysis_1;
								
					select 
							 st.stuffing_id
							,st.stuffing_row_id
							,st.stuffing_available_date
							,st.stuffing_before_next_available_date
							,st.stuffing_min_marking_kg
							,st.stuffing_step_marking_kg
							,st.stuffing_kg
							,st.stuffing_marking_kg
							,sh.shipment_id
							,sh.shipment_row_id
							,sh.shipment_name_table
							,sh.sap_id as shipment_sap_id
							,sh.shipment_kg
							,ROW_NUMBER() over (order by st.stuffing_id) as analysis_id
							,DENSE_RANK() over (order by st.stuffing_id) as group_stuffing_id
							,DATEDIFF(day, min(st.stuffing_available_date) over (), st.stuffing_available_date) as between_in_days
							,case DENSE_RANK() over (order by st.stuffing_id) when 1 then DATEDIFF(day, min(st.stuffing_available_date) over (), st.stuffing_available_date) end as between_in_days_1
							,case DENSE_RANK() over (order by st.stuffing_id) when 2 then DATEDIFF(day, min(st.stuffing_available_date) over (), st.stuffing_available_date) end as between_in_days_2
							,case DENSE_RANK() over (order by st.stuffing_id) when 3 then DATEDIFF(day, min(st.stuffing_available_date) over (), st.stuffing_available_date) end as between_in_days_3

							,CONVERT(smallint, null) as between_in_days_min_1, CONVERT(smallint, null) as analysis_id_min_1
							,CONVERT(smallint, null) as between_in_days_min_2, CONVERT(smallint, null) as analysis_id_min_2		
							,CONVERT(smallint, null) as between_in_days_min_3, CONVERT(smallint, null) as analysis_id_min_3
											   
							,CONVERT(smallint, null) as between_in_days_max_1, CONVERT(smallint, null) as analysis_id_max_1
							,CONVERT(smallint, null) as between_in_days_max_2, CONVERT(smallint, null) as analysis_id_max_2		
							,CONVERT(smallint, null) as between_in_days_max_3, CONVERT(smallint, null) as analysis_id_max_3	
							
							,CONVERT(dec(15,7),null) as avg_between_in_days_min
							,CONVERT(dec(15,7),null) as avg_between_in_days_max
							
							,CONVERT(dec(15,7),null)  as avg_between_in_days
							,CONVERT(varchar(3),null) as avg_between_select
					into #stuffing_for_analysis_1
					from #stuffing as st
					join #shipment_check_marking as sh 
					on st.sap_id = sh.sap_id and st.stuffing_id = sh.stuffing_id and st.stuffing_available_date between sh.shipment_date_min and sh.shipment_date_max
					where st.stuffing_kg + isnull(st.stuffing_marking_kg, 0) > 0;

			end;

			begin -- Переносим строку для каждой набивки, мин разница и максимальнаая разниц анализ 2 
					
					update m
					set  -- дней относительно выходам набивки минимум ближайщее
						 m.between_in_days_min_1	= iif( m.group_stuffing_id = 1, m.between_in_days,	(select top 1 a.between_in_days from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 1 and m.between_in_days >= a.between_in_days order by a.between_in_days DESC))
						,m.between_in_days_min_2	= iif( m.group_stuffing_id = 2, m.between_in_days,	(select top 1 a.between_in_days from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 2 and m.between_in_days >= a.between_in_days order by a.between_in_days DESC))
						,m.between_in_days_min_3	= iif( m.group_stuffing_id = 3, m.between_in_days,	(select top 1 a.between_in_days from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 3 and m.between_in_days >= a.between_in_days order by a.between_in_days DESC))

						-- дней относительно выходам набивки минимум ID
						,m.analysis_id_min_1		= iif( m.group_stuffing_id = 1, m.analysis_id,		(select top 1 a.analysis_id		from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 1 and m.between_in_days >= a.between_in_days order by a.between_in_days DESC))
						,m.analysis_id_min_2		= iif( m.group_stuffing_id = 2, m.analysis_id,		(select top 1 a.analysis_id		from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 2 and m.between_in_days >= a.between_in_days order by a.between_in_days DESC))
						,m.analysis_id_min_3		= iif( m.group_stuffing_id = 3, m.analysis_id,		(select top 1 a.analysis_id		from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 3 and m.between_in_days >= a.between_in_days order by a.between_in_days DESC))
						
						-- дней относительно выходам набивки максимум ближайщее					 
						,m.between_in_days_max_1	= iif( m.group_stuffing_id = 1, m.between_in_days,	(select top 1 a.between_in_days from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 1 and m.between_in_days <= a.between_in_days order by a.between_in_days ASC))
						,m.between_in_days_max_2	= iif( m.group_stuffing_id = 2, m.between_in_days,	(select top 1 a.between_in_days from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 2 and m.between_in_days <= a.between_in_days order by a.between_in_days ASC))
						,m.between_in_days_max_3	= iif( m.group_stuffing_id = 3, m.between_in_days,	(select top 1 a.between_in_days from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 3 and m.between_in_days <= a.between_in_days order by a.between_in_days ASC))

						-- дней относительно выходам набивки максимум ID
						,m.analysis_id_max_1		= iif( m.group_stuffing_id = 1, m.analysis_id,		(select top 1 a.analysis_id		from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 1 and m.between_in_days <= a.between_in_days order by a.between_in_days DESC))
						,m.analysis_id_max_2		= iif( m.group_stuffing_id = 2, m.analysis_id,		(select top 1 a.analysis_id		from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 2 and m.between_in_days <= a.between_in_days order by a.between_in_days DESC))
						,m.analysis_id_max_3		= iif( m.group_stuffing_id = 3, m.analysis_id,		(select top 1 a.analysis_id		from #stuffing_for_analysis_1 as a where a.group_stuffing_id = 3 and m.between_in_days <= a.between_in_days order by a.between_in_days DESC))
					from #stuffing_for_analysis_1 as m
					
			end;
			
			begin -- Расчитываем сред в днях между выходами 

					update m
					set avg_between_in_days_min =
							case 
							when @count_stuffing = 3 then isnull(   ( 0.0 + ABS(m.between_in_days_min_1 - m.between_in_days_min_2) + ABS(m.between_in_days_min_2 - m.between_in_days_min_3) +  ABS(m.between_in_days_min_1 - m.between_in_days_min_3)) / 3   , 99999)
							when @count_stuffing = 2 then isnull(     0.0 + ABS(m.between_in_days_min_1 - m.between_in_days_min_2)   , 99999)
							end 

						,avg_between_in_days_max =
							case 
							when @count_stuffing = 3 then isnull(   ( 0.0 + ABS(m.between_in_days_max_1 - m.between_in_days_max_2) + ABS(m.between_in_days_max_2 - m.between_in_days_max_3) +  ABS(m.between_in_days_max_1 - m.between_in_days_max_3)) / 3   , 99999)
							when @count_stuffing = 2 then isnull(     0.0 + ABS(m.between_in_days_max_1 - m.between_in_days_max_2)   , 99999)	
							end 
					from #stuffing_for_analysis_1 as m;		
			end;
	
			begin -- Определяем какие набивки берем для маркировки 

					update #stuffing_for_analysis_1
					set avg_between_in_days = case 
													when avg_between_in_days_min <> 99999 and avg_between_in_days_max  = 99999	then avg_between_in_days_min
													when avg_between_in_days_min  = 99999 and avg_between_in_days_max <> 99999	then avg_between_in_days_max
													when avg_between_in_days_min <= avg_between_in_days_max						then avg_between_in_days_min
													when avg_between_in_days_min  > avg_between_in_days_max						then avg_between_in_days_max
													else 99999
											  end 
					  ,avg_between_select = case 
													when avg_between_in_days_min <> 99999 and avg_between_in_days_max  = 99999	then 'min'
													when avg_between_in_days_min  = 99999 and avg_between_in_days_max <> 99999	then 'max'
													when avg_between_in_days_min <= avg_between_in_days_max						then 'min'
													when avg_between_in_days_min  > avg_between_in_days_max						then 'max'
													else 'no'
											  end;
			
			end;

			begin -- выбираем набивки для дальнейшего расчета

					select
							 @analysis_id_1 = max(a.analysis_id_1)
							,@analysis_id_2 = max(a.analysis_id_2)
							,@analysis_id_3 = max(a.analysis_id_3)
					from (
							select top 1
									 case when avg_between_select = 'min' then analysis_id_min_1 when avg_between_select = 'max' then analysis_id_max_1 end as analysis_id_1
									,case when avg_between_select = 'min' then analysis_id_min_2 when avg_between_select = 'max' then analysis_id_max_2 end as analysis_id_2
									,case when avg_between_select = 'min' then analysis_id_min_3 when avg_between_select = 'max' then analysis_id_max_3 end as analysis_id_3
							from #stuffing_for_analysis_1
							where avg_between_in_days <= 7
							order by avg_between_in_days
						 ) as a;

					if @analysis_id_1 is null or @analysis_id_2 IS NULL RETURN(0); -- выходим так как набивок должно быть мин 2
					if @count_stuffing = 3 and @analysis_id_3	IS NULL RETURN(0); -- выходим так как коробка состоит из 3 набивок, а набивок 2
			end;
			
						
			--select *
			--from #stuffing_for_analysis_1
			--where analysis_id in (@analysis_id_1,  @analysis_id_2, @analysis_id_3)
 
			begin -- отправляем на маркировку и отгрузку 

					declare @shipment_id				int
					declare @shipment_sap_id			bigint			-- log

					declare @shipment_kg				dec(11,5)
					declare @shipment_row_id			int				-- log
					declare @shipment_name_table		varchar(40)		-- log

					declare @stuffing_row_id			int			
					declare @stuffing_min_marking_kg	dec(11,5)
					declare @stuffing_step_marking_kg	dec(11,5)
					declare @stuffing_kg				dec(11,5)	
					declare @stuffing_marking_kg		dec(11,5)
					
					-- 1 набивка
					if not @analysis_id_1 is null
					begin
							select 
									 @shipment_sap_id			= shipment_sap_id			-- log
																	
									,@shipment_kg				= shipment_kg				
									,@shipment_row_id			= shipment_row_id			-- log
									,@shipment_name_table		= shipment_name_table		-- log
																		
									,@stuffing_row_id			= stuffing_row_id			
									,@stuffing_min_marking_kg	= stuffing_min_marking_kg	
									,@stuffing_step_marking_kg	= stuffing_step_marking_kg	
									,@stuffing_kg				= stuffing_kg				
									,@stuffing_marking_kg		= stuffing_marking_kg		
							from #stuffing_for_analysis_1
							where analysis_id = @analysis_id_1;

							exec project_plan_production_finished_products.calc.distribution_stuffing_marking_and_shipment  
									 @shipment_id				= @shipment_id					
									,@shipment_sap_id			= @shipment_sap_id				
																																				  								
									,@shipment_kg				= @shipment_kg					
									,@shipment_row_id			= @shipment_row_id				
									,@shipment_name_table		= @shipment_name_table			
																																				  								
									,@stuffing_row_id			= @stuffing_row_id				
									,@stuffing_min_marking_kg	= @stuffing_min_marking_kg		
									,@stuffing_step_marking_kg	= @stuffing_step_marking_kg		
									,@stuffing_kg				= @stuffing_kg					
									,@stuffing_marking_kg		= @stuffing_marking_kg;

					end;

					-- 2 набивка
					if not @analysis_id_2 is null
					begin

							select 
									 @shipment_id				= shipment_id				
									,@shipment_sap_id			= shipment_sap_id			-- log
																	
									,@shipment_kg				= shipment_kg				
									,@shipment_row_id			= shipment_row_id			-- log
									,@shipment_name_table		= shipment_name_table		-- log
																		
									,@stuffing_row_id			= stuffing_row_id			
									,@stuffing_min_marking_kg	= stuffing_min_marking_kg	
									,@stuffing_step_marking_kg	= stuffing_step_marking_kg	
									,@stuffing_kg				= stuffing_kg				
									,@stuffing_marking_kg		= stuffing_marking_kg		
							from #stuffing_for_analysis_1
							where analysis_id = @analysis_id_2;
		
							exec project_plan_production_finished_products.calc.distribution_stuffing_marking_and_shipment  
									 @shipment_id				= @shipment_id					
									,@shipment_sap_id			= @shipment_sap_id				
																																				  								
									,@shipment_kg				= @shipment_kg					
									,@shipment_row_id			= @shipment_row_id				
									,@shipment_name_table		= @shipment_name_table			
																																				  								
									,@stuffing_row_id			= @stuffing_row_id				
									,@stuffing_min_marking_kg	= @stuffing_min_marking_kg		
									,@stuffing_step_marking_kg	= @stuffing_step_marking_kg		
									,@stuffing_kg				= @stuffing_kg					
									,@stuffing_marking_kg		= @stuffing_marking_kg;

					end;
					
					-- 3 набивка
					if not @analysis_id_3 is null
					begin

							select 
									 @shipment_id				= shipment_id				
									,@shipment_sap_id			= shipment_sap_id			-- log
																	
									,@shipment_kg				= shipment_kg				
									,@shipment_row_id			= shipment_row_id			-- log
									,@shipment_name_table		= shipment_name_table		-- log
																		
									,@stuffing_row_id			= stuffing_row_id			
									,@stuffing_min_marking_kg	= stuffing_min_marking_kg	
									,@stuffing_step_marking_kg	= stuffing_step_marking_kg	
									,@stuffing_kg				= stuffing_kg				
									,@stuffing_marking_kg		= stuffing_marking_kg		
							from #stuffing_for_analysis_1
							where analysis_id = @analysis_id_3;
		
							exec project_plan_production_finished_products.calc.distribution_stuffing_marking_and_shipment  
									 @shipment_id				= @shipment_id					
									,@shipment_sap_id			= @shipment_sap_id				
																																				  								
									,@shipment_kg				= @shipment_kg					
									,@shipment_row_id			= @shipment_row_id				
									,@shipment_name_table		= @shipment_name_table			
																																				  								
									,@stuffing_row_id			= @stuffing_row_id				
									,@stuffing_min_marking_kg	= @stuffing_min_marking_kg		
									,@stuffing_step_marking_kg	= @stuffing_step_marking_kg		
									,@stuffing_kg				= @stuffing_kg					
									,@stuffing_marking_kg		= @stuffing_marking_kg;

					end;

			end;


end;




							
						









































