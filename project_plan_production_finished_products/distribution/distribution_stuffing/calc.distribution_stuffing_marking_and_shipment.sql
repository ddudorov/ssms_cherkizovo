use project_plan_production_finished_products

go

-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'fact' 
-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'plan'

ALTER PROCEDURE calc.distribution_stuffing_marking_and_shipment  @shipment_id						int
																,@shipment_sap_id					bigint			-- log
														
																,@shipment_kg						dec(11,5)
																,@shipment_row_id					int				-- log
																,@shipment_name_table				varchar(40)		-- log

																,@stuffing_row_id					int			
																,@stuffing_min_marking_kg			dec(11,5)
																,@stuffing_step_marking_kg			dec(11,5)
																,@stuffing_kg						dec(11,5)	
																,@stuffing_marking_kg				dec(11,5)
as
BEGIN

			SET NOCOUNT ON;
			
			declare @stuffing_shipment_kg				dec(11,5);

			
			if @shipment_id		IS NULL RETURN(0);	-- ПРОВЕРКА: ОТГРУЗКА NULL	| ВЫХОД 
			if @stuffing_row_id IS NULL RETURN(0);	-- ПРОВЕРКА: НАБИВКИ НЕТ	| ВЫХОД


			-------------
			-- МАРКИРОВКА
			-------------
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
						
						-- ВЫХОДИМ ЕСЛИ НЕТ МАРКИРОВКИ
						if ISNULL(@stuffing_marking_kg, 0) = 0 return(0);

						-- кол-во для маркировки вычитаем из остатка набивки и добавляем в маркировку
						update #stuffing
						set stuffing_kg			 = stuffing_kg - @stuffing_marking_kg
							,stuffing_marking_kg = iif(sap_id = @shipment_sap_id, isnull(stuffing_marking_kg, 0) + @stuffing_marking_kg, stuffing_marking_kg)
						where stuffing_row_id = @stuffing_row_id;


						-- записываем сколько всего кг для маркировки
						select @stuffing_kg = stuffing_kg
							  ,@stuffing_marking_kg = stuffing_marking_kg
						from #stuffing
						where sap_id = @shipment_sap_id
						 and stuffing_row_id = @stuffing_row_id;	

			end;
									


			-----------
			-- ОТГРУЗКА
			-----------
			begin

						-- ЗАПИСЫВАЕМ КОЛ-ВО ДЛЯ ОТГРУЗКИ
						set @stuffing_shipment_kg = iif(@stuffing_marking_kg >= @shipment_kg, @shipment_kg, @stuffing_marking_kg)
			
			
						-- РАСПРЕДЕЛЯЕМ: ОТГРУЖАЕМ ИЗ МАРКИРОВКИ ДЛЯ НАБИВКИ
						update #stuffing
						set  stuffing_marking_kg	= stuffing_marking_kg				- @stuffing_shipment_kg
							,stuffing_shipment_kg	= isnull(stuffing_shipment_kg, 0)	+ @stuffing_shipment_kg
						where stuffing_row_id = @stuffing_row_id
						  and sap_id = @shipment_sap_id;


						-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ НАБИВКИ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
						insert into #stuffing_log_calculation (  shipment_row_id,  shipment_name_table,  shipment_sap_id,  shipment_kg,  stuffing_row_id,  stuffing_kg,  stuffing_marking_kg,  stuffing_shipment_kg)	
														values( @shipment_row_id, @shipment_name_table, @shipment_sap_id, @shipment_kg, @stuffing_row_id, @stuffing_kg, @stuffing_marking_kg, @stuffing_shipment_kg)	
																	  

						-- РАСПРЕДЕЛЯЕМ: ОТГРУЖАЕМ ИЗ МАРКИРОВКИ ДЛЯ ЗАЯВКИ / ПЛАНА
						update #shipment
						set shipment_kg = shipment_kg - @stuffing_shipment_kg
						where shipment_id = @shipment_id;

			
			end;					
		
end;




							
						


