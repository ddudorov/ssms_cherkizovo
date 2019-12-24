use project_plan_production_finished_products

go

-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'fact' 
-- exec project_plan_production_finished_products.calc.distribution_stuffing @type_stuffing = 'plan'

ALTER PROCEDURE calc.distribution_stuffing_marking_and_shipment  @shipment_id						int
																,@shipment_sap_id					bigint			-- log
																,@shipment_date						datetime
																,@shipment_kg						dec(11,5)
																,@shipment_row_id					int				-- log

																,@stuffing_row_id					int			
																,@stuffing_min_marking_kg			dec(11,5)
																,@stuffing_step_marking_kg			dec(11,5)
																,@stuffing_kg						dec(11,5)	
																,@stuffing_marking_kg				dec(11,5)
																,@shipment_kg_output 				dec(11,5) OUTPUT  
as
BEGIN

			SET NOCOUNT ON;

			declare @stuffing_new_marking_kg	dec(11,5);
			declare @stuffing_shipment_kg		dec(11,5);

			-------------
			-- МАРКИРОВКА
			-------------
			if isnull(@stuffing_marking_kg, 0) < @shipment_kg and @stuffing_kg > 0
			begin
						
						set @stuffing_new_marking_kg = null -- пишем null так как надо понять, возможно, что-то отмаркировать

						set @stuffing_new_marking_kg = case 
															-- НАБИВКА БЕЗ ШАГА МАРКИРОВКИ | МАРКИРОВАЛАСЬ
															when @stuffing_step_marking_kg is null and not @stuffing_marking_kg is null then 
																case 																														
																	when @stuffing_kg + @stuffing_marking_kg >= @shipment_kg then @shipment_kg - @stuffing_marking_kg	-- остаток набивки + маркировка >= отгрузки
																	when @stuffing_kg + @stuffing_marking_kg <  @shipment_kg then @stuffing_kg							-- если артикул маркировался, и остаток набивки + маркировка <  отгрузки
																end
															
															-- НАБИВКА БЕЗ ШАГА МАРКИРОВКИ | НЕ МАРКИРОВАЛАСЬ | ОСТАТОК НАБИВКИ >= МИН ШАГА МАРКИРОВКИ
															when @stuffing_step_marking_kg is null and @stuffing_marking_kg is null and @stuffing_kg >= @stuffing_min_marking_kg then	
																case  
																	when @shipment_kg <  @stuffing_kg and @shipment_kg <= @stuffing_min_marking_kg	then @stuffing_min_marking_kg	-- отгрузка <= остаток набивки
																	when @shipment_kg <  @stuffing_kg and @shipment_kg >  @stuffing_min_marking_kg	then @shipment_kg				-- отгрузка <= остаток набивки
																	when @shipment_kg >= @stuffing_kg												then @stuffing_kg				-- отгрузка > остаток набивки
																end
																
															-- НАБИВКА C ШАГОМ МАРКИРОВКИ 	
															when not @stuffing_step_marking_kg is null then	
																case   
																	-- если потребность больше маркировки + остатки набивки --> отгружаем все
																	when @shipment_kg >= isnull(@stuffing_marking_kg, 0) + @stuffing_kg then @stuffing_kg
																	-- если остаток набивки после маркировки <  мин шага маркировки --> отгружаем все
																	when @stuffing_kg - (@shipment_kg - isnull(@stuffing_marking_kg, 0)) <  @stuffing_step_marking_kg then @stuffing_kg
																	-- если остаток набивки после маркировки >= мин шага маркировки --> шаг маркировки  * кол-во 
																	when @stuffing_kg - (@shipment_kg - isnull(@stuffing_marking_kg, 0)) >= @stuffing_step_marking_kg then @stuffing_step_marking_kg * CEILING((@shipment_kg - isnull(@stuffing_marking_kg, 0)) / @stuffing_step_marking_kg)

																	--when FLOOR(@stuffing_kg / @stuffing_min_marking_kg) - FLOOR((@shipment_kg - isnull(@stuffing_marking_kg, 0)) / @stuffing_min_marking_kg) <= 1	-- когда набивки мало или равна заказу и остается остаток меньше мин маркироки
																	--then @stuffing_kg

																	--when FLOOR(@stuffing_kg / @stuffing_min_marking_kg) - FLOOR((@shipment_kg - isnull(@stuffing_marking_kg, 0)) / @stuffing_min_marking_kg) >  1	-- когда набивки больше и остаток меньше мин маркироки
																	--then @stuffing_min_marking_kg * CEILING((@shipment_kg - isnull(@stuffing_marking_kg, 0)) / @stuffing_min_marking_kg)
																	-- маркируем и отгружаем всегда шаг маркировки, например остаток набивки 1700 кг шаг 500 кг, то маркируем 500 + 500 + 700
																	--when @stuffing_kg - @stuffing_step_marking_kg >  @stuffing_step_marking_kg then @stuffing_step_marking_kg		-- набивка - шаг маркировки > шага маркировки
																	--when @stuffing_kg - @stuffing_step_marking_kg <= @stuffing_step_marking_kg then @stuffing_kg					-- набивка - шаг маркировки <= шага маркировки
																end
														end;  

						-- обновляем переменные
						if ISNULL(@stuffing_new_marking_kg, 0) <> 0
						begin

								-- кол-во для маркировки вычитаем из остатка набивки и добавляем в маркировку
								update #stuffing
								set stuffing_kg			= stuffing_kg - @stuffing_new_marking_kg
								   ,stuffing_marking_kg = iif(stuffing_sap_id = @shipment_sap_id, isnull(stuffing_marking_kg, 0) + @stuffing_new_marking_kg, stuffing_marking_kg)
								where stuffing_row_id = @stuffing_row_id;


								-- записываем сколько всего кг для маркировки
								select @stuffing_kg = stuffing_kg
									  ,@stuffing_marking_kg = stuffing_marking_kg
								from #stuffing
								where stuffing_sap_id = @shipment_sap_id
								 and stuffing_row_id = @stuffing_row_id;

						end;
						
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
						where stuffing_row_id		= @stuffing_row_id
						  and stuffing_sap_id		= @shipment_sap_id;


						-- ПИШЕМ ЛОГИ РАСПРДЕЛЕНИЯ НАБИВКИ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ
						insert into #stuffing_log_calculation (  shipment_row_id,  stuffing_sap_id,  shipment_date,  shipment_kg,  stuffing_row_id,  stuffing_kg,  stuffing_marking_kg,  stuffing_shipment_kg)	
														values( @shipment_row_id, @shipment_sap_id, @shipment_date, @shipment_kg, @stuffing_row_id, @stuffing_kg, @stuffing_marking_kg, @stuffing_shipment_kg)	
											
						

						-- РАСПРЕДЕЛЯЕМ: ОТГРУЖАЕМ ИЗ МАРКИРОВКИ ДЛЯ ЗАЯВКИ / ПЛАНА
						update #shipment
						set shipment_kg			= shipment_kg - @stuffing_shipment_kg
						where shipment_row_id	= @shipment_row_id;

						set @shipment_kg_output	= @shipment_kg - @stuffing_shipment_kg;


			end;					
		
end;




							
						


