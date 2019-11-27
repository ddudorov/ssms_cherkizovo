use project_plan_production_finished_products

go


ALTER PROCEDURE calc.distribution_stuffing_select_for_no_box @shipment_id						int
															,@shipment_sap_id					bigint			-- log
															,@shipment_stuffing_id				VARCHAR(40) 
															,@shipment_date_min					date
															,@shipment_date_max					date
															,@shipment_kg						dec(11,5)
															,@shipment_row_id					int				-- log
															,@shipment_name_table				varchar(40)		-- log

as
BEGIN

			SET NOCOUNT ON;

			-- набивка
			declare @stuffing_row_id					int;			
			declare @stuffing_min_marking_kg			dec(11,5);
			declare @stuffing_step_marking_kg			dec(11,5);
			declare @stuffing_kg						dec(11,5);		
			declare @stuffing_marking_kg				dec(11,5);		

			select 
					 @stuffing_row_id			= max(f.stuffing_row_id)
					,@stuffing_min_marking_kg	= max(f.stuffing_min_marking_kg)
					,@stuffing_step_marking_kg	= max(f.stuffing_step_marking_kg)
					,@stuffing_kg				= max(f.stuffing_kg)
					,@stuffing_marking_kg		= max(f.stuffing_marking_kg)
			from (
					select top 1
							 f.stuffing_row_id
							,f.stuffing_name_table
							,f.stuffing_min_marking_kg
							,f.stuffing_step_marking_kg
							,f.stuffing_kg
							,f.stuffing_marking_kg
					from #stuffing as f
					where f.stuffing_id = @shipment_stuffing_id
						and f.sap_id = @shipment_sap_id
						and isnull(f.stuffing_kg, 0) + isnull(f.stuffing_marking_kg, 0) > 0
						and f.stuffing_available_date between @shipment_date_min and @shipment_date_max
					order by f.stuffing_available_date
				) as f;

			-- ПИШЕМ ЛОГИ РАСПРЕДЕЛЕНИЯ НАБИВКИ | ЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГЛОГ | ПОТРЕБНОСТЬ
			insert into #stuffing_log_calculation (  shipment_row_id,  shipment_name_table,  shipment_sap_id, shipment_kg)	
											values( @shipment_row_id, @shipment_name_table, @shipment_sap_id, @shipment_kg);

			
			if @stuffing_row_id IS NULL RETURN(0);	-- ПРОВЕРКА: НАБИВКИ НЕТ	| ВЫХОД

			exec project_plan_production_finished_products.calc.distribution_stuffing_marking_and_shipment   @shipment_id					= @shipment_id					
																											,@shipment_sap_id				= @shipment_sap_id				
																																					  								
																											,@shipment_kg					= @shipment_kg					
																											,@shipment_row_id				= @shipment_row_id				
																											,@shipment_name_table			= @shipment_name_table			
																																					  								
																											,@stuffing_row_id				= @stuffing_row_id				
																											,@stuffing_min_marking_kg		= @stuffing_min_marking_kg		
																											,@stuffing_step_marking_kg		= @stuffing_step_marking_kg		
																											,@stuffing_kg					= @stuffing_kg					
																											,@stuffing_marking_kg			= @stuffing_marking_kg	

end;




							
						


