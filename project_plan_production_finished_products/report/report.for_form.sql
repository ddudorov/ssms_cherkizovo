use project_plan_production_finished_products 

go

-- exec project_plan_production_finished_products.report.for_form

ALTER PROCEDURE report.for_form 
as
BEGIN
			SET NOCOUNT ON;
			
			-- подготовка таблицы для формы
			begin

					IF OBJECT_ID('tempdb..#for_form','U') is not null drop table #for_form;
					--select * from #for_form
					create table #for_form
					(
						 label_name		varchar(50) not null
						,label_caption	varchar(50)		null
					);

			end;




			-- остатки
			begin
					
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (


							select 
									 isnull(format(min(		d.data_on_date				),'dd.MM.yyyy'			),'') as 'lbl_stock_data_on_date'

									,isnull(format(sum(		s.stock_kg					),'### ### ### ### ###'	),'') as 'lbl_stock_kg'
									,isnull(format(sum(		s.stock_shipment_kg			),'### ### ### ### ###'	),'') as 'lbl_stock_shipment_kg'
									,isnull(format(sum(		s.stock_after_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_stock_after_shipment_kg'

							from project_plan_production_finished_products.data_import.stock as s
							join project_plan_production_finished_products.data_import.data_type as d on s.stock_data_type = d.data_type
							where s.stock_reason_ignore_in_calculate is null

						 ) pv
					UNPIVOT(   label_caption for label_name in (lbl_stock_data_on_date, lbl_stock_kg, lbl_stock_shipment_kg, lbl_stock_after_shipment_kg)   ) as pv

			end;
			

			-- маркировка
			begin		
						
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (

							select 
									 isnull(format(max(d.data_on_date				),'dd.MM.yyyy'			),'') as 'lbl_marking_date_file'
									,isnull(format(sum(s.marking_kg					),'### ### ### ### ###'	),'') as 'lbl_marking_kg'
									,isnull(format(sum(s.marking_shipment_kg		),'### ### ### ### ###'	),'') as 'lbl_marking_shipment_kg'
									,isnull(format(sum(s.marking_after_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_marking_after_shipment_kg'

							from project_plan_production_finished_products.data_import.marking as s
							join project_plan_production_finished_products.data_import.data_type as d on s.marking_data_type = d.data_type
						 ) pv
					UNPIVOT(   label_caption for label_name in (lbl_marking_date_file, lbl_marking_kg, lbl_marking_shipment_kg, lbl_marking_after_shipment_kg)   ) as pv


			end;


			-- набифка факт
			begin		
						
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(max(ie.data_on_date			),'dd.MM.yyyy'			),'') as 'lbl_stuffing_fact_date_file'
									,isnull(format(sum(s.stuffing_kg			),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_kg'
									,isnull(format(sum(s.stuffing_surplus_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_surplus_kg'
									,isnull(format(sum(s.stuffing_marking_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_marking_kg'
									,isnull(format(sum(s.stuffing_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_shipment_kg'
							from project_plan_production_finished_products.data_import.stuffing_fact as s
							join project_plan_production_finished_products.data_import.data_type as ie on s.stuffing_data_type = ie.data_type
							where s.stuffing_sap_id is null
						 ) pv
					UNPIVOT( label_caption for label_name in (lbl_stuffing_fact_date_file, lbl_stuffing_fact_kg, lbl_stuffing_fact_surplus_kg, lbl_stuffing_fact_marking_kg, lbl_stuffing_fact_shipment_kg)   ) as pv
					
			end;


			-- набифка план
			begin		

					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(max(ie.data_on_date			),'dd.MM.yyyy'			),'') as 'lbl_stuffing_plan_date_file'
									,isnull(format(sum(s.stuffing_kg			),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_kg'
									,isnull(format(sum(s.stuffing_surplus_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_surplus_kg'
									,isnull(format(sum(s.stuffing_marking_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_marking_kg'
									,isnull(format(sum(s.stuffing_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_shipment_kg'
							from project_plan_production_finished_products.data_import.stuffing_plan as s
							join project_plan_production_finished_products.data_import.data_type as ie on s.stuffing_data_type = ie.data_type
							where s.stuffing_sap_id is null
						 ) pv
					UNPIVOT( label_caption for label_name in (lbl_stuffing_plan_date_file, lbl_stuffing_plan_kg, lbl_stuffing_plan_surplus_kg, lbl_stuffing_plan_marking_kg, lbl_stuffing_plan_shipment_kg)   ) as pv

			end;





			-- shipments_SAP / shipments_1C / shipments_sales_plan продаж / TOTAL
			begin
									
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									-- shipments_SAP
									 isnull(format(max(	iif(s.shipment_data_type = 'shipment_SAP',			d.data_on_date						, null)	),'dd.MM.yyyy'			),'') as 'lbl_SAP_data_on_date'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_SAP',			s.shipment_kg						, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_SAP',			s.shipment_from_stock_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_from_stock_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_SAP',			s.shipment_after_stock_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_after_stock_kg'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_SAP',			s.shipment_from_stuffing_fact_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_from_stuffing_fact_kg'
									--,isnull(format(sum(	iif(s.shipment_data_type = 'shipments_SAP',			s.shipment_after_stuffing_fact_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_after_stuffing_fact_kg'
									
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_SAP',			s.shipment_from_stuffing_plan_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_from_stuffing_plan_kg'
									--,isnull(format(sum(	iif(s.shipment_data_type = 'shipments_SAP',			s.shipment_after_stuffing_plan_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_after_stuffing_plan_kg'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_SAP',			s.shipment_from_marking_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_from_marking_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_SAP',			s.shipment_after_marking_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_SAP_after_marking_kg'
									
									-- shipments_1C
									,isnull(format(max(	iif(s.shipment_data_type = 'shipment_1C',			d.data_on_date						, null)	),'dd.MM.yyyy'			),'') as 'lbl_1C_data_on_date'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_1C',			s.shipment_kg						, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_1C',			s.shipment_from_stock_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_from_stock_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_1C',			s.shipment_after_stock_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_after_stock_kg'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_1C',			s.shipment_from_stuffing_fact_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_from_stuffing_fact_kg'
									--,isnull(format(sum(	iif(s.shipment_data_type = 'shipments_1C',			s.shipment_after_stuffing_fact_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_after_stuffing_fact_kg'
									
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_1C',			s.shipment_from_stuffing_plan_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_from_stuffing_plan_kg'
									--,isnull(format(sum(	iif(s.shipment_data_type = 'shipments_1C',			s.shipment_after_stuffing_plan_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_after_stuffing_plan_kg'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_1C',			s.shipment_from_marking_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_from_marking_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_1C',			s.shipment_after_marking_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_1C_after_marking_kg'

									
									-- shipments_sales_plan
									,isnull(format(max(	iif(s.shipment_data_type = 'shipment_sales_plan',	d.data_on_date						, null)	),'dd.MM.yyyy'			),'') as 'lbl_sales_plan_data_on_date'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_sales_plan',	s.shipment_kg						, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_sales_plan',	s.shipment_from_stock_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_from_stock_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_sales_plan',	s.shipment_after_stock_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_after_stock_kg'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_sales_plan',	s.shipment_from_stuffing_fact_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_from_stuffing_fact_kg'
									--,isnull(format(sum(	iif(s.shipment_data_type = 'shipments_sales_plan',	s.shipment_after_stuffing_fact_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_after_stuffing_fact_kg'
									
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_sales_plan',	s.shipment_from_stuffing_plan_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_from_stuffing_plan_kg'
									--,isnull(format(sum(	iif(s.shipment_data_type = 'shipments_sales_plan',	s.shipment_after_stuffing_plan_kg	, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_after_stuffing_plan_kg'

									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_sales_plan',	s.shipment_from_marking_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_from_marking_kg'
									,isnull(format(sum(	iif(s.shipment_data_type = 'shipment_sales_plan',	s.shipment_after_marking_kg			, null)	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_after_marking_kg'
									
									-- total
									,isnull(format(sum(														s.shipment_kg								),'### ### ### ### ###'	),'') as 'lbl_total_kg'
									,isnull(format(sum(														s.shipment_from_stock_kg					),'### ### ### ### ###'	),'') as 'lbl_total_from_stock_kg'
									,isnull(format(sum(														s.shipment_after_stock_kg					),'### ### ### ### ###'	),'') as 'lbl_total_after_stock_kg'

									,isnull(format(sum(														s.shipment_from_stuffing_fact_kg			),'### ### ### ### ###'	),'') as 'lbl_total_from_stuffing_fact_kg'
									--,isnull(format(sum(														s.shipment_after_stuffing_fact_kg			),'### ### ### ### ###'	),'') as 'lbl_total_after_stuffing_fact_kg'
									
									,isnull(format(sum(														s.shipment_from_stuffing_plan_kg			),'### ### ### ### ###'	),'') as 'lbl_total_from_stuffing_plan_kg'
									--,isnull(format(sum(														s.shipment_after_stuffing_plan_kg			),'### ### ### ### ###'	),'') as 'lbl_total_after_stuffing_plan_kg'

									,isnull(format(sum(														s.shipment_from_marking_kg					),'### ### ### ### ###'	),'') as 'lbl_total_from_marking_kg'
									,isnull(format(sum(														s.shipment_after_marking_kg					),'### ### ### ### ###'	),'') as 'lbl_total_after_marking_kg'

							from project_plan_production_finished_products.data_import.shipment as s
							join project_plan_production_finished_products.data_import.data_type as d on s.shipment_data_type = d.data_type
							where s.shipment_stuffing_id_box_type in (0, 1) and s.shipment_delete = 0
						 ) pv
					UNPIVOT( label_caption for label_name in (	
																 lbl_SAP_data_on_date, lbl_SAP_kg
																,lbl_SAP_from_stock_kg, lbl_SAP_after_stock_kg
																,lbl_SAP_from_stuffing_fact_kg--, lbl_SAP_after_stuffing_fact_kg
																,lbl_SAP_from_stuffing_plan_kg--, lbl_SAP_after_stuffing_plan_kg
																,lbl_SAP_from_marking_kg, lbl_SAP_after_marking_kg

																,lbl_1C_data_on_date, lbl_1C_kg
																,lbl_1C_from_stock_kg, lbl_1C_after_stock_kg
																,lbl_1C_from_stuffing_fact_kg--, lbl_1C_after_stuffing_fact_kg
																,lbl_1C_from_stuffing_plan_kg--, lbl_1C_after_stuffing_plan_kg
																,lbl_1C_from_marking_kg, lbl_1C_after_marking_kg

																,lbl_sales_plan_data_on_date, lbl_sales_plan_kg
																,lbl_sales_plan_from_stock_kg, lbl_sales_plan_after_stock_kg
																,lbl_sales_plan_from_stuffing_fact_kg--, lbl_sales_plan_after_stuffing_fact_kg
																,lbl_sales_plan_from_stuffing_plan_kg--, lbl_sales_plan_after_stuffing_plan_kg
																,lbl_sales_plan_from_marking_kg, lbl_sales_plan_after_marking_kg

																,lbl_total_kg
																,lbl_total_from_stock_kg, lbl_total_after_stock_kg
																,lbl_total_from_stuffing_fact_kg--, lbl_total_after_stuffing_fact_kg
																,lbl_total_from_stuffing_plan_kg--, lbl_total_after_stuffing_plan_kg
																,lbl_total_from_marking_kg, lbl_total_after_marking_kg
																
															)   ) as pv
					
			end;


			select 
					 label_name		
					,label_caption	
			from #for_form;



end;








































