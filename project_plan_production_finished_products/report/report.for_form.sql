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
									 isnull(format(max(ie.date_file				),'dd.MM.yyyy'			),'') as 'lbl_stock_date_file'
									,isnull(format(sum(s.stock_kg				),'### ### ### ### ###'	),'') as 'lbl_stock_kg'
									,isnull(format(sum(s.stock_shipment_kg		),'### ### ### ### ###'	),'') as 'lbl_stock_shipment_kg'
									,isnull(format(sum(s.stock_after_shipment_kg),'### ### ### ### ###'	),'') as 'lbl_stock_after_shipment_kg'
							from project_plan_production_finished_products.data_import.stock as s	
							join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
						 ) pv
					UNPIVOT(   label_caption for label_name in (lbl_stock_date_file, lbl_stock_kg, lbl_stock_shipment_kg, lbl_stock_after_shipment_kg)   ) as pv

			end;
			

			-- маркировка
			begin		
						
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (

							select 
									 isnull(format(max(ie.date_file					),'dd.MM.yyyy'			),'') as 'lbl_marking_date_file'
									,isnull(format(sum(s.marking_kg					),'### ### ### ### ###'	),'') as 'lbl_marking_kg'
									,isnull(format(sum(s.marking_shipment_kg		),'### ### ### ### ###'	),'') as 'lbl_marking_shipment_kg'
									,isnull(format(sum(s.marking_after_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_marking_after_shipment_kg'
							from project_plan_production_finished_products.data_import.marking as s
							join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
						 ) pv
					UNPIVOT(   label_caption for label_name in (lbl_marking_date_file, lbl_marking_kg, lbl_marking_shipment_kg, lbl_marking_after_shipment_kg)   ) as pv


			end;


			-- набифка факт
			begin		
						
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(max(ie.date_file				),'dd.MM.yyyy'			),'') as 'lbl_stuffing_fact_date_file'
									,isnull(format(sum(s.stuffing_kg			),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_kg'
									,isnull(format(sum(s.stuffing_surplus_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_surplus_kg'
									,isnull(format(sum(s.stuffing_marking_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_marking_kg'
									,isnull(format(sum(s.stuffing_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_fact_shipment_kg'
							from project_plan_production_finished_products.data_import.stuffing_fact as s
							join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
							where sap_id is null
						 ) pv
					UNPIVOT( label_caption for label_name in (lbl_stuffing_fact_date_file, lbl_stuffing_fact_kg, lbl_stuffing_fact_surplus_kg, lbl_stuffing_fact_marking_kg, lbl_stuffing_fact_shipment_kg)   ) as pv
					
			end;


			-- набифка план
			begin		
						
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(max(ie.date_file				),'dd.MM.yyyy'			),'') as 'lbl_stuffing_plan_date_file'
									,isnull(format(sum(s.stuffing_kg			),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_kg'
									,isnull(format(sum(s.stuffing_surplus_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_surplus_kg'
									,isnull(format(sum(s.stuffing_marking_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_marking_kg'
									,isnull(format(sum(s.stuffing_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_stuffing_plan_shipment_kg'
							from project_plan_production_finished_products.data_import.stuffing_plan as s
							join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
							where sap_id is null
						 ) pv
					UNPIVOT( label_caption for label_name in (lbl_stuffing_plan_date_file, lbl_stuffing_plan_kg, lbl_stuffing_plan_surplus_kg, lbl_stuffing_plan_marking_kg, lbl_stuffing_plan_shipment_kg)   ) as pv
					
			end;

			-- SAP
			begin
									
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(max(ie.date_file					),'dd.MM.yyyy'			),'') as 'lbl_SAP_date_file'
									,isnull(format(sum(s.shipment_kg				),'### ### ### ### ###'	),'') as 'lbl_SAP_shipment_kg'
									,isnull(format(sum(s.stock_shipment_kg			),'### ### ### ### ###'	),'') as 'lbl_SAP_stock_shipment_kg'
									,isnull(format(sum(s.stock_net_need_kg			),'### ### ### ### ###'	),'') as 'lbl_SAP_stock_net_need_kg'
									,isnull(format(sum(s.stuffing_fact_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_SAP_stuffing_fact_shipment_kg'
									,isnull(format(sum(s.stuffing_plan_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_SAP_stuffing_plan_shipment_kg'
									,isnull(format(sum(s.marking_shipment_kg		),'### ### ### ### ###'	),'') as 'lbl_SAP_marking_shipment_kg'
									,isnull(format(sum(s.marking_net_need_kg		),'### ### ### ### ###'	),'') as 'lbl_SAP_marking_net_need_kg'
							from project_plan_production_finished_products.data_import.shipments_SAP as s
							join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
							where s.stuffing_id_box_type in (0, 1) and s.shipment_delete = 0
						 ) pv
					UNPIVOT( label_caption for label_name in (	 lbl_SAP_date_file
																,lbl_SAP_shipment_kg
																,lbl_SAP_stock_shipment_kg
																,lbl_SAP_stock_net_need_kg
																,lbl_SAP_stuffing_fact_shipment_kg
																,lbl_SAP_stuffing_plan_shipment_kg
																,lbl_SAP_marking_shipment_kg
																,lbl_SAP_marking_net_need_kg)   ) as pv
					
			end;

			


			-- 1C
			begin
									
					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(max(ie.date_file					),'dd.MM.yyyy'			),'') as 'lbl_1C_date_file'
									,isnull(format(sum(s.shipment_kg				),'### ### ### ### ###'	),'') as 'lbl_1C_shipment_kg'
									,isnull(format(sum(s.stock_shipment_kg			),'### ### ### ### ###'	),'') as 'lbl_1C_stock_shipment_kg'
									,isnull(format(sum(s.stock_net_need_kg			),'### ### ### ### ###'	),'') as 'lbl_1C_stock_net_need_kg'
									,isnull(format(sum(s.stuffing_fact_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_1C_stuffing_fact_shipment_kg'
									,isnull(format(sum(s.stuffing_plan_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_1C_stuffing_plan_shipment_kg'
									,isnull(format(sum(s.marking_shipment_kg		),'### ### ### ### ###'	),'') as 'lbl_1C_marking_shipment_kg'
									,isnull(format(sum(s.marking_net_need_kg		),'### ### ### ### ###'	),'') as 'lbl_1C_marking_net_need_kg'
							from project_plan_production_finished_products.data_import.shipments_1C as s
							join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
							where s.stuffing_id_box_type in (0, 1)
						 ) pv
					UNPIVOT( label_caption for label_name in (	 lbl_1C_date_file
																,lbl_1C_shipment_kg
																,lbl_1C_stock_shipment_kg
																,lbl_1C_stock_net_need_kg
																,lbl_1C_stuffing_fact_shipment_kg
																,lbl_1C_stuffing_plan_shipment_kg
																,lbl_1C_marking_shipment_kg
																,lbl_1C_marking_net_need_kg)   ) as pv
					
					
			end;

			-- план продаж
			begin

					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(max(ie.date_file					),'dd.MM.yyyy'			),'') as 'lbl_sales_plan_date_file'
									,isnull(format(sum(s.shipment_kg				),'### ### ### ### ###'	),'') as 'lbl_sales_plan_shipment_kg'
									,isnull(format(sum(s.stock_shipment_kg			),'### ### ### ### ###'	),'') as 'lbl_sales_plan_stock_shipment_kg'
									,isnull(format(sum(s.stock_net_need_kg			),'### ### ### ### ###'	),'') as 'lbl_sales_plan_stock_net_need_kg'
									,isnull(format(sum(s.stuffing_fact_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_stuffing_fact_shipment_kg'
									,isnull(format(sum(s.stuffing_plan_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_sales_plan_stuffing_plan_shipment_kg'
									,isnull(format(sum(s.marking_shipment_kg		),'### ### ### ### ###'	),'') as 'lbl_sales_plan_marking_shipment_kg'
									,isnull(format(sum(s.marking_net_need_kg		),'### ### ### ### ###'	),'') as 'lbl_sales_plan_marking_net_need_kg'
							from project_plan_production_finished_products.data_import.shipments_sales_plan as s
							join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
							where s.stuffing_id_box_type in (0, 1) and s.shipment_delete = 0
						 ) pv
					UNPIVOT( label_caption for label_name in (	 lbl_sales_plan_date_file
																,lbl_sales_plan_shipment_kg
																,lbl_sales_plan_stock_shipment_kg
																,lbl_sales_plan_stock_net_need_kg
																,lbl_sales_plan_stuffing_fact_shipment_kg
																,lbl_sales_plan_stuffing_plan_shipment_kg
																,lbl_sales_plan_marking_shipment_kg
																,lbl_sales_plan_marking_net_need_kg)   ) as pv
					
			end;
		
		
			-- ИТОГ SAP + 1C + план продаж
			begin

					insert into #for_form (label_name, label_caption)
					select label_name, label_caption
					from (
							select 
									 isnull(format(sum(o.shipment_kg				),'### ### ### ### ###'	),'') as 'lbl_total_shipment_kg'
									,isnull(format(sum(o.stock_shipment_kg			),'### ### ### ### ###'	),'') as 'lbl_total_stock_shipment_kg'
									,isnull(format(sum(o.stock_net_need_kg			),'### ### ### ### ###'	),'') as 'lbl_total_stock_net_need_kg'
									,isnull(format(sum(o.stuffing_fact_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_total_stuffing_fact_shipment_kg'
									,isnull(format(sum(o.stuffing_plan_shipment_kg	),'### ### ### ### ###'	),'') as 'lbl_total_stuffing_plan_shipment_kg'
									,isnull(format(sum(o.marking_shipment_kg		),'### ### ### ### ###'	),'') as 'lbl_total_marking_shipment_kg'
									,isnull(format(sum(o.marking_net_need_kg		),'### ### ### ### ###'	),'') as 'lbl_total_marking_net_need_kg'
							from (
									select shipment_kg, stock_shipment_kg, stock_net_need_kg, stuffing_fact_shipment_kg, stuffing_plan_shipment_kg, marking_shipment_kg, marking_net_need_kg	
									from project_plan_production_finished_products.data_import.shipments_SAP
									where stuffing_id_box_type in (0, 1) and shipment_delete = 0

									union all

									select shipment_kg, stock_shipment_kg, stock_net_need_kg, stuffing_fact_shipment_kg, stuffing_plan_shipment_kg, marking_shipment_kg, marking_net_need_kg	
									from project_plan_production_finished_products.data_import.shipments_1C
									where stuffing_id_box_type in (0, 1)

									union all

									select shipment_kg, stock_shipment_kg, stock_net_need_kg, stuffing_fact_shipment_kg, stuffing_plan_shipment_kg, marking_shipment_kg, marking_net_need_kg	
									from project_plan_production_finished_products.data_import.shipments_sales_plan
									where stuffing_id_box_type in (0, 1) and shipment_delete = 0
								 ) as o
						 ) pv
					UNPIVOT( label_caption for label_name in (	 lbl_total_shipment_kg
																,lbl_total_stock_shipment_kg
																,lbl_total_stock_net_need_kg
																,lbl_total_stuffing_fact_shipment_kg
																,lbl_total_stuffing_plan_shipment_kg
																,lbl_total_marking_shipment_kg
																,lbl_total_marking_net_need_kg)   ) as pv
					
			end;

			select 
					 label_name		
					,label_caption	
			from #for_form;



end;








































