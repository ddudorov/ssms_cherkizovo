use project_plan_production_finished_products

go



-- exec project_plan_production_finished_products.report.for_SAP_sales_plan_select
-- exec project_plan_production_finished_products.report.for_SAP_sales_plan_select @type_report = 'main'
-- exec project_plan_production_finished_products.report.for_SAP_sales_plan_select @type_report = 'list_production_name'


alter PROCEDURE report.for_SAP_sales_plan_select @type_report varchar(50), @production_name varchar(50) = ''
											
as
BEGIN
			SET NOCOUNT ON;
			
			-- выгрузка списка заводов
			if @type_report = 'list_production_name'
			begin

					select sp.production_name
					from (
							select isnull(f.production_name, 'Завод не указан') as production_name
							from project_plan_production_finished_products.data_import.shipments_SAP as sp
							join project_plan_production_finished_products.info.stuffing as f on sp.stuffing_id = f.stuffing_id	
							where sp.reason_ignore_in_calculate is null

							union

							select isnull(f.production_name, 'Завод не указан') as production_name
							from project_plan_production_finished_products.data_import.shipments_sales_plan as sp
							join project_plan_production_finished_products.info.stuffing as f on sp.stuffing_id = f.stuffing_id	
							where sp.reason_ignore_in_calculate is null
						 ) as sp
					order by case 
									when sp.production_name = 'Завод не указан'	then 1
									when sp.production_name = 'Кашира'			then 2
									when sp.production_name = 'ЧМПЗ'			then 3
									when sp.production_name = 'ОП'				then 4
									else 5
							 end;
					
			end;

			-- выгрузка данных для отчета
			if @type_report = 'main'
			begin

					declare @dt_from datetime;  set @dt_from = (select date_file from project_plan_production_finished_products.data_import.info_excel where name_table = 'shipments_SAP');
					declare @dt_to datetime;	set @dt_to   = @dt_from  + 13;
					declare @sql varchar(max);
					declare @yyyyMMdd varchar(8);

					begin -- создаем таблицу из заявок SAP и плана продаж

							IF OBJECT_ID('tempdb..#union_shipment','U') is not null drop table #union_shipment;
					
							select 
									 sp.row_id
									,sp.name_table
									,sp.production_name
									,sp.shipment_delete
									,s.product_1C_full_name
									,convert(varchar(24), FORMAT(sp.sap_id, '000000000000000000000000')) as sap_id
									,sp.shipment_customer_name
									,isnull(sp.shipment_customer_id, 'Код клиента не указан') as shipment_customer_id  
									,sp.shipment_date
									,sp.shipment_kg
							into #union_shipment
							from (
									select 
											 sp.row_id
											,sp.name_table
											,isnull(f.production_name, 'Завод не указан') as production_name
											,sp.shipment_delete
											,sp.sap_id
											,sp.shipment_customer_id
											,sp.shipment_customer_name
											,sp.shipment_date
											,iif(sp.stuffing_id_box_type in (0, 1), sp.shipment_kg, null) as shipment_kg
									from project_plan_production_finished_products.data_import.shipments_SAP as sp
									join project_plan_production_finished_products.info.stuffing as f on sp.stuffing_id = f.stuffing_id
									where sp.reason_ignore_in_calculate is null
									  and sp.shipment_date between @dt_from and @dt_to
										

									union all

									select 
											 sp.row_id
											,sp.name_table
											,isnull(f.production_name, 'Завод не указан') as production_name
											,sp.shipment_delete
											,sp.sap_id
											,sp.shipment_customer_id
											,sp.shipment_customer_name
											,sp.shipment_date
											,iif(sp.stuffing_id_box_type in (0, 1), sp.shipment_kg, null) as shipment_kg
									from project_plan_production_finished_products.data_import.shipments_sales_plan as sp
									join project_plan_production_finished_products.info.stuffing as f on sp.stuffing_id = f.stuffing_id
									where sp.shipment_date between @dt_from and @dt_to
									  and sp.shipment_branch_name = 'ЧМПЗ Москва'
									  and sp.reason_ignore_in_calculate is null

								 ) as sp
							join cherkizovo.info.products_sap as s on sp.sap_id = s.SAP_id
							where sp.production_name like iif(@production_name = '','%',@production_name) ;
					end;

					begin -- создаем окончательную таблицу из заявок SAP и плана продаж

							IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment;

							select 
									 u.production_name	
									,u.product_1C_full_name	
									,u.sap_id	
									,u.shipment_customer_name	
									,u.shipment_customer_id	
									,u.shipment_date	

									,sum(iif(u.name_table = 'shipments_SAP',		u.shipment_kg, null)) as SAP_kg
									,sum(iif(u.name_table = 'shipments_sales_plan', u.shipment_kg, null)) as SP_kg

									,max(iif(u.name_table = 'shipments_SAP',		u.shipment_delete, null)) as SAP_del
									,max(iif(u.name_table = 'shipments_sales_plan', u.shipment_delete, null)) as SP_del
							
									,convert(varchar(5000),	(
																select convert(varchar(20), x.row_id) + ','
																from #union_shipment as x 
																where u.sap_id = x.sap_id 
																	and u.shipment_customer_id = x.shipment_customer_id
																	and u.shipment_customer_name = x.shipment_customer_name
																	and u.shipment_date = x.shipment_date
																	and x.name_table = 'shipments_SAP'
																FOR XML PATH('')
															 )) as SAP_row_id
							
									,convert(varchar(5000),	(
																select convert(varchar(20), x.row_id) + ','
																from #union_shipment as x 
																where u.sap_id = x.sap_id 
																	and u.shipment_customer_id = x.shipment_customer_id
																	and u.shipment_customer_name = x.shipment_customer_name
																	and u.shipment_date = x.shipment_date
																	and x.name_table = 'shipments_sales_plan'
																FOR XML PATH('')
															 )) as SP_row_id
							into #shipment
							from #union_shipment as u
							group by 							 
									 u.production_name		
									,u.product_1C_full_name	
									,u.sap_id	
									,u.shipment_customer_name	
									,u.shipment_customer_id	
									,u.shipment_date;

					end;
					

					-- индекс 
					--CREATE NONCLUSTERED INDEX NoCl_1 ON #shipment (sap_id, shipment_customer_name, shipment_customer_id)
					--include(row_id); 



							set @sql = '';
							set @sql = @sql + char(10) + '	select 
																	 ROW_NUMBER() over(order by  s.production_name
																								,GROUPING_ID(s.product_1C_full_name) desc, s.product_1C_full_name
																								,GROUPING_ID(s.sap_id) desc, s.sap_id
																								,GROUPING_ID(s.shipment_customer_name) desc, s.shipment_customer_name
																								,GROUPING_ID(s.shipment_customer_id) desc, s.shipment_customer_id
																								) as row_id
																	,case 
																			when GROUPING_ID(s.shipment_customer_name) = 0 then 3
																			when GROUPING_ID(s.product_1C_full_name) = 0 then 2
																			else 1
																	 end as frm_id

																	,s.production_name	
																	,case 
																			when GROUPING_ID(s.shipment_customer_name) = 0 then s.shipment_customer_name
																			when GROUPING_ID(s.product_1C_full_name) = 0 then s.product_1C_full_name
																			else s.production_name
																	 end as data_name
															
																	,case 
																			when GROUPING_ID(s.shipment_customer_id) = 0 then s.shipment_customer_id
																			when GROUPING_ID(s.sap_id) = 0 then s.sap_id
																	 end as data_id'

															

					while @dt_from < = @dt_to
					begin

							set @yyyyMMdd = format(@dt_from , 'yyyyMMdd')

							--set @sql = @sql + char(10) + ',null																																as listbox_'	+ @yyyyMMdd

																														
							set @sql = @sql + char(10) + ',iif(GROUPING_ID(s.shipment_customer_id) = 0,  
																										case 
																											when max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SAP_del, null)) = 0
																											 and max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SP_del, null)) = 0
																											then ''SAP и План!!!''

																											when max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SAP_del, null)) = 1
																											 and max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SP_del, null)) = 1
																											then ''SAP и План!!!''

																											when max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SAP_del, null)) = 0
																											  or max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SP_del, null)) = 1
																											then ''SAP''

																											when max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SAP_del, null)) = 1
																											  or max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SP_del, null)) = 0
																											then ''План продаж''
																										end																			, null) as listbox_'	+ @yyyyMMdd
																											
																											


							set @sql = @sql + char(10) + ',null																																as SAP_formula_'+ @yyyyMMdd
							set @sql = @sql + char(10) + '												,sum(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SAP_kg		, null))			as SAP_kg_'		+ @yyyyMMdd
							set @sql = @sql + char(10) + ',iif(GROUPING_ID(s.shipment_customer_id) = 0,  max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SAP_del		, null))	, null)	as SAP_del_'	+ @yyyyMMdd	
							set @sql = @sql + char(10) + ',iif(GROUPING_ID(s.shipment_customer_id) = 0,  max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SAP_row_id	, null))	, null)	as SAP_row_id_'	+ @yyyyMMdd

							set @sql = @sql + char(10) + ',null																																as SP_formula_'	+ @yyyyMMdd
							set @sql = @sql + char(10) + '												,sum(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SP_kg		, null))			as SP_kg_'		+ @yyyyMMdd
							set @sql = @sql + char(10) + ',iif(GROUPING_ID(s.shipment_customer_id) = 0,  max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SP_del		, null))	, null)	as SP_del_'		+ @yyyyMMdd
							set @sql = @sql + char(10) + ',iif(GROUPING_ID(s.shipment_customer_id) = 0,  max(iif(''' + @yyyyMMdd + ''' = s.shipment_date, s.SP_row_id	, null))	, null)	as SP_row__id_' + @yyyyMMdd	
												
							set @dt_from = @dt_from + 1;
					end;

							set @sql = @sql + char(10) + '	from #shipment as s'

							set @sql = @sql + char(10) + '	group by rollup( s.production_name
																			,s.product_1C_full_name
																			,s.sap_id
																			,(
																			 s.shipment_customer_name
																			,s.shipment_customer_id
																			 ))'

							set @sql = @sql + char(10) + '	having
																	GROUPING_ID(s.production_name) <> 1
																	and  not (GROUPING_ID(s.product_1C_full_name) = 0 and GROUPING_ID(s.sap_id) = 1)'

					
					--print @sql
					exec( @sql)
			

					IF OBJECT_ID('tempdb..#union_shipment','U') is not null drop table #union_shipment;
					IF OBJECT_ID('tempdb..#shipment','U') is not null drop table #shipment;

			end;

end;







