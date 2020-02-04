use project_plan_production_finished_products

-- exec report.surplus_with_production_date	

go

alter procedure report.surplus_with_production_date										
as
BEGIN
			SET NOCOUNT ON;
						
			begin -- СОЗДАЕМ ТАБЛИЦУ КУДА БУДУТ ЗАГРУЖЕНЫ ВСЕ ДАННЫЕ 

					IF OBJECT_ID('tempdb..#union_data','U') is not null drop table #union_data; 

					create table #union_data
					(													
								 data_type						varchar(30)	not null
								,sap_id							bigint		not null	
								,production_date				datetime	not null
								,expiration_date				datetime	not null
								,expiration_date_in_days		as datediff(day, production_date, expiration_date)

								,date_not_shipped				datetime	not null					
								,surplus_kg						dec(11, 5)	not null		
					);

			end;

			begin -- СОЗДАЕМ ТАБЛИЦУ С ДАТАМИ 

					IF OBJECT_ID('tempdb..#calendar','U') is not null drop table #calendar; 

					select dt_tm as dt
					into #calendar
					from cherkizovo.info.calendar
					where dt_tm between (select min(shipment_date) from data_import.shipment)
									and (select max(shipment_date) from data_import.shipment);
			end;
			
			begin -- ОСТАТКИ 

					insert into #union_data
					(
							 data_type
							,sap_id		
							,production_date				
							,expiration_date
							,date_not_shipped				
							,surplus_kg			
					)			

					select 
							 st.stock_data_type
							,st.stock_sap_id
							,st.stock_production_date
							,st.stock_expiration_date
							,st.dt
							,sum(st.surplus_kg) as surplus_kg

					from (
							select
									 st.stock_data_type
									,st.stock_row_id
									,st.stock_sap_id
									,st.stock_production_date
									,st.stock_expiration_date
									,st.dt
									--,sum(st.stock_kg - isnull(st.stock_log_shipment_kg, 0)) as stock_total_kg
									,sum(st.stock_after_shipment_kg) as surplus_kg
									,row_number() over (partition by st.stock_row_id order by st.dt) as first_date_surplus
							from (
									select 
											 st.stock_row_id
											,st.stock_data_type
											,st.stock_sap_id
											,st.stock_production_date
											,st.stock_expiration_date
											,c.dt
											,st.stock_kg
											,st.stock_shipment_kg
											,st.stock_after_shipment_kg
											,(select sum(l.stock_shipment_kg) 
												from data_import.stock_log_calculation as l
												where st.stock_row_id = l.stock_row_id
												and c.dt >= l.shipment_date 
												and not l.stock_shipment_kg is null) as stock_log_shipment_kg
									from data_import.stock as st
									join #calendar as c on st.stock_on_date <= c.dt
									where st.stock_reason_ignore_in_calculate is null
										and not st.stock_after_shipment_kg is null
										--and st.stock_sap_id in (000000001030625004300101,000000001030631604300101)
									) as st
							where st.stock_kg - isnull(st.stock_log_shipment_kg, 0) <> 0
							group by 
									 st.stock_data_type
									,st.stock_row_id
									,st.stock_sap_id
									,st.stock_production_date
									,st.stock_expiration_date
									,st.dt
							having sum(st.stock_kg - isnull(st.stock_log_shipment_kg, 0)) = sum(st.stock_after_shipment_kg)
						) as st 
					where st.first_date_surplus = 1	
					group by 
							 st.stock_data_type
							,st.stock_sap_id
							,st.stock_production_date
							,st.stock_expiration_date
							,st.dt;

			end;
						
			begin -- МАРКИРОВКА

					insert into #union_data
					(
								 data_type
								,sap_id		
								,production_date				
								,expiration_date
								,date_not_shipped				
								,surplus_kg			
					)			

					select 
							 st.marking_data_type
							,st.marking_sap_id
							,st.marking_production_date
							,st.marking_expiration_date
							,st.dt
							,sum(st.surplus_kg) as surplus_kg

					from (
							select
									 st.marking_data_type
									,st.marking_row_id
									,st.marking_sap_id
									,st.marking_production_date
									,st.marking_expiration_date
									,st.dt
									,sum(st.marking_after_shipment_kg) as surplus_kg
									,row_number() over (partition by st.marking_row_id order by st.dt) as first_date_surplus								
							from (
									select 
											 st.marking_row_id
											,st.marking_data_type
											,st.marking_sap_id
											,st.marking_production_date
											,st.marking_expiration_date
											,c.dt
											,st.marking_kg
											,st.marking_shipment_kg
											,st.marking_after_shipment_kg
											,(select sum(l.marking_shipment_kg) 
												from data_import.marking_log_calculation as l
												where st.marking_row_id = l.marking_row_id
												and c.dt >= l.shipment_date 
												and not l.marking_shipment_kg is null) as marking_log_shipment_kg
									from data_import.marking as st
									join #calendar as c on st.marking_on_date <= c.dt
									where st.marking_reason_ignore_in_calculate is null
										and not st.marking_after_shipment_kg is null
										--and st.marking_sap_id in (000000001030625004300101,000000001030631604300101)
									) as st
							where st.marking_kg - isnull(st.marking_log_shipment_kg, 0) <> 0
							group by 
									 st.marking_data_type
									,st.marking_row_id
									,st.marking_sap_id
									,st.marking_production_date
									,st.marking_expiration_date
									,st.dt
							having sum(st.marking_kg - isnull(st.marking_log_shipment_kg, 0)) = sum(st.marking_after_shipment_kg)
						) as st 
					where st.first_date_surplus = 1	
					group by 
							 st.marking_data_type
							,st.marking_sap_id
							,st.marking_production_date
							,st.marking_expiration_date
							,st.dt;

			end;


			-- ОТЧЕТ 
			select 
					 'Ключ для excel'			= convert(varchar(7), convert(int, ud.production_date)) +  sp.product_1C_full_name +  convert(varchar(3), ud.expiration_date_in_days)
					,'Тип данных ->'			= ud.data_type
					,'Дата выработки ->'		= ud.production_date
					,'Продукт'					= sp.product_1C_full_name
					,'Дата выработки'			= ud.production_date
					,'Годен до'					= ud.expiration_date
					,'Срок годности, дней'		= ud.expiration_date_in_days
					,'Дата начало профицита'	= ud.date_not_shipped
					,'Профицит,кг'				= ud.surplus_kg
					
			from #union_data as ud
			join info_view.sap_id as sp on ud.sap_id = sp.sap_id_for_join





end;


			