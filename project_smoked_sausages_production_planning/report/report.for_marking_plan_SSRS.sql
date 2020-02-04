use project_plan_production_finished_products 

go

-- exec .report.for_marking_plan_SSMS
-- exec .report.for_marking_plan @type_report = 'report_for_pivot'

ALTER PROCEDURE report.for_marking_plan_SSMS
as
BEGIN
			SET NOCOUNT ON;
			
			-- для теста

			declare @report_dt_from datetime;	set @report_dt_from =	(select top 1 dt 
																		 from (	select  isnull(min(stuffing_production_date_to),'29990101') as dt  from .data_import.stuffing_fact union all
																				select  isnull(min(stuffing_production_date_to),'29990101') as dt  from .data_import.stuffing_plan ) as s 
																		 order by dt);																 
			declare @report_dt_to datetime;		set @report_dt_to =		(select			max(shipment_date)						from .data_import.shipment);																 
			declare @dt_while as datetime;

			declare @sql varchar(max);
			
			declare @sql_iif			varchar(500);
			declare @sql_name_column	varchar(500);
			--------------------
			-- ПОДГОТОВКА ДАННЫХ
			--------------------

			-- НАБИВКИ
			begin
					IF OBJECT_ID('tempdb..#stuffing','U') is not null drop table #stuffing;

					select 
							 s.stuffing_sap_id as sap_id
							,s.stuffing_id
							,s.stuffing_production_date_to
							,sum(s.stuffing_surplus_kg) as stuffing_surplus_kg
							,sum(s.stuffing_marking_kg) as stuffing_marking_kg
					into #stuffing
					from (
							select 
									 s.stuffing_sap_id
									,s.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_to
									,s.stuffing_available_date
									--,s.stuffing_kg
									--,s.stuffing_marking_kg
									--,s.stuffing_shipment_kg
									,s.stuffing_surplus_kg
									,case when not s.stuffing_sap_id is null then   nullif( isnull( s.stuffing_marking_kg, 0) + isnull( s.stuffing_shipment_kg, 0) , 0)   end as stuffing_marking_kg
							from .data_import.stuffing_fact as s
							
							union all
							select 
									 s.stuffing_sap_id
									,s.stuffing_id
									,s.stuffing_production_name
									,s.stuffing_production_date_to
									,s.stuffing_available_date
									--,s.stuffing_kg
									--,s.stuffing_marking_kg
									--,s.stuffing_shipment_kg
									,s.stuffing_surplus_kg
									,case when not s.stuffing_sap_id is null then   nullif( isnull( s.stuffing_marking_kg, 0) + isnull( s.stuffing_shipment_kg, 0) , 0)   end as stuffing_marking_kg
							from .data_import.stuffing_plan as s
						 ) as s
					group by 
							 s.stuffing_sap_id
							,s.stuffing_id
							,s.stuffing_production_date_to;

			end;

			-- ПОТРЕБНОСТЬ К ОТГРУЗКЕ
			begin

						IF OBJECT_ID('tempdb..#shipments','U') is not null drop table #shipments;

						select
								 p.sap_id
								,p.stuffing_id	
								,p.stuffing_id_box	
								,p.stuffing_production_date_to
								,p.stuffing_available_date
								,sum(p.shipment_kg) as shipment_kg
								,sum(p.net_need_kg) as net_need_kg
						into #shipments
						from (

								-- потребность которая не распределилась
								select 
										 p.shipment_sap_id					as sap_id
										--,p.shipment_stuffing_id				as stuffing_id
										,case 
												when p.shipment_stuffing_id_box_type = 0 then p.shipment_stuffing_id
												when p.shipment_stuffing_id_box_type = 1 then p.shipment_stuffing_id
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_stuffing_id_box
										 end as stuffing_id
										,case 
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_stuffing_id
										 end as stuffing_id_box
										,p.shipment_date - sf.transit_days - sf.packaging_days as stuffing_production_date_to
										,p.shipment_date as stuffing_available_date
										,case 
												when p.shipment_stuffing_id_box_type = 0 then p.shipment_after_stuffing_plan_kg
												--when p.shipment_stuffing_id_box_type = 1 then p.shipment_after_stock_kg
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_kg
										 end as shipment_kg
										,case 
												when p.shipment_stuffing_id_box_type = 0 then p.shipment_after_stuffing_plan_kg
												--when p.shipment_stuffing_id_box_type = 1 then p.shipment_after_stuffing_plan_kg
												when p.shipment_stuffing_id_box_type = 2 then p.shipment_after_stock_kg
										 end as net_need_kg
								from .data_import.shipment as p 
								join .info.stuffing as sf on p.shipment_stuffing_id = sf.stuffing_id
								where p.shipment_delete = 0		
									and p.shipment_stuffing_id_box_type in (0 ,2)
									and not p.shipment_sap_id is null 
									and not p.shipment_stuffing_id is null 					 
									and not isnull(p.shipment_product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
									and ISNUMERIC(left(isnull(p.shipment_stuffing_id,''), 5)) = 1	
								    --and p.shipment_kg is not null
									
									
								union all
								 
								-- потребность из остатков она то и будет чистой после отгрузки остатков, есть аналог в группе, потребность закрылась из другого артикула
								select 
										 st.stock_sap_id
										,st.stock_stuffing_id
										,null as stuffing_id_box
										,l.shipment_date - sf.transit_days - sf.packaging_days as stuffing_production_date_to
										,l.shipment_date as stuffing_available_date
										,l.stock_shipment_kg as shipment_kg
										,null as net_need_kg
								from .data_import.stock_log_calculation as l
								join .data_import.stock					as st on l.stock_row_id = st.stock_row_id
								join .info.stuffing						as sf on st.stock_stuffing_id = sf.stuffing_id
								join .info_view.sap_id					as ps on st.stock_sap_id = ps.sap_id_for_join 
																	and not isnull(ps.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
																	and ISNUMERIC(left(isnull(st.stock_stuffing_id,''), 5)) = 1	
								where not l.shipment_row_id in (select s.shipment_row_id
																from .data_import.shipment as s
																where not s.shipment_stuffing_id_box_row_id is null) -- исключаем коробки

								union all
								 
								-- потребность из набивок факт, так как есть приоритеты и артикул может изменить
								select 
										 l.stuffing_sap_id
										,st.stuffing_id
										,null as stuffing_id_box
										,l.shipment_date - DATEDIFF(day,st.stuffing_production_date_to, st.stuffing_available_date) as stuffing_production_date_to
										,l.shipment_date as stuffing_available_date
										,l.stuffing_shipment_kg as shipment_kg
										,l.stuffing_shipment_kg as net_need_kg
								from .data_import.stuffing_fact_log_calculation as l
								join .data_import.stuffing_fact	as st on l.stuffing_sap_id = st.stuffing_sap_id and l.stuffing_row_id = st.stuffing_sap_id_row_id
								where not l.shipment_row_id in (select s.shipment_row_id
																from .data_import.shipment as s
																where not s.shipment_stuffing_id_box_row_id is null)

								union all
								 
								-- потребность из набивок план, так как есть приоритеты и артикул может изменить
								select 
										 l.stuffing_sap_id
										,st.stuffing_id
										,null as stuffing_id_box
										,l.shipment_date - DATEDIFF(day,st.stuffing_production_date_to, st.stuffing_available_date) as stuffing_production_date_to
										,l.shipment_date as stuffing_available_date
										,l.stuffing_shipment_kg as shipment_kg
										,l.stuffing_shipment_kg as net_need_kg
								from .data_import.stuffing_plan_log_calculation as l
								join .data_import.stuffing_plan	as st on l.stuffing_sap_id = st.stuffing_sap_id and l.stuffing_row_id = st.stuffing_sap_id_row_id
								where not l.shipment_row_id in (select s.shipment_row_id
																from .data_import.shipment as s
																where not s.shipment_stuffing_id_box_row_id is null) -- исключаем коробки


							) as p join .info.stuffing as st on p.stuffing_id = st.stuffing_id
						group by  
								 p.sap_id
								,p.stuffing_id
								,p.stuffing_id_box
								,p.stuffing_production_date_to
								,p.stuffing_available_date;


			end;
		
			-- СТОЛБЦЫ ДЛЯ ОТЧЕТА
			begin

						IF OBJECT_ID('tempdb..#columns','U') is not null drop table #columns;		
						-- select * from #columns where sap_id = 000000001030603716300101 order by 1,2	
						-- select stuffing_id, stuffing_id_box, sap_id  from #columns order by 1,2


						with clm as 
						(
								select distinct stuffing_id, stuffing_id_box, sap_id from #shipments 

								union
								
								select distinct stuffing_id_box, null, sap_id from #shipments where not stuffing_id_box is null

								union 

								select distinct stuffing_id, null,			  sap_id from #stuffing

								union 

								select 
										 sp.stuffing_id
										,null			  
										,sp.sap_id 
								from info_view.sap_id as sp	
								where not isnull(sp.product_status,'') in ('БлокирДляЗаготов/Склада','Устаревший')
								  and sp.sap_id_corrected_need is null
								  and not sp.sap_id_for_join is null
								  and ISNUMERIC(LEFT(sp.stuffing_id, 5)) = 1 
								 
						)

						select 
								 c.stuffing_id
								,c.stuffing_id_box
								,t.mml
								,t.stuffing_name
								,t.production_name as stuffing_production_name
								,t.stuffing_type
								,t.stuffing_group	

								,t.fermentation_and_maturation_days as maturation_days
								,t.fermentation_and_maturation_days + t.packaging_days as maturation_and_packaging_days
								,t.transit_days as transit_from_production_days

								,t.chamber_count as count_chamber
								,t.stuffing_minimum_volume_kg as  minimum_preparation_materials_kg
								,t.chamber_minimum_volume_kg as minimum_volume_for_chamber_kg
								
								,t.marking_minimum_kg as minimum_volume_for_marking_kg
								,t.marking_step_kg as step_marking_kg
								,t.marking_line_productivity_kg
								,t.marking_line_type

								,c.sap_id
								,convert(bigint,p.position_dependent_id) * 100 + p.individual_marking_id as position_dependent_id_and_individual_marking_id
								,p.individual_marking_name
								,p.product_1C_full_name
								,p.production_full_name as product_production_name
								,p.expiration_date_in_days

						into #columns
						from clm as c
						--from (
						--		select stuffing_id, stuffing_id_box, sap_id	from clm						
						--		union
						--		select stuffing_id, null		   , null	from clm
						--	 ) as c
						left join .info.stuffing as t on c.stuffing_id = t.stuffing_id
						left join info_view.sap_id as p on c.sap_id = p.sap_id_for_join
			end;



			--select
			--		 c.stuffing_id	
			--		,c.stuffing_id_box	
			--		,c.mml	
			--		,c.stuffing_name	
			--		,c.stuffing_production_name	
			--		,c.stuffing_type	
			--		,c.stuffing_group	
			--		,c.maturation_days	
			--		,c.maturation_and_packaging_days	
			--		,c.transit_from_production_days	
			--		,c.count_chamber	
			--		,c.minimum_preparation_materials_kg	
			--		,c.minimum_volume_for_chamber_kg	
			--		,c.minimum_volume_for_marking_kg	
			--		,c.step_marking_kg	
			--		,c.marking_line_productivity_kg	
			--		,c.marking_line_type	
			--		,c.sap_id	
			--		,c.position_dependent_id_and_individual_marking_id	
			--		,c.individual_marking_name	
			--		,c.product_1C_full_name	
			--		,c.product_production_name	
			--		,c.expiration_date_in_days
			--from #columns as c


			select 
					 sh.sap_id	
					,sh.stuffing_id	
					,sh.stuffing_id_box	
					,sh.stuffing_production_date_to	
					,sh.stuffing_available_date	
					,sh.shipment_kg	
					,sh.net_need_kg

					
					,c.mml	
					,c.stuffing_name	
					,c.stuffing_production_name	
					,c.stuffing_type	
					,c.stuffing_group	
					,c.maturation_days	
					,c.maturation_and_packaging_days	
					,c.transit_from_production_days	
					,c.count_chamber	
					,c.minimum_preparation_materials_kg	
					,c.minimum_volume_for_chamber_kg	
					,c.minimum_volume_for_marking_kg	
					,c.step_marking_kg	
					,c.marking_line_productivity_kg	
					,c.marking_line_type						
					,c.position_dependent_id_and_individual_marking_id	
					,c.individual_marking_name	
					,c.product_1C_full_name	
					,c.product_production_name	
					,c.expiration_date_in_days

			from #shipments as sh
			join  #columns as c on sh.sap_id = c.sap_id

			--select *
			--from #stuffing as st





			
end;

		




