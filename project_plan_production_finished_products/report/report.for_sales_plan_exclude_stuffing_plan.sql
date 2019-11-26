use project_plan_production_finished_products

go


-- exec project_plan_production_finished_products.report.for_sales_plan_exclude_stuffing_plan @type_run = 'select_sum_shipment_kg'
-- exec project_plan_production_finished_products.report.for_sales_plan_exclude_stuffing_plan @type_run = 'select'
-- exec project_plan_production_finished_products.report.for_sales_plan_exclude_stuffing_plan @type_run = 'update', @where = ' isnull(shipment_sales_channel_name,'�� ������') in ('�������','����')'

ALTER PROCEDURE report.for_sales_plan_exclude_stuffing_plan @type_run varchar(50), @where varchar(7000) = ''
											
as
BEGIN
			SET NOCOUNT ON;
			
			declare @sql varchar(max);
			
			-- ��������� ����������
			if @type_run = 'update'
			begin

						update project_plan_production_finished_products.data_import.shipments_sales_plan 
						set shipment_exclude_for_stuffing_plan = 0
						where shipment_exclude_for_stuffing_plan = 1;

						if @where <> ''
						begin
								set @sql = '';
								set @sql = @sql + char(10) + 'update project_plan_production_finished_products.data_import.shipments_sales_plan'
								set @sql = @sql + char(10) + 'set shipment_exclude_for_stuffing_plan = 1'
								set @sql = @sql + char(10) + 'where ' + @where

								exec(@sql);
						end;
						-- ��������� ������ � excel
						select 

								 '������'							= h.reason_ignore_in_calculate
								,'SAP ID'							= h.sap_id_text
								,'��� �������'						= h.stuffing_id
								
								,'������ �����'						= h.shipment_promo_status 
								,'�����/�� �����'					= h.shipment_promo 
								,'�����/�� �����/���/�������'		= h.shipment_promo_kos_listing 

								,'��� ��������� �������'			= h.position_dependent_id
								,'��� �������������� ����������'	= h.individual_marking_id
								,'������� ����'						= h.article_packaging
								,'������� ������������'				= h.article_nomenclature

								,'��� �������'						= h.shipment_branch_id	
								,'�������� �������'					= h.shipment_branch_name
								,'�������� ������ �����'			= h.shipment_sales_channel_name
								,'��� �����������'					= h.shipment_customer_id
								,'�������� �����������'				= h.shipment_customer_name
								,'��������� ��������'				= h.shipment_priority
								,'��� ��������'						= h.shipment_min_KOS
								,'���� �������� � �������'			= h.shipment_with_branch_date
								,'���� ��������'					= h.shipment_date
								,'���� ������, ��'					= h.shipment_kg
								
						from project_plan_production_finished_products.data_import.shipments_sales_plan as h
						where h.stuffing_id_box_type in (0, 1)
						  and h.shipment_exclude_for_stuffing_plan = 1;


			end;


			-- �������� ��� �����
			if @type_run = 'select'
			begin
							
						IF OBJECT_ID('tempdb..#sales_plan','U') is not null drop table #sales_plan; 
				
						select distinct 
								 isnull(convert(varchar(250),shipment_promo_status			),'---') as shipment_promo_status
								,isnull(convert(varchar(250),shipment_promo					),'---') as shipment_promo
								,isnull(convert(varchar(250),shipment_promo_kos_listing		),'---') as shipment_promo_kos_listing
								,isnull(convert(varchar(250),shipment_sales_channel_name	),'---') as shipment_sales_channel_name
								,isnull(convert(varchar(250),shipment_customer_name			),'---') as shipment_customer_name
								,shipment_exclude_for_stuffing_plan
						into #sales_plan
						from project_plan_production_finished_products.data_import.shipments_sales_plan
						where shipment_delete = 0
						  and reason_ignore_in_calculate is null;

							

						select	 'shipment_promo_status'		as ComboBoxSelectColumn
								,'������ �����'					as ComboBoxSelectColumnValue
								,(	select	 shipment_promo_status + '||' from #sales_plan 
									group by shipment_promo_status		   
									order by shipment_promo_status FOR XML PATH('')		  ) as ListBoxSelectData
											
								,isnull(
									(	select	 shipment_promo_status + '||' from #sales_plan where shipment_exclude_for_stuffing_plan = 1	
									group by shipment_promo_status 	  
									order by shipment_promo_status FOR XML PATH('')		  ),'') as ListBoxSelectExclude		
	
						union all

						select	 'shipment_promo'				as ComboBoxSelectColumn
								,'�����/�� �����'				as ComboBoxSelectColumnValue
								,(	select	 shipment_promo + '||' from #sales_plan 
									group by shipment_promo		   
									order by shipment_promo 
									FOR XML PATH('')		  ) as ListBoxSelectData
											
								,isnull(
									(	select	 shipment_promo + '||' from #sales_plan where shipment_exclude_for_stuffing_plan = 1	
									group by shipment_promo 	  
									order by shipment_promo FOR XML PATH('')		  ),'') as ListBoxSelectExclude		
	
						union all

						select	 'shipment_promo_kos_listing'	as ComboBoxSelectColumn
								,'�����/�� �����/���/�������'	as ComboBoxSelectColumnValue
								,(	select	 shipment_promo_kos_listing + '||' from #sales_plan 
									group by shipment_promo_kos_listing		   
									order by shipment_promo_kos_listing 
									FOR XML PATH('')		  ) as ListBoxSelectData
											
								,isnull(
									(	select	 shipment_promo_kos_listing + '||' from #sales_plan where shipment_exclude_for_stuffing_plan = 1	
									group by shipment_promo_kos_listing 	  
									order by shipment_promo_kos_listing FOR XML PATH('')		  ),'') as ListBoxSelectExclude		
	
						union all

						select	 'shipment_sales_channel_name'	as ComboBoxSelectColumn
								,'�������� ������ �����'		as ComboBoxSelectColumnValue
								,(	select	 shipment_sales_channel_name + '||' from #sales_plan 
									group by shipment_sales_channel_name		   
									order by shipment_sales_channel_name 
									FOR XML PATH('')		  ) as ListBoxSelectData
											
								,isnull(
									(	select	 shipment_sales_channel_name + '||' from #sales_plan where shipment_exclude_for_stuffing_plan = 1	
									group by shipment_sales_channel_name 	  
									order by shipment_sales_channel_name FOR XML PATH('')		  ),'') as ListBoxSelectExclude		
	
						union all

						select	 'shipment_customer_name'		as ComboBoxSelectColumn
								,'�������� �����������'			as ComboBoxSelectColumnValue
								,(	select	 shipment_customer_name + '||' from #sales_plan 
									group by shipment_customer_name		   
									order by shipment_customer_name 
									FOR XML PATH('')		  ) as ListBoxSelectData
											
								,isnull(
									(	select	 shipment_customer_name + '||' from #sales_plan where shipment_exclude_for_stuffing_plan = 1	
									group by shipment_customer_name 	  
									order by shipment_customer_name FOR XML PATH('')		  ),'') as ListBoxSelectExclude		
			
						IF OBJECT_ID('tempdb..#sales_plan','U') is not null drop table #sales_plan; 
			end;
			
			-- �������� ��� �����
			if @type_run = 'select_sum_shipment_kg'
			begin
					select FORMAT(isnull(SUM(shipment_kg),0),'#,###') as shipment_kg
					from project_plan_production_finished_products.data_import.shipments_sales_plan
					where shipment_delete = 0
					  and shipment_exclude_for_stuffing_plan = 1
					  and reason_ignore_in_calculate is null;
			end;

end;




