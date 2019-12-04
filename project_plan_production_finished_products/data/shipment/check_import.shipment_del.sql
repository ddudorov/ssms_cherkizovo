use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.shipments @shipment_data_type = 'shipments_1C'

go

alter procedure check_import.shipments @shipment_data_type	VARCHAR(30)	
									  ,@path_file			varchar(300) = null
									  ,@data_on_date		datetime = null	
as
BEGIN


					


			 --ЗАЯВКИ 1С: ПРОСТАВЛЯЕМ ИНФОРМАЦИЮ ПО КЛИЕНТАМ 
			if @shipment_data_type = 'shipments_1C'
			begin  

						-- проставляем данные в 1C
						update c
						set c.shipment_sales_channel_name = r.[Название канала сбыта]
							,c.shipment_priority = r.[Приоритет отгрузки]
							,c.shipment_min_KOS = r.[ручной КОС]
						from project_plan_production_finished_products.data_import.shipments as c
						join project_plan_production_finished_products.info_view.customers as r 
							on c.shipment_customer_name = r.[Название контрагента]
							and c.shipment_data_type = 'shipments_1C'	
							and not r.[Ошибки] like '%Название контрагента дублируется%';

			end;



			 -- В ПЛАНЕ ПРОДАЖ: СЧИТАЕМ ДАТУ ОТГРУЗКИ C ФИЛИАЛА / УДАЛЯЕМ ОТГРУЗКИ ПОСЛЕ ДАТЫ ОТГРУЗКИ ЗАЯВОК 
			if @shipment_data_type = 'shipments_sales_plan1'
			begin  

						-- СЧИТАЕМ ДАТУ ОТГРУЗКИ C ФИЛИАЛА
						update c
						set c.shipment_date = DATEADD(day, -b.to_branch_days, c.shipment_with_branch_date)
						from project_plan_production_finished_products.data_import.shipments as c
						join project_plan_production_finished_products.info.branches as b on c.shipment_branch_id = b.branch_id and not c.shipment_branch_id is null;
				
						-- УДАЛЯЕМ ОТГРУЗКИ ПОСЛЕ ДАТЫ ОТГРУЗКИ ЗАЯВОК 
						delete s
						from project_plan_production_finished_products.data_import.shipments as s
						join (
								select max(data_on_date) + 1  as shipment_date
								from project_plan_production_finished_products.data_import.data_type
								where data_type in ('shipments_SAP', 'shipments_1C')
							 ) as d on s.shipment_data_type	= 'shipments_sales_plan' and s.shipment_date <= d.shipment_date;

			end; 
		




		
			---- пишем ошибки ---------------------------------------------------------------
			update d
			Set d.shipment_reason_ignore_in_calculate = null
			from project_plan_production_finished_products.data_import.shipments as d;

			update d
			Set d.shipment_reason_ignore_in_calculate = 
				nullif(
							case 
								when d.shipment_sap_id is null							then	'Отсутствует sap id | '
								when d.shipment_stuffing_id is null						then	'Код набивки отсутствует | '
								when d.shipment_sap_id_expiration_date_in_days is null	then	'Отсутствует срок годности | '
								else ''
							end
						+ iif(d.shipment_sales_channel_name is null,			'Канал сбыта не присвоен | ', '')
						+ iif(d.shipment_priority is null,						'Отсутствует приоритет отгрузки | ', '')
						+ iif(d.shipment_min_KOS is null,						'Отсутствует КОС | ', '')

						, '')
			from project_plan_production_finished_products.data_import.shipments as d;



		







			-- выводим отчет показывающий, что загрузили
			--declare @shipment_data_type	VARCHAR(30); set @shipment_data_type = 'shipments_sales_plan'

			declare @sql varchar(3000);
				set @sql = ''
				set @sql = @sql + 'select s.shipment_reason_ignore_in_calculate
										 ,s.shipment_sap_id
										 ,s.shipment_product_status
										 ,s.shipment_sap_id_expiration_date_in_days
										 ,s.shipment_stuffing_id'
								
				if @shipment_data_type = 'shipments_sales_plan'
				set @sql = @sql + '		 ,s.shipment_promo_status
										 ,s.shipment_promo
										 ,s.shipment_promo_kos_listing'

				if @shipment_data_type = 'shipments_SAP'
				set @sql = @sql + '		 ,s.position_dependent_id
										 ,s.individual_marking_id'

								--,s.article_nomenclature
								--,s.article_packaging
								--,s.product_finished_id

				if @shipment_data_type in ('shipments_sales_plan')
				set @sql = @sql + '		 ,s.shipment_branch_id
										 ,s.shipment_branch_name'

				set @sql = @sql + '		 ,s.shipment_sales_channel_id
										 ,s.shipment_sales_channel_name
										 ,s.shipment_customer_id
										 ,s.shipment_customer_name'
				
				if @shipment_data_type in ('shipments_SAP', 'shipments_1C')
				set @sql = @sql + '		 ,s.shipment_delivery_address'

				set @sql = @sql + '		 ,s.shipment_priority
										 ,s.shipment_min_KOS'

								--,s.shipment_with_branch_date
				set @sql = @sql + '		 ,s.shipment_date
										 ,s.shipment_kg
										 ,d.path_file
										 ,d.data_on_date '
				set @sql = @sql + 'from project_plan_production_finished_products.data_import.shipments as s 
								   join project_plan_production_finished_products.data_import.data_type as d on s.shipment_data_type = d.data_type 
																											and s.shipment_data_type = ''' + @shipment_data_type + ''''


				exec (@sql);


end;









