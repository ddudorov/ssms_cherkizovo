use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.shipments @shipment_data_type = 'shipments_1C'

go

alter procedure check_import.shipments @shipment_data_type	VARCHAR(30)	
									  ,@path_file			varchar(300) = null
									  ,@data_on_date		datetime = null	
as
BEGIN


			SET NOCOUNT ON;
			
			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ФАЙЛЕ И ВЫГРУЖАЕМ ТАБЛИЦУ
			if @shipment_data_type in ('shipments_SAP', 'shipments_1C', 'shipments_sales_plan') and not @path_file is null	 
			begin
						-- удаляем данные
						delete project_plan_production_finished_products.data_import.data_type where data_type = @shipment_data_type;
						
						-- добавляем данные
						insert into project_plan_production_finished_products.data_import.data_type
							   (		  data_type,  source_data,  path_file,  data_on_date)
						values (@shipment_data_type,	  'Excel', @path_file, @data_on_date);
			
						-- удаляем и выгружаем
						delete from project_plan_production_finished_products.data_import.shipments where shipment_data_type = @shipment_data_type;
						select top 0 * from project_plan_production_finished_products.data_import.shipments;

						return(0);
			end;	
					
			


			-- УДАЛЯЕМ: АДРЕСА ДОСТАВКИ, ТАК КАК ЭТО ВНУТР ПЕРЕМЕЩЕНИЯ 
			begin 

					delete 
					from project_plan_production_finished_products.data_import.shipments
					where (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Москва г, Пермская ул, вл. 5'))
					   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in (', Москва г, Пермская ул., дом № 5'))
					   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('115372, Москва г, Бирюлевская ул., дом № 38'))
					   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
					   or (shipment_customer_name in ('ЧМПЗ АО')		  and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
					   or (isnull(shipment_kg, 0) = 0);

			end;


			 -- В ЗАЯВКАХ SAP: ДОБАВЛЯЕМ НАЗВАНИЕ КАНАЛА ПРОДАЖ / ОБНОВЛЯЕМ СПИСОК КЛИЕНТОВ / ДОБАВЛЯЕМ НОВЫХ КЛИЕНТОВ В СПРАВОЧНИК
			if @shipment_data_type = 'shipments_SAP'
			begin  
						-- добавляем наименование канала сбыта
						update project_plan_production_finished_products.data_import.shipments
						set shipment_sales_channel_name = case shipment_sales_channel_id
																when 10 then 'Внутрихолдинговый'
																when 11 then 'Сети'
																when 12 then 'Розница'
																when 13 then 'HoReCa'
																when 14 then 'Опт'
																when 15 then 'Экспорт'
																when 16 then 'Прочие продажи'
																when 17 then 'Дистрибьютор'									
														  end	
						where shipment_data_type ='shipments_SAP';



						-- создаем таблицу по клиентам
						IF OBJECT_ID('tempdb..#customers_from_sap','U') is not null drop table #customers_from_sap;

						select 
								 d.shipment_customer_id	
								,d.shipment_customer_name
								,d.shipment_sales_channel_name	
								,min(d.shipment_priority) as shipment_priority
								,min(d.shipment_min_KOS)  as SAP_min_KOS
								,max(d.shipment_min_KOS)  as SAP_max_KOS
								,'Заявки SAP от ' + FORMAT(min(ie.data_on_date),'dd.MM.yyyy') as source_insert		
						into #customers_from_sap
						from project_plan_production_finished_products.data_import.shipments as d
						join project_plan_production_finished_products.data_import.data_type as ie on d.shipment_data_type = ie.data_type
						where not d.shipment_customer_id is null 
							and not d.shipment_sales_channel_name is null			
							and d.shipment_data_type = 'shipments_SAP'	
						group by 					
								 d.shipment_customer_id	
								,d.shipment_customer_name
								,d.shipment_sales_channel_name;	
					


						-- обновляем справочник клиентов
						update c
						set	 c.shipment_priority	= d.shipment_priority	
							,c.SAP_min_KOS			= d.SAP_min_KOS	
							,c.SAP_max_KOS			= d.SAP_max_KOS	
							,c.dt_tm_change			= getdate()
							,c.source_insert		= d.source_insert	
						from project_plan_production_finished_products.info.customers as c
						join #customers_from_sap as d 
						  on c.customer_id = d.shipment_customer_id
						 and c.sales_channel_name = d.shipment_sales_channel_name;


						-- добавляем новых
						insert into project_plan_production_finished_products.info.customers
						(
								 customer_id
								,customer_name
								,sales_channel_name	
								,shipment_priority
								,SAP_min_KOS
								,SAP_max_KOS
								,manual_KOS
								,source_insert
						)
						select 
								 d.shipment_customer_id			
								,d.shipment_customer_name
								,d.shipment_sales_channel_name	
								,d.shipment_priority
								,d.SAP_min_KOS
								,d.SAP_max_KOS
								,case when d.SAP_min_KOS = d.SAP_max_KOS then d.SAP_min_KOS end as manual_KOS
								,d.source_insert			
						from #customers_from_sap as d
						where not exists (select * 
											from project_plan_production_finished_products.info.customers as c
											where d.shipment_customer_id = c.customer_id
												and d.shipment_sales_channel_name = c.sales_channel_name); 

						
						IF OBJECT_ID('tempdb..#customers_from_sap','U') is not null drop table #customers_from_sap;
					



			end;


			

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









