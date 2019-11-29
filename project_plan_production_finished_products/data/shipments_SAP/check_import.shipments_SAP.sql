use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.shipments_SAP

go

alter procedure check_import.shipments_SAP								
as
BEGIN
			SET NOCOUNT ON;

			-- удаляем, адреса доставки, так как это внутр перемещения
			delete 
			from project_plan_production_finished_products.data_import.shipments_SAP
			where shipment_customer_name	in ('ТД ЧЕРКИЗОВО ООО') 
			  and shipment_delivery_address in ( '107143, Москва г, Пермская ул, вл. 5'
												,', Москва г, Пермская ул., дом № 5'
												,'115372, Москва г, Бирюлевская ул., дом № 38'
												,'107143, Россия, Москва г, Пермская ул; вл. 5');

			delete 
			from project_plan_production_finished_products.data_import.shipments_SAP 
			where shipment_customer_name	in ('ЧМПЗ АО')		  
			  and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5');

			delete 
			from project_plan_production_finished_products.data_import.shipments_SAP 
			where isnull(shipment_kg, 0) = 0;


			-- подтягиваем SAP ID к данным SAP
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

			select 
					 sm.sap_id 
					,sm.active_before
					,sm.position_dependent_id
					,sm.individual_marking_id
					,sp.expiration_date_in_days
					,sp.product_status
					,st.stuffing_id
					,count(sm.sap_id) over (partition by sm.active_before, sm.position_dependent_id, sm.individual_marking_id) as check_double_sap_id
			into #sap_id
			from ( 
			
						-- берем таблицу с ручными артикулами где указана дата действия артикула, подтягиваем по исключение другие артикула которые имеют данное исключение
						select distinct
								 sm.sap_id
								,sm.active_before
								,sp.position_dependent_id
								,sp.individual_marking_id
						from project_plan_production_finished_products.info.finished_products_sap_id_manual as sm
						join project_plan_production_finished_products.info.finished_products_sap_id_manual as a on sm.sap_id_shipment_manual = ISNULL(a.sap_id_shipment_manual, a.sap_id)
						join cherkizovo.info.products_sap as sp on a.sap_id = sp.sap_id
						where not sm.active_before is null

						union 

						-- берем таблицу с ручными артикулами, подтягиваем варианты артикулов из другой системы и если у нормального артикула указано исключение отображаем ислючение
						select 
								 isnull(sm.sap_id_shipment_manual, sm.sap_id) as sap_id
								,null as active_before
								,sp.position_dependent_id
								,sp.individual_marking_id
						from project_plan_production_finished_products.info.finished_products_sap_id_manual as sm
						join cherkizovo.info.products_sap as sp on sm.sap_id = sp.sap_id

				 ) as sm 
			join cherkizovo.info.products_sap as sp on sm.sap_id = sp.sap_id
			join project_plan_production_finished_products.info.finished_products_sap_id_manual as st on sm.sap_id = st.sap_id;


			-- обновляем данные до даты
			update c
			set c.sap_id							= s.SAP_id
				,c.stuffing_id						= s.stuffing_id
				,c.sap_id_expiration_date_in_days	= s.expiration_date_in_days
				,c.product_status					= s.product_status
			from project_plan_production_finished_products.data_import.shipments_SAP as c
			join #sap_id as s on c.position_dependent_id = s.position_dependent_id and c.individual_marking_id = s.individual_marking_id and not s.active_before is null and c.shipment_date <= s.active_before 
			where s.check_double_sap_id = 1;

			-- обновляем остальные
			update c
			set c.sap_id							= s.SAP_id
				,c.stuffing_id						= s.stuffing_id
				,c.sap_id_expiration_date_in_days	= s.expiration_date_in_days
				,c.product_status					= s.product_status
			from project_plan_production_finished_products.data_import.shipments_SAP as c
			join #sap_id as s on c.position_dependent_id = s.position_dependent_id and c.individual_marking_id = s.individual_marking_id and s.active_before is null
			where s.check_double_sap_id = 1
			  and c.sap_id is null;



			-- добавляем клиента и канал сбыта если нет в cherkizovo.info.customers
			-- обновляем справочник
			update c
			set	 c.shipment_priority = d.shipment_priority	
				,c.SAP_min_KOS = d.SAP_min_KOS	
				,c.SAP_max_KOS = d.SAP_max_KOS	
				--,c.manual_KOS = iif(d.SAP_min_KOS = d.SAP_max_KOS, d.SAP_min_KOS , c.manual_KOS)
				,c.dt_tm_change = getdate()
				,c.source_insert = d.source_insert	
			from project_plan_production_finished_products.info.customers as c
			join (
					select 
							 d.shipment_customer_id	
							,d.shipment_customer_name
							,d.shipment_sales_channel_name	
							,min(d.shipment_priority) as shipment_priority
							,min(d.shipment_min_KOS)  as SAP_min_KOS
							,max(d.shipment_min_KOS)  as SAP_max_KOS
							,'Заявки SAP от ' + FORMAT(min(ie.date_file),'dd.MM.yyyy') as source_insert		
					from project_plan_production_finished_products.data_import.shipments_SAP as d
					join project_plan_production_finished_products.data_import.info_excel as ie on d.name_table = ie.name_table
					where not d.shipment_customer_id is null
						and not d.shipment_sales_channel_name is null				
					group by 					
							 d.shipment_customer_id	
							,d.shipment_customer_name
							,d.shipment_sales_channel_name	
					) as d on c.customer_id = d.shipment_customer_id
						and c.sales_channel_name = d.shipment_sales_channel_name;

			-- добавляем 
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
					 s.shipment_customer_id			
					,s.shipment_customer_name
					,s.shipment_sales_channel_name	
					,min(s.shipment_priority) as shipment_priority
					,min(s.shipment_min_KOS) as SAP_min_KOS
					,max(s.shipment_min_KOS) as SAP_max_KOS
					,case when min(s.shipment_min_KOS) = max(s.shipment_min_KOS) then min(s.shipment_min_KOS) end as manual_KOS
					,'Заявки SAP от ' + FORMAT(min(ie.date_file),'dd.MM.yyyy') as source_insert			
			from project_plan_production_finished_products.data_import.shipments_SAP as s
			join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table
			where not s.shipment_customer_id is null
				and not s.shipment_sales_channel_name is null	
				and not exists (select * 
								from project_plan_production_finished_products.info.customers as c
								where s.shipment_customer_id = c.customer_id
									and s.shipment_sales_channel_name = c.sales_channel_name)
			group by 					
					 s.shipment_customer_id			
					,s.shipment_customer_name
					,s.shipment_sales_channel_name; 		




			-- разбиваем коробочки на набивки
			begin

					insert into project_plan_production_finished_products.data_import.shipments_SAP
					(
							 reason_ignore_in_calculate
							,product_status
							,sap_id
							,sap_id_expiration_date_in_days
							,stuffing_id
							,stuffing_id_box_row_id
							,stuffing_id_box

							,position_dependent_id
							,individual_marking_id
							,shipment_delete
							,shipment_sales_channel_id
							,shipment_customer_id
							,shipment_customer_name
							,shipment_delivery_address
							,shipment_priority
							,shipment_date
							,shipment_min_KOS
							,shipment_kg

					)

					select 
							 s.reason_ignore_in_calculate
							,s.product_status
							,s.sap_id
							,s.sap_id_expiration_date_in_days
							,t.stuffing_id
							,s.row_id as stuffing_id_box_row_id
							,t.stuffing_id_box

							,s.position_dependent_id
							,s.individual_marking_id
							,s.shipment_delete
							,s.shipment_sales_channel_id
							,s.shipment_customer_id
							,s.shipment_customer_name
							,s.shipment_delivery_address
							,s.shipment_priority
							,s.shipment_date
							,s.shipment_min_KOS
							,s.shipment_kg
							 * (t.stuffing_share_in_box / sum(t.stuffing_share_in_box) over (partition by s.row_id)) as shipment_kg
					from project_plan_production_finished_products.data_import.shipments_SAP as s
					join project_plan_production_finished_products.info.stuffing as t on s.stuffing_id = t.stuffing_id_box;

					
					-- проставляем row_id у группа набивок
					update s
					set s.stuffing_id_box_row_id = b.stuffing_id_box_row_id
					from project_plan_production_finished_products.data_import.shipments_SAP as s
					join (select distinct stuffing_id_box_row_id
						  from project_plan_production_finished_products.data_import.shipments_SAP 
						  where not stuffing_id_box is null) as b on s.row_id = b.stuffing_id_box_row_id;

					
					-- проставляем тип набивки
					update project_plan_production_finished_products.data_import.shipments_SAP
					set stuffing_id_box_type = case 
													when stuffing_id_box_row_id is null then 0 -- набивка не коробка
													when stuffing_id_box is null		then 1 -- набивка коробка
													when not stuffing_id_box is null	then 2 -- набивка разбитая на коробки
											   end;
			end;



			---- пишем ошибки ---------------------------------------------------------------
			update d
			Set d.reason_ignore_in_calculate = 
				nullif(
							case 
								when d.sap_id is null							then	'Не найден sap id | '
								when d.stuffing_id is null						then	'Код набивки отсутствует | '
								when d.sap_id_expiration_date_in_days is null	then	'Отсутствует срок годности | '
								else ''
							end
						+ iif(d.shipment_sales_channel_name is null,			'Канал сбыта не присвоен | ', '')
						+ iif(d.shipment_priority is null,						'Отсутствует приоритет отгрузки | ', '')
						+ iif(d.shipment_min_KOS is null,						'Отсутствует КОС | ', '')

						, '')
			from project_plan_production_finished_products.data_import.shipments_SAP as d;



			-- выгружаем данные в excel
			select 
					 h.reason_ignore_in_calculate
					,h.product_status
					,h.sap_id_text
					,sp.product_1C_full_name
					,h.stuffing_id
					,h.position_dependent_id
					,h.individual_marking_id
					,h.shipment_delete
					,h.shipment_sales_channel_id
					,h.shipment_sales_channel_name
					,h.shipment_customer_id
					,h.shipment_customer_name
					,h.shipment_delivery_address
					,h.shipment_priority
					,h.shipment_min_KOS
					,h.shipment_date
					,h.shipment_kg
					,ie.path_file
					,ie.date_file
					,ie.user_insert
					,ie.dt_tm_insert
			from project_plan_production_finished_products.data_import.shipments_SAP as h
			join project_plan_production_finished_products.data_import.info_excel as ie on h.name_table = ie.name_table
			left join cherkizovo.info.products_sap as sp on h.sap_id = sp.sap_id
			where h.stuffing_id_box_type in (0, 1);


end;


