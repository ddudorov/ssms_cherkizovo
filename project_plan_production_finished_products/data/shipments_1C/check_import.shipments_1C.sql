use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.shipments_1C

go

alter procedure check_import.shipments_1C								
as
BEGIN
			SET NOCOUNT ON;

			-- удаляем, адреса доставки, так как это внутр перемещения
			delete 
			from project_plan_production_finished_products.data_import.shipments_1C 
			where shipment_customer_name	in ('ТД ЧЕРКИЗОВО ООО') 
			  and shipment_delivery_address in ( '107143, Москва г, Пермская ул, вл. 5'
												,', Москва г, Пермская ул., дом № 5'
												,'115372, Москва г, Бирюлевская ул., дом № 38'
												,'107143, Россия, Москва г, Пермская ул; вл. 5');

			delete 
			from project_plan_production_finished_products.data_import.shipments_1C 
			where shipment_customer_name	in ('ЧМПЗ АО')		  
			  and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5');



			-- подтягиваем SAP ID к данным 1С, article_packaging должен быть 1
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

			select 
					 sm.sap_id 
					,sm.active_before
					,sm.article_packaging
					,sp.expiration_date_in_days
					,sp.product_status
					,st.stuffing_id
					,count(sm.sap_id) over (partition by sm.active_before, sm.article_packaging) as check_double_sap_id
			into #sap_id
			from ( 
			
						-- берем таблицу с ручными артикулами где указана дата действия артикула, подтягиваем по исключение другие артикула которые имеют данное исключение
						select distinct
								 sm.sap_id
								,sm.active_before
								,sp.article_packaging
						from project_plan_production_finished_products.info.finished_products_sap_id_manual as sm
						join project_plan_production_finished_products.info.finished_products_sap_id_manual as a on sm.sap_id_shipment_manual = ISNULL(a.sap_id_shipment_manual, a.sap_id)
						join cherkizovo.info.products_sap as sp on a.sap_id = sp.sap_id
						where not sm.active_before is null

						union 

						-- берем таблицу с ручными артикулами, подтягиваем варианты артикулов из другой системы и если у нормального артикула указано исключение отображаем ислючение
						select 
								 isnull(sm.sap_id_shipment_manual, sm.sap_id) as sap_id
								,null as active_before
								,sp.article_packaging
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
			from project_plan_production_finished_products.data_import.shipments_1C as c
			join #sap_id as s on c.article_packaging = s.article_packaging and not s.active_before is null and c.shipment_date <= s.active_before 
			where s.check_double_sap_id = 1;

			-- обновляем остальные
			update c
			set c.sap_id							= s.SAP_id
				,c.stuffing_id						= s.stuffing_id
				,c.sap_id_expiration_date_in_days	= s.expiration_date_in_days
				,c.product_status					= s.product_status
			from project_plan_production_finished_products.data_import.shipments_1C as c
			join #sap_id as s on c.article_packaging = s.article_packaging and s.active_before is null
			where s.check_double_sap_id = 1
			  and c.sap_id is null;




			-- проставляем данные в 1C
			update c
			set c.shipment_sales_channel_name = r.[Название канала сбыта]
				,c.shipment_priority = r.[Приоритет отгрузки]
				,c.shipment_min_KOS = r.[ручной КОС]
			from project_plan_production_finished_products.data_import.shipments_1C as c
			join project_plan_production_finished_products.info_view.customers as r 
				on c.shipment_customer_name = r.[Название контрагента]
				and not r.[Ошибки] like '%Название контрагента дублируется%';

				
			-- разбиваем коробочки на набивки
			begin

					insert into project_plan_production_finished_products.data_import.shipments_1C
					(
							 reason_ignore_in_calculate
							,product_status
							,sap_id
							,sap_id_expiration_date_in_days
							,stuffing_id
							,stuffing_id_box_row_id
							,stuffing_id_box
							,article_packaging
							,shipment_sales_channel_name
							,shipment_customer_name
							,shipment_delivery_address
							,shipment_priority
							,shipment_min_KOS
							,shipment_date
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
							,s.article_packaging
							,s.shipment_sales_channel_name
							,s.shipment_customer_name
							,s.shipment_delivery_address
							,s.shipment_priority
							,s.shipment_min_KOS
							,s.shipment_date
							,s.shipment_kg
							 * (t.stuffing_share_in_box / sum(t.stuffing_share_in_box) over (partition by s.row_id)) as shipment_kg
					from project_plan_production_finished_products.data_import.shipments_1C as s
					join project_plan_production_finished_products.info.stuffing as t on s.stuffing_id = t.stuffing_id_box;

					
					-- проставляем row_id у группа набивок
					update s
					set s.stuffing_id_box_row_id = b.stuffing_id_box_row_id
					from project_plan_production_finished_products.data_import.shipments_1C as s
					join (select distinct stuffing_id_box_row_id
						  from project_plan_production_finished_products.data_import.shipments_1C 
						  where not stuffing_id_box is null) as b on s.row_id = b.stuffing_id_box_row_id;

					
					-- проставляем тип набивки
					update project_plan_production_finished_products.data_import.shipments_1C
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
								when (select top 1 c.check_double_sap_id from #sap_id as c where d.article_packaging = c.article_packaging) > 1 
																				then	'Артикул тары возрощает > 1 SAP ID | '
								when d.sap_id is null							then	'Не найден sap id | '
								when d.stuffing_id is null						then	'Код набивки отсутствует | '
								when d.sap_id_expiration_date_in_days is null	then	'Отсутствует срок годности | '
								else ''
							end
						+ iif(d.shipment_sales_channel_name is null,					'Канал сбыта не присвоен | ', '')
						+ iif(d.shipment_priority is null,								'Отсутствует приоритет отгрузки | ', '')
						+ iif(d.shipment_min_KOS is null,								'Отсутствует КОС | ', '')
						, '')
			from project_plan_production_finished_products.data_import.shipments_1C as d;


			-- удаляем ранее созданную таблицу
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;


			-- выгружаем данные в excel
			select 
					 h.reason_ignore_in_calculate
					,h.product_status
					,h.sap_id_text
					,sp.product_1C_full_name
					,h.stuffing_id
					,h.article_packaging
					,h.shipment_sales_channel_name
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
			from project_plan_production_finished_products.data_import.shipments_1C as h
			join project_plan_production_finished_products.data_import.info_excel as ie on h.name_table = ie.name_table
			left join cherkizovo.info.products_sap as sp on h.sap_id = sp.sap_id
			where h.stuffing_id_box_type in (0, 1);

end;


