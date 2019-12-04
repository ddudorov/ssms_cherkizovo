use project_plan_production_finished_products

-- exec project_plan_production_finished_products.check_import.shipment_SAP

go

alter procedure check_import.shipment_SAP @path_file		varchar(300) = null
										 ,@data_on_date	datetime = null							
as
BEGIN

			SET NOCOUNT ON;
			
			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ФАЙЛЕ И ВЫГРУЖАЕМ ТАБЛИЦУ ДЛЯ ЗАГРУЗКИ
			if not @path_file is null	 
			begin
						-- удаляем данные
						delete project_plan_production_finished_products.data_import.data_type where data_type = 'shipment_SAP';
						
						-- добавляем данные
						insert into project_plan_production_finished_products.data_import.data_type
							   (	data_type,  source_data,  path_file,  data_on_date)
						values ('shipment_SAP',	    'Excel', @path_file, @data_on_date);
			
						-- удаляем и выгружаем
						delete from project_plan_production_finished_products.data_import.shipment where shipment_data_type = 'shipment_SAP';
						select top 0 * from project_plan_production_finished_products.data_import.shipment;

						return(0);
			end;






			-- УДАЛЯЕМ: АДРЕСА ДОСТАВКИ, ТАК КАК ЭТО ВНУТР ПЕРЕМЕЩЕНИЯ 
			begin 

						delete 
						from project_plan_production_finished_products.data_import.shipment
						where (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Москва г, Пермская ул, вл. 5'))
						   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in (', Москва г, Пермская ул., дом № 5'))
						   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('115372, Москва г, Бирюлевская ул., дом № 38'))
						   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
						   or (shipment_customer_name in ('ЧМПЗ АО')		  and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
						   or (isnull(shipment_kg, 0) = 0);

			end;


			-- ОБНОВЛЯЕМ СПРАВОЧНИК КЛИЕНТОВ
			begin 
						-- добавляем наименование канала сбыта
						update project_plan_production_finished_products.data_import.shipment
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
						where shipment_data_type ='shipment_SAP';



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
						from project_plan_production_finished_products.data_import.shipment as d
						join project_plan_production_finished_products.data_import.data_type as ie on d.shipment_data_type = ie.data_type
						where not d.shipment_customer_id is null 
						  and not d.shipment_sales_channel_name is null			
						  and d.shipment_data_type = 'shipment_SAP'	
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


			-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ SAP
			begin 
						IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

						-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ SAP
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
						set c.shipment_sap_id							= s.SAP_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from project_plan_production_finished_products.data_import.shipment as c
						join #sap_id as s on c.position_dependent_id = s.position_dependent_id and c.individual_marking_id = s.individual_marking_id and not s.active_before is null and c.shipment_date <= s.active_before 
						where s.check_double_sap_id = 1 and c.shipment_data_type ='shipment_SAP';


						-- обновляем остальные
						update c
						set c.shipment_sap_id							= s.SAP_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from project_plan_production_finished_products.data_import.shipment as c
						join #sap_id as s on c.position_dependent_id = s.position_dependent_id and c.individual_marking_id = s.individual_marking_id and s.active_before is null
						where s.check_double_sap_id = 1 and c.shipment_data_type ='shipment_SAP' and c.shipment_sap_id is null;


			end;


			-- ПИШЕМ ОШИБКИ
			begin
			
					update d
					Set d.shipment_reason_ignore_in_calculate = 
						nullif(
									case 
										when (select top 1 s.check_double_sap_id 
											  from #sap_id as s
											  where d.position_dependent_id = s.position_dependent_id 
											    and d.individual_marking_id = s.individual_marking_id) > 1 then	'Код зависимой позиции & ИМ неуникальны | '
										when d.shipment_sap_id is null							then	'Отсутствует sap id | '
										when d.shipment_stuffing_id is null						then	'Код набивки отсутствует | '
										when d.shipment_sap_id_expiration_date_in_days is null	then	'Отсутствует срок годности | '
										else ''
									end
								+ iif(d.shipment_sales_channel_name is null,			'Канал сбыта не присвоен | ', '')
								+ iif(d.shipment_priority is null,						'Отсутствует приоритет отгрузки | ', '')
								+ iif(d.shipment_min_KOS is null,						'Отсутствует КОС | ', '')

								, '')
					from project_plan_production_finished_products.data_import.shipment as d
					where d.shipment_data_type ='shipment_SAP';


			end;


			---- РАЗБИВАЕМ КОРОБОЧКИ НА НАБИВКИ
			--begin

			--		insert into project_plan_production_finished_products.data_import.shipment
			--		(
			--				 reason_ignore_in_calculate
			--				,product_status
			--				,sap_id
			--				,sap_id_expiration_date_in_days
			--				,stuffing_id
			--				,stuffing_id_box_row_id
			--				,stuffing_id_box

			--				,position_dependent_id
			--				,individual_marking_id
			--				,shipment_delete
			--				,shipment_sales_channel_id
			--				,shipment_customer_id
			--				,shipment_customer_name
			--				,shipment_delivery_address
			--				,shipment_priority
			--				,shipment_date
			--				,shipment_min_KOS
			--				,shipment_kg

			--		)

			--		select 
			--				 s.reason_ignore_in_calculate
			--				,s.product_status
			--				,s.sap_id
			--				,s.sap_id_expiration_date_in_days
			--				,t.stuffing_id
			--				,s.row_id as stuffing_id_box_row_id
			--				,t.stuffing_id_box

			--				,s.position_dependent_id
			--				,s.individual_marking_id
			--				,s.shipment_delete
			--				,s.shipment_sales_channel_id
			--				,s.shipment_customer_id
			--				,s.shipment_customer_name
			--				,s.shipment_delivery_address
			--				,s.shipment_priority
			--				,s.shipment_date
			--				,s.shipment_min_KOS
			--				,s.shipment_kg
			--				 * (t.stuffing_share_in_box / sum(t.stuffing_share_in_box) over (partition by s.row_id)) as shipment_kg
			--		from project_plan_production_finished_products.data_import.shipments_SAP as s
			--		join project_plan_production_finished_products.info.stuffing as t on s.stuffing_id = t.stuffing_id_box;

					
			--		-- проставляем row_id у группа набивок
			--		update s
			--		set s.stuffing_id_box_row_id = b.stuffing_id_box_row_id
			--		from project_plan_production_finished_products.data_import.shipments_SAP as s
			--		join (select distinct stuffing_id_box_row_id
			--			  from project_plan_production_finished_products.data_import.shipments_SAP 
			--			  where not stuffing_id_box is null) as b on s.row_id = b.stuffing_id_box_row_id;

					
			--		-- проставляем тип набивки
			--		update project_plan_production_finished_products.data_import.shipments_SAP
			--		set stuffing_id_box_type = case 
			--										when stuffing_id_box_row_id is null then 0 -- набивка не коробка
			--										when stuffing_id_box is null		then 1 -- набивка коробка
			--										when not stuffing_id_box is null	then 2 -- набивка разбитая на коробки
			--								   end;
			--end;




			-- ВЫГРУЖАЕМ РЕЗУЛЬТАТ
			begin

						select 
								 h.shipment_reason_ignore_in_calculate
								,h.shipment_product_status
								,convert(varchar(24), FORMAT(h.shipment_sap_id, '000000000000000000000000')) as sap_id
								,sp.product_1C_full_name
								,h.shipment_stuffing_id
								,h.position_dependent_id
								,h.individual_marking_id
								--,h.shipment_delete
								--,h.shipment_sales_channel_id
								,h.shipment_sales_channel_name
								,h.shipment_customer_id
								,h.shipment_customer_name
								,h.shipment_delivery_address
								,h.shipment_priority
								,h.shipment_min_KOS
								,h.shipment_date
								,h.shipment_kg
								,ie.path_file
								,ie.data_on_date
						from project_plan_production_finished_products.data_import.shipment as h
						join project_plan_production_finished_products.data_import.data_type as ie on h.shipment_data_type = ie.data_type and h.shipment_data_type = 'shipment_SAP'
						left join cherkizovo.info.products_sap as sp on h.shipment_sap_id = sp.sap_id
						where h.shipment_stuffing_id_box_type in (0, 1);

			end;




end;


