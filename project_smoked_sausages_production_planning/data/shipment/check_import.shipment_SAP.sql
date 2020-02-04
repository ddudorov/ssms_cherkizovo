use project_plan_production_finished_products

-- exec check_import.shipment_SAP

go

alter procedure check_import.shipment_SAP @path_file	varchar(300) = null
										 ,@data_on_date	datetime = null							
as
BEGIN

			SET NOCOUNT ON;
			
			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ФАЙЛЕ И ВЫГРУЖАЕМ ТАБЛИЦУ ДЛЯ ЗАГРУЗКИ
			if not @path_file is null	 
			begin
						-- удаляем данные
						delete data_import.data_type where data_type = 'shipment_SAP';
						
						-- добавляем данные
						insert into data_import.data_type
							   (	data_type,  source_data,  path_file,  data_on_date)
						values ('shipment_SAP',	    'Excel', @path_file, @data_on_date);
			
						-- удаляем и выгружаем
						delete from data_import.shipment where shipment_data_type = 'shipment_SAP';
						select top 0 * from .data_import.shipment;

						return(0);
			end;
			
			
			-- УДАЛЯЕМ: АДРЕСА ДОСТАВКИ, ТАК КАК ЭТО ВНУТР ПЕРЕМЕЩЕНИЯ 
			begin 

						delete 
						from .data_import.shipment
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
						update .data_import.shipment
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
						from .data_import.shipment as d
						join .data_import.data_type as ie on d.shipment_data_type = ie.data_type
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
						from info.customers as c
						join #customers_from_sap as d 
						  on c.customer_id = d.shipment_customer_id
						 and c.sales_channel_name = d.shipment_sales_channel_name;


						-- добавляем новых
						insert into info.customers
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
											from info.customers as c
											where d.shipment_customer_id = c.customer_id
												and d.shipment_sales_channel_name = c.sales_channel_name); 

						
						IF OBJECT_ID('tempdb..#customers_from_sap','U') is not null drop table #customers_from_sap;
					

						-- Заявки SAP более 2 дней приоритет берем из справочника
						update sh
						set sh.shipment_priority = c.manual_shipment_priority
						from data_import.shipment as sh
						join (
								select 
									 customer_id
									,sales_channel_name
									,manual_shipment_priority
								from info.customers
								where not manual_shipment_priority is null
							 ) as c on sh.shipment_customer_id = c.customer_id and sh.shipment_sales_channel_name = c.sales_channel_name
						where sh.shipment_data_type ='shipment_SAP'
						  and sh.shipment_date > (select data_on_date + 1
												  from data_import.data_type
												  where data_type ='shipment_SAP');


			end;


			-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ SAP
			begin 
						IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

						-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ SAP
						select 
								 sp.sap_id 
								,sp.need_before_date
								,sp.position_dependent_id
								,sp.individual_marking_id
								,sp.expiration_date_in_days
								,sp.product_status
								,sp.stuffing_id
								,count(sp.sap_id) over (partition by sp.need_before_date, sp.position_dependent_id, sp.individual_marking_id) as check_double_sap_id
						into #sap_id
						from ( 
			
									-- берем таблицу с ручными артикулами где указана дата действия артикула, подтягиваем по исключение другие артикула которые имеют данное исключение

									select 
											 c.sap_id
											,c.need_before_date
											,c.position_dependent_id
											,c.individual_marking_id
											,sp.expiration_date_in_days
											,sp.product_status
											,sp.stuffing_id
									from (
											SELECT distinct
												 max(iif(not need_before_date is null, sap_id_corrected, null)) over (partition by sap_id) as sap_id
												,max(iif(not need_before_date is null, need_before_date, null)) over (partition by sap_id) as need_before_date
												,position_dependent_id
												,individual_marking_id
												--,sap_ID
												--,sap_id_corrected
											FROM info_view.sap_id
											where sap_id_type in ('Основной', 'Потребность')
											  and sap_id_corrected_need is null
										 ) as c
									join info_view.sap_id as sp on c.sap_id = sp.sap_id and sp.sap_id_type = 'Основной'
									where not c.need_before_date is null

									union all

									SELECT distinct
											 c.sap_id
											,null as need_before_date
											,c.position_dependent_id
											,c.individual_marking_id
											,sp.expiration_date_in_days
											,sp.product_status
											,sp.stuffing_id

									FROM info_view.sap_id as c
									join info_view.sap_id as sp on c.sap_id = sp.sap_id and sp.sap_id_type = 'Основной'
									where c.sap_id_type in ('Основной', 'Потребность')
									  and c.sap_id_corrected_need is null


							 ) as sp;






						-- обновляем данные до даты
						update c
						set c.shipment_sap_id							= s.SAP_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from data_import.shipment as c
						join #sap_id as s on c.position_dependent_id = s.position_dependent_id and c.individual_marking_id = s.individual_marking_id and not s.need_before_date is null and c.shipment_date <= s.need_before_date 
						where s.check_double_sap_id = 1 and c.shipment_data_type ='shipment_SAP';


						-- обновляем остальные
						update c
						set c.shipment_sap_id							= s.SAP_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from data_import.shipment as c
						join #sap_id as s on c.position_dependent_id = s.position_dependent_id and c.individual_marking_id = s.individual_marking_id and s.need_before_date is null
						where s.check_double_sap_id = 1 and c.shipment_data_type ='shipment_SAP' and c.shipment_sap_id is null;


			end;
			

			---- РАЗБИВАЕМ КОРОБОЧКИ НА НАБИВКИ
			begin
					
					IF OBJECT_ID('tempdb..#stuffing_share_box','U') is not null drop table #stuffing_share_box;

					select 
							 s.stuffing_id as stuffing_id_box
							,s.stuffing_box_1 as stuffing_id
							,s.stuffing_share_box_1 / sum(s.stuffing_share_box_1) over (partition by s.stuffing_id) as stuffing_share_in_box
					into #stuffing_share_box
					from (
							select stuffing_id, stuffing_box_1, stuffing_share_box_1 from info.stuffing where not stuffing_box_1 is null union all
							select stuffing_id, stuffing_box_2, stuffing_share_box_2 from info.stuffing where not stuffing_box_2 is null union all
							select stuffing_id, stuffing_box_3, stuffing_share_box_3 from info.stuffing where not stuffing_box_3 is null
						 ) as s;



					IF OBJECT_ID('tempdb..#stuffing_box','U') is not null drop table #stuffing_box;
					select 
							 s.shipment_row_id							
							,s.shipment_data_type							
							,s.shipment_reason_ignore_in_calculate		
							,s.shipment_delete							
							
							,s.shipment_sap_id							
							,s.shipment_product_status					
							,s.shipment_sap_id_expiration_date_in_days	
							
							,t.stuffing_id				as shipment_stuffing_id						
							,s.shipment_stuffing_id		as shipment_stuffing_id_box					
							,s.shipment_row_id			as shipment_stuffing_id_box_row_id			
							,2							as shipment_stuffing_id_box_type				
							
							,s.shipment_promo_status						
							,s.shipment_promo								
							,s.shipment_promo_kos_listing					
							
							,s.sap_id_from_sales_plan										
							,s.position_dependent_id						
							,s.individual_marking_id						
							,s.article_nomenclature						
							,s.article_packaging							
							,s.product_finished_id						
							
							,s.shipment_branch_id							
							,s.shipment_branch_name						
							,s.shipment_sales_channel_id					
							,s.shipment_sales_channel_name				
							,s.shipment_customer_id						
							,s.shipment_customer_name						
							,s.shipment_delivery_address					
							
							,s.shipment_priority							
							,s.shipment_min_KOS							
							
							,s.shipment_with_branch_date					
							,s.shipment_date	
							,s.shipment_kg * t.stuffing_share_in_box as shipment_kg
					into #stuffing_box
					from data_import.shipment as s
					join #stuffing_share_box as t on s.shipment_stuffing_id = t.stuffing_id_box
					where s.shipment_data_type ='shipment_SAP';



					-- собираеми список полей
					declare @sql_columns_stuffing_box varchar(1000); set @sql_columns_stuffing_box = '';

						select @sql_columns_stuffing_box = @sql_columns_stuffing_box + t.name  + ','
						from tempdb.dbo.syscolumns as t
						where id = object_id('tempdb..#stuffing_box')
							and t.name <> 'shipment_row_id';

						set @sql_columns_stuffing_box = left(@sql_columns_stuffing_box,len(@sql_columns_stuffing_box) - 1)

					-- ВСТАВЛЯЕМ ДАННЫЕ
					declare @sql_insert_stuffing_box varchar(max);
						set @sql_insert_stuffing_box = 'insert into data_import.shipment ( ' + @sql_columns_stuffing_box + ' )  
														select ' + @sql_columns_stuffing_box + ' from #stuffing_box as upv'
						exec(@sql_insert_stuffing_box);

						update data_import.shipment
						set shipment_stuffing_id_box_type = 1
						   ,shipment_stuffing_id_box_row_id = shipment_row_id
						where shipment_row_id in (select shipment_stuffing_id_box_row_id from #stuffing_box);

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
					from data_import.shipment as d
					where d.shipment_data_type ='shipment_SAP';


			end;
			
			-- добавляем данные в общию таблицу, которую выводим на форму
			exec report.for_form

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
						from data_import.shipment as h
						join data_import.data_type as ie on h.shipment_data_type = ie.data_type and h.shipment_data_type = 'shipment_SAP'
						left join info_view.sap_id as sp on h.shipment_sap_id = sp.sap_id_for_join
						where h.shipment_stuffing_id_box_type in (0, 1);

			end;




end;


