use project_plan_production_finished_products

--exec check_import.shipment_1C

go

alter procedure check_import.shipment_1C @path_file		varchar(300) = null
										,@data_on_date	datetime = null										
as
BEGIN
			SET NOCOUNT ON;

			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ФАЙЛЕ И ВЫГРУЖАЕМ ТАБЛИЦУ ДЛЯ ЗАГРУЗКИ
			if not @path_file is null	 
			begin
						-- удаляем данные
						delete data_import.data_type where data_type = 'shipment_1C';
						
						-- добавляем данные
						insert into data_import.data_type
							   (	  data_type,  source_data,  path_file,  data_on_date)
						values ('shipment_1C',	  'Excel', @path_file, @data_on_date);
			
						-- удаляем и выгружаем
						delete from data_import.shipment where shipment_data_type = 'shipment_1C';
						select top 0 * from data_import.shipment;

						return(0);
			end;






			-- УДАЛЯЕМ: АДРЕСА ДОСТАВКИ, ТАК КАК ЭТО ВНУТР ПЕРЕМЕЩЕНИЯ 
			begin 

						delete 
						from data_import.shipment
						where (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Москва г, Пермская ул, вл. 5'))
						   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in (', Москва г, Пермская ул., дом № 5'))
						   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('115372, Москва г, Бирюлевская ул., дом № 38'))
						   or (shipment_customer_name in ('ТД ЧЕРКИЗОВО ООО') and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
						   or (shipment_customer_name in ('ЧМПЗ АО')		  and shipment_delivery_address in ('107143, Россия, Москва г, Пермская ул; вл. 5'))
						   or (													  shipment_delivery_address in (', Москва г, Лесная ул., дом № 5'))
						   or (isnull(shipment_kg, 0) = 0);

			end;


			
			-- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ПО КЛИЕНТАМ
			begin
					update c
					set c.shipment_sales_channel_name = r.[Название канала сбыта]
						,c.shipment_priority = r.[Ручной приор отгрузки]
						,c.shipment_min_KOS = r.[ручной КОС]
					from data_import.shipment as c
					join info_view.customers as r 
						on c.shipment_customer_name = r.[Название контрагента]
						and not r.[Ошибки] like '%Название контрагента дублируется%'
					where c.shipment_data_type ='shipment_1C';

			end;


				

			-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ 1С
			begin 
						IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

						-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ SAP
						select 
								 sp.sap_id 
								,sp.need_before_date
								,sp.article_packaging
								,sp.expiration_date_in_days
								,sp.product_status
								,sp.stuffing_id
								,count(sp.sap_id) over (partition by sp.need_before_date, sp.article_packaging) as check_double_sap_id
						into #sap_id
						from ( 
			
									-- берем таблицу с ручными артикулами где указана дата действия артикула, подтягиваем по исключение другие артикула которые имеют данное исключение

									select 
											 c.sap_id
											,c.need_before_date
											,c.article_packaging
											,sp.expiration_date_in_days
											,sp.product_status
											,sp.stuffing_id
									from (
											SELECT distinct
												 max(iif(not need_before_date is null, sap_id_corrected, null)) over (partition by sap_id) as sap_id
												,max(iif(not need_before_date is null, need_before_date, null)) over (partition by sap_id) as need_before_date
												,article_packaging
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
											,c.article_packaging
											,sp.expiration_date_in_days
											,sp.product_status
											,sp.stuffing_id

									FROM info_view.sap_id as c
									join info_view.sap_id as sp on c.sap_id = sp.sap_id and sp.sap_id_type = 'Основной'
									where c.sap_id_type in ('Основной', 'Потребность')
									  and c.sap_id_corrected_need is null


							 ) as sp
						where not sp.article_packaging is null;


						-- обновляем данные до даты
						update c
						set c.shipment_sap_id							= s.SAP_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from data_import.shipment as c
						join #sap_id as s on c.article_packaging = s.article_packaging and not s.need_before_date is null and c.shipment_date <= s.need_before_date 
						where s.check_double_sap_id = 1 and c.shipment_data_type ='shipment_1C';


						-- обновляем остальные
						update c
						set c.shipment_sap_id							= s.SAP_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from data_import.shipment as c
						join #sap_id as s on c.article_packaging = s.article_packaging and  s.need_before_date is null
						where s.check_double_sap_id = 1 and c.shipment_data_type ='shipment_1C' and c.shipment_sap_id is null;


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
					where s.shipment_data_type ='shipment_1C';



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
											  where d.article_packaging = s.article_packaging) > 1 then	'Артикул тары неуникален | '
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
					where d.shipment_data_type ='shipment_1C';


			end;

			-- УДАЛЯЕМ РАНЕЕ СОЗДАННУЮ ТАБЛИЦУ
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

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
								,h.article_packaging
								,h.shipment_sales_channel_name
								,h.shipment_customer_name
								,h.shipment_delivery_address
								,h.shipment_priority
								,h.shipment_min_KOS
								,h.shipment_date
								,h.shipment_kg
								,ie.path_file
								,ie.data_on_date
						from data_import.shipment as h
						join data_import.data_type as ie on h.shipment_data_type = ie.data_type and h.shipment_data_type = 'shipment_1C'
						left join info_view.sap_id as sp on h.shipment_sap_id = sp.sap_id_for_join
						where h.shipment_stuffing_id_box_type in (0, 1);

			end;

end;


