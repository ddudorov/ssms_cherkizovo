﻿use project_plan_production_finished_products


go

alter procedure check_import.shipment_sales_plan @path_file			varchar(300) = null
												,@data_on_date			datetime = null			 										
as
BEGIN
			SET NOCOUNT ON;

			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ФАЙЛЕ И ВЫГРУЖАЕМ ТАБЛИЦУ ДЛЯ ЗАГРУЗКИ
			if not @path_file is null	 
			begin
						-- удаляем данные
						delete data_import.data_type where data_type = 'shipment_sales_plan';
						
						-- добавляем данные
						insert into data_import.data_type
							   (			data_type, source_data,  path_file,  data_on_date)
						values ('shipment_sales_plan',	   'Excel', @path_file, @data_on_date);
			
						-- удаляем и выгружаем
						delete from data_import.shipment where shipment_data_type = 'shipment_sales_plan';

						return(0);
			end;


	
			-- ВСТАВЛЯЕМ ДАННЫЕ
			begin

					-- ОСНОВНЫЕ СТОЛБЦЫ
					declare @sql_columns_main varchar(1000); set @sql_columns_main = '';

						select @sql_columns_main = @sql_columns_main + t.name  + ', '
						from tempdb.dbo.syscolumns as t
						where id = object_id('tempdb..#sales_plan')
							and not t.name like '%[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
						order by t.name;


							 
					-- СТОЛБЦЫ С ДАННЫМИ (ДЛЯ ПИВОТ)
					declare @sql_columns_for_pivot varchar(max); set @sql_columns_for_pivot = '';

						select @sql_columns_for_pivot = @sql_columns_for_pivot + t.name  + ','
						from tempdb.dbo.syscolumns as t
						where id = object_id('tempdb..#sales_plan')
							and t.name like '%[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
						order by t.name;
						set @sql_columns_for_pivot = left(@sql_columns_for_pivot, len(@sql_columns_for_pivot) - 1)


					-- ВСТАВЛЯЕМ ДАННЫЕ
					declare @sql_for_pivot varchar(max); set @sql_for_pivot = '';

						set @sql_for_pivot = 'insert into data_import.shipment
												( ' + @sql_columns_main + '  shipment_data_type,	shipment_with_branch_date,															shipment_kg) 
											  select	
												  ' + @sql_columns_main + '''shipment_sales_plan'', convert(datetime, RIGHT(shipment_with_branch_date,8)) as shipment_with_branch_date, shipment_kg																				
											from #sales_plan as upv
											UNPIVOT (shipment_kg for shipment_with_branch_date in (' + @sql_columns_for_pivot + ')) as upv
											where shipment_kg <> 0.0'
						exec( @sql_for_pivot);


			end;

			

			-- СЧИТАЕМ ДАТУ ОТГРУЗКИ C ФИЛИАЛА И УДАЛЯЕМ ЕСЛИ ОТГРУЗКА ПОПАДАЕТ НА 2 ДНЯ ЗАЯВОК
			begin

						update c
						set c.shipment_date = DATEADD(day, -b.to_branch_days, c.shipment_with_branch_date)
						from data_import.shipment as c
						join info.branches as b
							on c.shipment_branch_id = b.branch_id
						where shipment_data_type = 'shipment_sales_plan';
										
						-- удаляем отгрузки после даты отгрузки заявок 
						delete st
						from data_import.shipment as st
						join (

									select max(data_on_date) + 1 as dt_for_delete
									from data_import.data_type 
									where data_type in ('shipment_SAP', 'shipment_1C')

							 ) as d on st.shipment_date <= d.dt_for_delete
						where not st.shipment_date is null
						  and st.shipment_data_type = 'shipment_sales_plan';;


			end;



			-- ФОМРМАТИРОВАНИЕ ДАННЫХ 
			begin

						---- корректируем название контрагента
						update data_import.shipment
						set  shipment_promo_status			= trim(shipment_promo_status)
							,shipment_promo					= trim(shipment_promo)
							,shipment_promo_kos_listing		= trim(shipment_promo_kos_listing)
							,shipment_sales_channel_name	= case trim(shipment_sales_channel_name)  ---- канал сбыта
																when 'Дистрибьюторы'		then 'Дистрибьютор'
																else trim(shipment_sales_channel_name)
															  end
							,shipment_customer_name			= case trim(shipment_customer_name)
																when 'Окей'							then 'О''КЕЙ ООО'
																when 'О''кей'						then 'О''КЕЙ ООО'
											
																when 'METRO Group'					then 'МЕТРО КЭШ ЭНД КЕРРИ ООО'
																when 'Метро'						then 'МЕТРО КЭШ ЭНД КЕРРИ ООО'
											
																when 'Lenta'						then 'ЛЕНТА ООО'
																when 'Лента'						then 'ЛЕНТА ООО'
											
																when 'Билла'						then 'БИЛЛА ООО'											
																when 'Billa'						then 'БИЛЛА ООО'
													
																when 'МИР КОЛБАС ТД'				then 'ТД МИР КОЛБАС ООО'
																when 'Мир Колбас ТД ООО'			then 'ТД МИР КОЛБАС ООО'
																when 'Новый Импульс-50 СЗАО ООО'	then 'НОВЫЙ ИМПУЛЬС-50 ООО'
																when 'Auchan Group'					then 'АШАН ООО'



																when 'Зельгрос'						then 'Зельгрос ООО'
																when 'Auchan Group'					then 'АШАН ООО'
																when 'ИП Гаврилов (Джиком)'			then 'ДЖИКОМ ООО'
																when 'ОП_Регион-трейд ООО'			then 'РЕГИОН-ТРЕЙД ООО'

																when 'X5 Retail Group'				then 'ТОРГОВЫЙ ДОМ ПЕРЕКРЕСТОК АО'
																when 'Виктория'						then 'ВИКТОРИЯ БАЛТИЯ ООО'
																when 'ТОРГОВЫЙ ДОМ ПРОГРЕСС'		then 'ТД ПРОГРЕСС ООО'
																when 'ПЛАНЕТА КОЛБАС 58'			then 'ПЛАНЕТА КОЛБАС 58 ООО'
																when 'ТОРГОВЫЙ ДОМ СМИТ'			then 'ТОРГОВЫЙ ДОМ СМИТ ООО'
																when 'АСПЕКТ НТ'					then 'АСПЕКТ НТ ООО'
																when 'ООО "ТОРГ-ЦЕНТР"'				then 'ТОРГ-ЦЕНТР ООО'
																when 'ТК ЛЕТО ООО2'					then 'ТК ЛЕТО ООО'
																when 'ООО "Холлифуд"'				then 'ХОЛЛИФУД ООО'
																when 'ТК СТАРТ'						then 'ТК СТАРТ ООО'
																when 'ООО Паприка'					then 'ПАПРИКА ООО'
																when 'ООО "QURMAN PLYUS"'			then 'QURMAN PLYUS ООО'
																when 'ИП Дианова Е.В.'				then 'ДИАНОВА Е.В. ИП'
																when 'Говоров В.В. ИП'				then 'ГОВОРОВ ВЯЧЕСЛАВ ВАЛЕНТИНОВИЧ ИП'
																when 'ИП Гусев А.В.'				then 'ГУСЕВ А.В. ИП'
																when 'ИП Староверов'				then 'СТАРОВЕРОВ В.П. ИП'
																when 'Магнит'						then 'ТАНДЕР АО'
																when 'Гиперглобус'					then 'ГИПЕРГЛОБУС ООО'
																when 'ТОРГОВЫЙ ДОМ СМИТ'			then 'ТОРГОВЫЙ ДОМ СМИТ ООО'
																when 'ТД Интерторг'					then 'ТД Интерторг ООО'
																when 'АСПЕКТ НТ'					then 'АСПЕКТ НТ ООО'
																when 'ВИНТОВКИН А. Г. ИП'			then 'ВИНТОВКИН А.Г. ИП'
																when 'Зеон ОО'						then 'ЗЕОН ООО'
																when 'Меркушев Д. В. ИП'			then 'МЕРКУШЕВ Д.В. ИП'
																when 'ПРОДУКТОВАЯ МОЗАИКА'			then 'ПРОДУКТОВАЯ МОЗАИКА ООО'
																when 'ТК СТАРТ'						then 'ТК СТАРТ ООО'
																when 'ТОРГОВЫЙ ДОМ СМИТ'			then 'ТОРГОВЫЙ ДОМ СМИТ ООО'
																when 'КАРАВАНООО'					then 'КАРАВАН ООО'
																when 'ТД МИР КОЛБАС ООО'			then 'Мир Колбас ТД ООО'

																when 'МЫТИЩИНСКИЙ ФИЛИАЛ ТВОЙ ДОМ КРОКУС'		then 'МЫТИЩИНСКИЙ ФИЛИАЛ ТВОЙ ДОМ КРОКУС АО'
																when 'Красногорский филиал ТВОЙ ДОМ АО КР'		then 'Красногорский филиал ТВОЙ ДОМ АО КРОКУС'
																when 'КРОКУС АО (Красногорский филиал "Крокус'	then 'КРОКУС АО Красногорский филиал Крокус'
																when 'КРОКУС АО (Красногорский филиал "Кр'		then 'КРОКУС АО Красногорский филиал Крокус'
																when 'КОМПАНИЯ АЛЕКСАНДР И ПАРТНЕРЫ, ЛТЛ'		then 'КОМПАНИЯ АЛЕКСАНДР И ПАРТНЕРЫ, ЛТЛ ООО'

																else trim(shipment_customer_name)
															 end
						where shipment_data_type = 'shipment_sales_plan';

			end;

			-- ИНФОРМАЦИЯ ПО КЛИЕНТАМ
			begin

						-- кос и приоритет отгрузки
						update ts
						set ts.shipment_priority = c.manual_shipment_priority
						   ,ts.shipment_min_KOS	 = c.manual_KOS
						from data_import.shipment as ts
						join info.customers as c
							on ts.shipment_customer_id = c.customer_id
							and ts.shipment_sales_channel_name = c.sales_channel_name
						where shipment_data_type = 'shipment_sales_plan'
						  and not c.manual_shipment_priority is null 
						  and not c.manual_KOS is null;

			


						-- создаем таблицу по клиентам
						IF OBJECT_ID('tempdb..#customers_from_sap','U') is not null drop table #customers_from_sap;

						select 
								 d.shipment_customer_id	
								,min(d.shipment_customer_name) as shipment_customer_name
								,d.shipment_sales_channel_name	
								,'План продаж от ' + FORMAT(min(ie.data_on_date),'dd.MM.yyyy') as source_insert		
						into #customers_from_sap
						from data_import.shipment as d
						join data_import.data_type as ie on d.shipment_data_type = ie.data_type
						where not d.shipment_customer_id is null 
						  and not d.shipment_sales_channel_name is null			
						  and d.shipment_data_type = 'shipment_sales_plan'	
						group by 					
								 d.shipment_customer_id	
								,d.shipment_sales_channel_name;	



						-- обновляем справочник клиентов, что бы иметь возможность отследить клиента
						update c
						set c.source_insert = d.source_insert	
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
								,source_insert
						)
						select 
								 d.shipment_customer_id			
								,d.shipment_customer_name
								,d.shipment_sales_channel_name	
								,d.source_insert			
						from #customers_from_sap as d
						where not exists (select * 
											from info.customers as c
											where d.shipment_customer_id = c.customer_id
												and d.shipment_sales_channel_name = c.sales_channel_name); 

					
						
						IF OBJECT_ID('tempdb..#customers_from_sap','U') is not null drop table #customers_from_sap;

						
			end;





			-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ 
			begin 
						IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

						-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ SAP select * from #sap_id
						select 
								 sp.sap_id 
								,sp.need_before_date
								,sp.sap_id_corrected
								,sp.expiration_date_in_days
								,sp.product_status
								,sp.stuffing_id
						into #sap_id
						from ( 
			
									-- берем таблицу с ручными артикулами где указана дата действия артикула, подтягиваем по исключение другие артикула которые имеют данное исключение

									select 
											 c.sap_id
											,c.need_before_date
											,c.sap_id_corrected
											,sp.expiration_date_in_days
											,sp.product_status
											,sp.stuffing_id
									from (
											SELECT distinct
												 max(iif(not need_before_date is null, sap_id_corrected, null)) over (partition by sap_id) as sap_id
												,max(iif(not need_before_date is null, need_before_date, null)) over (partition by sap_id) as need_before_date
												,sap_id_corrected
											FROM info_view.sap_id
											where sap_id_type in ('Основной', 'Потребность')
											  and sap_id_corrected_need is null
										 ) as c
									join info_view.sap_id as sp on c.sap_id = sp.sap_id_for_join
									where not c.need_before_date is null

									union all

									SELECT distinct
											 c.sap_id
											,null as need_before_date
											,c.sap_id_corrected
											,sp.expiration_date_in_days
											,sp.product_status
											,sp.stuffing_id

									FROM info_view.sap_id as c
									join info_view.sap_id as sp on c.sap_id = sp.sap_id_for_join
									where c.sap_id_type in ('Основной', 'Потребность')
									  and c.sap_id_corrected_need is null


							 ) as sp;


						-- обновляем данные до даты
						update c
						set c.shipment_sap_id							= s.sap_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from data_import.shipment as c
						join #sap_id as s on c.sap_id_from_sales_plan = s.sap_id_corrected and not s.need_before_date is null and c.shipment_date <= s.need_before_date 
						where c.shipment_data_type ='shipment_sales_plan';


						-- обновляем остальные
						update c
						set c.shipment_sap_id							= s.sap_id
						   ,c.shipment_stuffing_id						= s.stuffing_id
						   ,c.shipment_sap_id_expiration_date_in_days	= s.expiration_date_in_days
						   ,c.shipment_product_status					= s.product_status
						from data_import.shipment as c
						join #sap_id as s on c.sap_id_from_sales_plan = s.sap_id_corrected and  s.need_before_date is null
						where c.shipment_data_type ='shipment_sales_plan' and c.shipment_sap_id is null;


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
							select stuffing_id, stuffing_box_1, stuffing_share_box_1 from .info.stuffing where not stuffing_box_1 is null union all
							select stuffing_id, stuffing_box_2, stuffing_share_box_2 from .info.stuffing where not stuffing_box_2 is null union all
							select stuffing_id, stuffing_box_3, stuffing_share_box_3 from .info.stuffing where not stuffing_box_3 is null
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
					from .data_import.shipment as s
					join #stuffing_share_box as t on s.shipment_stuffing_id = t.stuffing_id_box
					where s.shipment_data_type ='shipment_sales_plan';



					-- собираеми список полей
					declare @sql_columns_stuffing_box varchar(1000); set @sql_columns_stuffing_box = '';

						select @sql_columns_stuffing_box = @sql_columns_stuffing_box + t.name  + ','
						from tempdb.dbo.syscolumns as t
						where id = object_id('tempdb..#stuffing_box')
							and t.name <> 'shipment_row_id';

						set @sql_columns_stuffing_box = left(@sql_columns_stuffing_box,len(@sql_columns_stuffing_box) - 1)

					-- ВСТАВЛЯЕМ ДАННЫЕ
					declare @sql_insert_stuffing_box varchar(max);
						set @sql_insert_stuffing_box = 'insert into .data_import.shipment ( ' + @sql_columns_stuffing_box + ' )  
														select ' + @sql_columns_stuffing_box + ' from #stuffing_box'
						exec(@sql_insert_stuffing_box);

						update .data_import.shipment
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
										when d.shipment_sap_id is null								then	'Отсутствует sap id | '
										when d.shipment_stuffing_id is null							then	'Код набивки отсутствует | '
										when d.shipment_sap_id_expiration_date_in_days is null		then	'Отсутствует срок годности | '
										else ''
									end
								+ iif(d.shipment_sales_channel_name is null,			'Канал сбыта не присвоен | ', '')
								+ iif(d.shipment_priority is null,						'Отсутствует приоритет отгрузки | ', '')
								+ iif(d.shipment_min_KOS is null,						'Отсутствует КОС | ', '')

								, '')
					from .data_import.shipment as d
					where d.shipment_data_type ='shipment_sales_plan';


			end;
			
			-- добавляем данные в общию таблицу, которую выводим на форму
			exec report.for_form
			

			-- ВЫГРУЖАЕМ РЕЗУЛЬТАТ
			begin

						select 
								 'Ошибки'					= h.shipment_reason_ignore_in_calculate
								,'Статус артикула'			= h.shipment_product_status
								,'SAP ID для расчетов'		= convert(varchar(24), FORMAT(h.shipment_sap_id, '000000000000000000000000'))
								,'SAP ID из план продаж'	= convert(varchar(24), FORMAT(h.sap_id_from_sales_plan, '000000000000000000000000'))
								,'1С название'				= sp.product_1C_full_name
								,'Код набивки'				= h.shipment_stuffing_id
								,'Артикул номенклатуры'		= h.article_nomenclature
								,'Артикул тары'				= h.article_packaging
								,'Код филиала'				= h.shipment_branch_id
								,'Название филиала'			= h.shipment_branch_name
								,'Название канала сбыта'	= h.shipment_sales_channel_name
								,'Код контрагента'			= h.shipment_customer_id
								,'Название контрагента'		= h.shipment_customer_name
								,'Приоритет отгрузки'		= h.shipment_priority
								,'Мин КОС'					= h.shipment_min_KOS
								,'Дата отгрузки с филиала'	= h.shipment_with_branch_date
								,'Дата отгрузки из Москвы'	= h.shipment_date
								,'План, кг'					= h.shipment_kg
								,'Загружено из файла'		= ie.path_file
								,'Данные на дату'			= ie.data_on_date
								

						from data_import.shipment as h
						join data_import.data_type as ie on h.shipment_data_type = ie.data_type and h.shipment_data_type = 'shipment_sales_plan'
						left join info_view.sap_id as sp on h.shipment_sap_id = sp.sap_id and sp.sap_id_type = 'Основной'
						where h.shipment_stuffing_id_box_type in (0, 1);

			end;



					
end;
