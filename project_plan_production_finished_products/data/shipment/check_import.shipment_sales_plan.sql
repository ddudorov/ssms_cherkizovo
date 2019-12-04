use project_plan_production_finished_products


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
						delete project_plan_production_finished_products.data_import.data_type where data_type = 'shipment_sales_plan';
						
						-- добавляем данные
						insert into project_plan_production_finished_products.data_import.data_type
							   (			data_type, source_data,  path_file,  data_on_date)
						values ('shipment_sales_plan',	   'Excel', @path_file, @data_on_date);
			
						-- удаляем и выгружаем
						delete from project_plan_production_finished_products.data_import.shipment where shipment_data_type = 'shipment_sales_plan';

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

						set @sql_for_pivot = 'insert into project_plan_production_finished_products.data_import.shipment
												( ' + @sql_columns_main + '  shipment_data_type,	shipment_with_branch_date,															shipment_kg) 
											  select	
												  ' + @sql_columns_main + '''shipment_sales_plan'', convert(datetime, RIGHT(shipment_with_branch_date,8)) as shipment_with_branch_date, shipment_kg																				
											from #sales_plan as upv
											UNPIVOT (shipment_kg for shipment_with_branch_date in (' + @sql_columns_for_pivot + ')) as upv
											where shipment_kg <> 0.0'
						exec( @sql_for_pivot);


			end;

			
	
			-- ФОМРМАТИРОВАНИЕ ДАННЫХ 
			begin
			
					---- канал сбыта
						update project_plan_production_finished_products.data_import.shipment
						set shipment_sales_channel_name = 'Дистрибьютор'
						where shipment_sales_channel_name = 'Дистрибьюторы'
						  and shipment_data_type = 'shipment_sales_plan';



						---- корректируем название контрагента
						update project_plan_production_finished_products.data_import.shipment
						set  shipment_promo_status			= trim(shipment_promo_status)
							,shipment_promo					= trim(shipment_promo)
							,shipment_promo_kos_listing		= trim(shipment_promo_kos_listing)
							,shipment_sales_channel_name	= trim(shipment_sales_channel_name)
							,shipment_customer_name			= case trim(shipment_customer_name)
																when 'Окей'					then 'О''КЕЙ ООО'
																when 'О''кей'				then 'О''КЕЙ ООО'
											
																when 'METRO Group'			then 'МЕТРО КЭШ ЭНД КЕРРИ ООО'
																when 'Метро'				then 'МЕТРО КЭШ ЭНД КЕРРИ ООО'
											
																when 'Lenta'				then 'ЛЕНТА ООО'
																when 'Лента'				then 'ЛЕНТА ООО'
											
																when 'Билла'				then 'БИЛЛА ООО'											
																when 'Billa'				then 'БИЛЛА ООО'
													
																when 'МИР КОЛБАС ТД'		then 'ТД МИР КОЛБАС ООО'
																when 'Мир Колбас ТД ООО'	then 'ТД МИР КОЛБАС ООО'

																when 'Магнит'				then 'ТАНДЕР АО'
																when 'Гиперглобус'			then 'ГИПЕРГЛОБУС ООО'
																when 'ТОРГОВЫЙ ДОМ СМИТ'	then 'ТОРГОВЫЙ ДОМ СМИТ ООО'
																when 'ТД Интерторг'			then 'ТД Интерторг ООО'
																when 'АСПЕКТ НТ'			then 'АСПЕКТ НТ ООО'
																when 'ВИНТОВКИН А. Г. ИП'	then 'ВИНТОВКИН А.Г. ИП'
																when 'Зеон ОО'				then 'ЗЕОН ООО'
																when 'Меркушев Д. В. ИП'	then 'МЕРКУШЕВ Д.В. ИП'
																when 'ПРОДУКТОВАЯ МОЗАИКА'	then 'ПРОДУКТОВАЯ МОЗАИКА ООО'
																when 'ТК СТАРТ'				then 'ТК СТАРТ ООО'
																when 'ТОРГОВЫЙ ДОМ СМИТ'	then 'ТОРГОВЫЙ ДОМ СМИТ ООО'

																else trim(shipment_customer_name)
															 end
						where shipment_sales_channel_name = 'Дистрибьюторы'
						  and shipment_data_type = 'shipment_sales_plan';

			end;

			---- кос и приоритет отгрузки
			--update ts
			--set ts.shipment_priority = c.shipment_priority
			--   ,ts.shipment_min_KOS	 = c.manual_KOS
			--from project_plan_production_finished_products.data_import.shipments_sales_plan as ts
			--join project_plan_production_finished_products.info.customers as c
			--	on ts.shipment_customer_id = c.customer_id
			--	and ts.shipment_sales_channel_name = c.sales_channel_name
			--where not c.shipment_priority is null 
			--	and not c.manual_KOS is null;


			---- добавляем клиента и канал сбыта если нет в project_plan_production_finished_products.info.customers
			--insert into project_plan_production_finished_products.info.customers
			--(
			--		 customer_id
			--		,customer_name
			--		,sales_channel_name	
			--		,source_insert
			--)
			--select 
			--		 sp.shipment_customer_id			
			--		,min(sp.shipment_customer_name)
			--		,sp.shipment_sales_channel_name	
			--		,'План продаж от ' + FORMAT(min(ie.date_file),'dd.MM.yyyy') as source_insert			
			--from project_plan_production_finished_products.data_import.shipments_sales_plan as sp
			--join project_plan_production_finished_products.data_import.info_excel as ie on sp.name_table = ie.name_table
			--where not sp.shipment_customer_id is null
			--  and not sp.shipment_sales_channel_name is null
			--  and not exists (select * 
			--				  from project_plan_production_finished_products.info.customers as c
			--				  where sp.shipment_customer_id = c. customer_id
			--				    and sp.shipment_sales_channel_name = c.sales_channel_name)	
			--group by 
			--		 sp.shipment_customer_id
			--		,sp.shipment_sales_channel_name;	
				

			---- обновляем справочник
			--update c
			--set	 c.dt_tm_change = getdate()
			--	,c.source_insert = d.source_insert	
			--from project_plan_production_finished_products.info.customers as c
			--join (
			--		select
			--				 d.shipment_customer_id	
			--				,d.shipment_customer_name
			--				,d.shipment_sales_channel_name	
			--				,'План продаж от ' + FORMAT(min(ie.date_file),'dd.MM.yyyy') as source_insert
			--		from project_plan_production_finished_products.data_import.shipments_sales_plan as d
			--		join project_plan_production_finished_products.data_import.info_excel as ie on d.name_table = ie.name_table
			--		where not d.shipment_customer_id is null
			--			and not d.shipment_sales_channel_name is null	
			--		group by 
			--				 d.shipment_customer_id	
			--				,d.shipment_customer_name
			--				,d.shipment_sales_channel_name	
			--		) as d on c.customer_id = d.shipment_customer_id
			--			and c.sales_channel_name = d.shipment_sales_channel_name;



			---- считаем дату отгрузки c филиала
			--update c
			--set c.shipment_date = DATEADD(day, -b.to_branch_days, c.shipment_with_branch_date)
			--from project_plan_production_finished_products.data_import.shipments_sales_plan as c
			--join project_plan_production_finished_products.info.branches as b
			--	on c.shipment_branch_id = b.branch_id;


			
			---- удаляем отгрузки после даты отгрузки заявок 
			--select @dt_for_delete = max(date_file) + 1
			--from project_plan_production_finished_products.data_import.info_excel 
			--where name_table in ('shipments_SAP', 'shipments_1C');
	
			--delete project_plan_production_finished_products.data_import.shipments_sales_plan
			--where not shipment_date is null
			--  and shipment_date <= @dt_for_delete;



			
			---- подтягиваем SAP ID к данным план продаж, article_packaging должен быть 1
			--IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

			--select 
			--		 sm.sap_id 
			--		,sm.active_before
			--		,sm.article_packaging
			--		,sp.expiration_date_in_days
			--		,sp.product_status
			--		,st.stuffing_id
			--		,count(sm.sap_id) over (partition by sm.active_before, sm.article_packaging) as check_double_sap_id
			--into #sap_id
			--from ( 
			
			--			-- берем таблицу с ручными артикулами где указана дата действия артикула, подтягиваем по исключение другие артикула которые имеют данное исключение
			--			select distinct
			--					 sm.sap_id
			--					,sm.active_before
			--					,sp.article_packaging
			--			from project_plan_production_finished_products.info.finished_products_sap_id_manual as sm
			--			join project_plan_production_finished_products.info.finished_products_sap_id_manual as a on sm.sap_id_shipment_manual = ISNULL(a.sap_id_shipment_manual, a.sap_id)
			--			join cherkizovo.info.products_sap as sp on a.sap_id = sp.sap_id
			--			where not sm.active_before is null

			--			union 

			--			-- берем таблицу с ручными артикулами, подтягиваем варианты артикулов из другой системы и если у нормального артикула указано исключение отображаем ислючение
			--			select 
			--					 isnull(sm.sap_id_shipment_manual, sm.sap_id) as sap_id
			--					,null as active_before
			--					,sp.article_packaging
			--			from project_plan_production_finished_products.info.finished_products_sap_id_manual as sm
			--			join cherkizovo.info.products_sap as sp on sm.sap_id = sp.sap_id

			--	 ) as sm 
			--join cherkizovo.info.products_sap as sp on sm.sap_id = sp.sap_id
			--join project_plan_production_finished_products.info.finished_products_sap_id_manual as st on sm.sap_id = st.sap_id;


			---- обновляем данные до даты
			--update c
			--set c.sap_id							= s.SAP_id
			--	,c.stuffing_id						= s.stuffing_id
			--	,c.sap_id_expiration_date_in_days	= s.expiration_date_in_days
			--	,c.product_status					= s.product_status
			--from project_plan_production_finished_products.data_import.shipments_sales_plan as c
			--join #sap_id as s on c.article_packaging = s.article_packaging and not s.active_before is null and c.shipment_date <= s.active_before 
			--where s.check_double_sap_id = 1;

			---- обновляем остальные
			--update c
			--set c.sap_id							= s.SAP_id
			--	,c.stuffing_id						= s.stuffing_id
			--	,c.sap_id_expiration_date_in_days	= s.expiration_date_in_days
			--	,c.product_status					= s.product_status
			--from project_plan_production_finished_products.data_import.shipments_sales_plan as c
			--join #sap_id as s on c.article_packaging = s.article_packaging and s.active_before is null
			--where s.check_double_sap_id = 1
			--  and c.sap_id is null;




			--select *, count(s.sap_id) over (partition by s.article_packaging) as check_double_sap_id
			--into #sap_id
			--from (
			--		select distinct
			--				 s1.article_packaging
			--				,s2.sap_id 
			--				,s2.expiration_date_in_days
			--				,s2.product_status
			--				,sm2.stuffing_id
			--		from cherkizovo.info.products_sap													as s1
			--		join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm1 on s1.sap_id = sm1.sap_id
			--		join cherkizovo.info.products_sap													as s2  on isnull(sm1.sap_id_shipment_manual, sm1.SAP_id) = s2.sap_id 
			--		join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm2 on s2.sap_id = sm2.sap_id
			--	 ) as s;


			--update c
			--set c.sap_id							= s.SAP_id
			--	,c.stuffing_id						= s.stuffing_id
			--	,c.sap_id_expiration_date_in_days	= s.expiration_date_in_days
			--	,c.product_status					= s.product_status
			--from project_plan_production_finished_products.data_import.shipments_sales_plan as c
			--join #sap_id as s on c.article_packaging = s.article_packaging
			--where s.check_double_sap_id = 1;


			-- разбиваем коробочки на набивки
			--begin

			--		insert into project_plan_production_finished_products.data_import.shipments_sales_plan
			--		(
			--				 reason_ignore_in_calculate		
			--				,product_status
			--				,sap_id							
			--				,sap_id_expiration_date_in_days	
							
			--				,stuffing_id
			--				,stuffing_id_box_row_id
			--				,stuffing_id_box		

			--				,sap_id_from_excel
			--				,position_dependent_id
			--				,individual_marking_id
			--				,article_nomenclature
			--				,article_packaging
			--				,product_finished_id

			--				,shipment_promo_status
			--				,shipment_promo
			--				,shipment_promo_kos_listing

			--				,shipment_sales_channel_name
			--				,shipment_branch_id
			--				,shipment_branch_name
			--				,shipment_customer_id
			--				,shipment_customer_name
			--				,shipment_priority
			--				,shipment_min_KOS
			--				,shipment_with_branch_date
			--				,shipment_date
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

			--				,s.sap_id_from_excel
			--				,s.position_dependent_id
			--				,s.individual_marking_id
			--				,s.article_nomenclature
			--				,s.article_packaging
			--				,s.product_finished_id

			--				,s.shipment_promo_status
			--				,s.shipment_promo
			--				,s.shipment_promo_kos_listing

			--				,s.shipment_sales_channel_name
			--				,s.shipment_branch_id
			--				,s.shipment_branch_name
			--				,s.shipment_customer_id
			--				,s.shipment_customer_name
			--				,s.shipment_priority
			--				,s.shipment_min_KOS
			--				,s.shipment_with_branch_date
			--				,s.shipment_date
			--				,s.shipment_kg * (t.stuffing_share_in_box / sum(t.stuffing_share_in_box) over (partition by s.row_id)) as shipment_kg

			--		from project_plan_production_finished_products.data_import.shipments_sales_plan as s
			--		join project_plan_production_finished_products.info.stuffing as t on s.stuffing_id = t.stuffing_id_box;
					

					
			--		--проставляем row_id у группа набивок
			--		update s
			--		set s.stuffing_id_box_row_id = b.stuffing_id_box_row_id
			--		from project_plan_production_finished_products.data_import.shipments_sales_plan as s
			--		join (select distinct stuffing_id_box_row_id
			--			  from project_plan_production_finished_products.data_import.shipments_sales_plan 
			--			  where not stuffing_id_box is null) as b on s.row_id = b.stuffing_id_box_row_id;

					
			--		-- проставляем тип набивки
			--		update project_plan_production_finished_products.data_import.shipments_sales_plan
			--		set stuffing_id_box_type = case 
			--										when stuffing_id_box_row_id is null then 0 -- набивка не коробка
			--										when stuffing_id_box is null		then 1 -- набивка коробка
			--										when not stuffing_id_box is null	then 2 -- набивка разбитая на коробки
			--								   end;
			--end;



			------ пишем ошибки ---------------------------------------------------------------
			--update d
			--Set reason_ignore_in_calculate = 
			--	nullif(
			--				case 
			--					when (select top 1 c.check_double_sap_id from #sap_id as c where d.article_packaging = c.article_packaging)>1 
			--																	then	'Артикул тары возрощает > 1 SAP ID | '
			--					when d.sap_id is null							then	'Не найден sap id | '
			--					when d.stuffing_id is null						then	'Код набивки отсутствует | '
			--					when d.sap_id_expiration_date_in_days is null	then	'Отсутствует срок годности | '
			--					else ''
			--				end
			--			+ iif(shipment_min_KOS is null,						'Отсутствует КОС | ', '')
			--			+ iif(isnull(shipment_sales_channel_name, '') = '',	'Канал сбыта не присвоен | ', '')
			--			+ iif(shipment_with_branch_date is null,			'Дата отгрузки отсутствует  | ', '')
			--			, '')
			--from project_plan_production_finished_products.data_import.shipments_sales_plan as d;


			-- ВЫГРУЖАЕМ РЕЗУЛЬТАТ
			begin

						select 
								 h.shipment_reason_ignore_in_calculate
								,h.shipment_product_status
								,convert(varchar(24), FORMAT(h.sap_id, '000000000000000000000000'))
								,sp.product_1C_full_name
								,h.shipment_stuffing_id
								,h.article_packaging
								,h.article_nomenclature
								,h.shipment_branch_id
								,h.shipment_branch_name
								,h.shipment_sales_channel_name
								,h.shipment_customer_id
								,h.shipment_customer_name
								,h.shipment_priority
								,h.shipment_min_KOS
								,h.shipment_with_branch_date
								,h.shipment_date
								,h.shipment_kg
								,ie.path_file
								,ie.data_on_date
						from project_plan_production_finished_products.data_import.shipment as h
						join project_plan_production_finished_products.data_import.data_type as ie on h.shipment_data_type = ie.data_type and h.shipment_data_type = 'shipment_sales_plan'
						left join cherkizovo.info.products_sap as sp on h.sap_id = sp.sap_id
						where h.shipment_stuffing_id_box_type in (0, 1);

			end;



					
end;
