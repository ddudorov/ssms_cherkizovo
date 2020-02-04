use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.transits	

go

alter procedure check_import.transit @path_file		varchar(300) = null
									,@data_on_date	datetime = null														
as
BEGIN

			SET NOCOUNT ON;

			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ФАЙЛЕ И ВЫГРУЖАЕМ ТАБЛИЦУ ДЛЯ ЗАГРУЗКИ
			if not @path_file is null	 
			begin
						-- удаляем данные
						delete project_plan_production_finished_products.data_import.data_type where data_type = 'transit';
						
						-- добавляем данные
						insert into project_plan_production_finished_products.data_import.data_type
							   (data_type, source_data,  path_file,  data_on_date)
						values ('transit',     'Excel', @path_file, @data_on_date);
			
						-- удаляем и выгружаем
						delete from project_plan_production_finished_products.data_import.stock where stock_data_type = 'transit';
						select top 0 * from project_plan_production_finished_products.data_import.stock;

						return(0);
			end;


			-- по наименование 1С подтягиваем SAP ID
			begin 
						IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

						-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ SAP
						select 
								 sm.sap_id 
								,sm.product_1C_full_name
								,sp.expiration_date_in_days
								,sp.product_status
								,st.stuffing_id
								,count(sm.sap_id) over (partition by sm.product_1C_full_name) as check_double_sap_id
						into #sap_id
						from ( 
									-- берем таблицу с ручными артикулами, подтягиваем варианты артикулов из другой системы и если у нормального артикула указано исключение отображаем ислючение
									select distinct
											 isnull(sm.sap_id_stock_manual, sm.sap_id) as sap_id
											,sp.product_1C_full_name
									from project_plan_production_finished_products.info.finished_products_sap_id_manual as sm
									join cherkizovo.info.products_sap as sp on sm.sap_id = sp.sap_id

							 ) as sm 
						join cherkizovo.info.products_sap as sp on sm.sap_id = sp.sap_id
						join project_plan_production_finished_products.info.finished_products_sap_id_manual as st on sm.sap_id = st.sap_id;


						-- обновляем данные до даты
						update c
						set c.stock_sap_id			= s.SAP_id
						   ,c.stock_stuffing_id		= s.stuffing_id
						   ,c.stock_expiration_date	= c.stock_production_date + s.expiration_date_in_days
						from project_plan_production_finished_products.data_import.stock as c
						join #sap_id as s on c.product_1C_full_name = s.product_1C_full_name 
						where s.check_double_sap_id = 1 and c.stock_data_type ='transit';



			end;


			---- ПИШЕМ ОШИБКИ ---------------------------------------------------------------
			update project_plan_production_finished_products.data_import.stock
			Set stock_reason_ignore_in_calculate = 
				nullif(								
							case 
								when (select top 1 c.check_double_sap_id 
									  from #sap_id as c 
									  where d.product_1C_full_name = c.product_1C_full_name) > 1 then	'Название SKU 1С > 1 SAP ID | '
								when d.stock_sap_id is null										 then	'Не найден sap id | '
								else ''
							end											
						+ case when d.stock_current_KOS is null									 then 'КОС некорректный | ' else '' end
						+ case when d.stock_current_KOS < 0.1									 then 'КОС меньше 10% | ' else '' end
						, '')
			from project_plan_production_finished_products.data_import.stock as d
			where stock_data_type = 'transit';



			-- выгружаем данные ---------------------------------------------------------------
			select 
					 s.stock_reason_ignore_in_calculate	
					,convert(varchar(24), FORMAT(s.stock_sap_id, '000000000000000000000000')) as sap_id
					,s.product_1C_full_name
					,s.stock_stuffing_id
					,s.stock_production_date
					,s.stock_on_date
					,s.stock_expiration_date
					,s.stock_current_KOS
					,s.stock_kg		
			from project_plan_production_finished_products.data_import.stock as s
			left join cherkizovo.info.products_sap as sp on s.stock_sap_id = sp.sap_id
			where s.stock_data_type = 'transit';


		
end;


