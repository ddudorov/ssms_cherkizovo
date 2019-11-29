use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.stock @date_stock = '20190918'

go

alter procedure check_import.stock @date_stock datetime																		
as
BEGIN

			SET NOCOUNT ON;

			exec project_plan_production_finished_products.check_import.info_excel @path_file = 'MSSQL'
                                                                                  ,@date_file = @date_stock
                                                                                  ,@name_table = 'stock'
																				  ,@select = 0;

			-- вставляем данные
			insert into project_plan_production_finished_products.data_import.stock
			(		
					 product_finished_id
					,stock_warehouse_name			
					,stock_storage_area_name		
					,stock_branch_name				
					,stock_production_date		
					,stock_on_date					
					,stock_expiration_date				
					,stock_kg										 
			)
			select 			
					 product_finished_id		= st.[Код SAP MDG]
					,stock_warehouse_name		= st.[Склад 2] 
					,stock_storage_area_name	= st.[Склад] 
					,stock_branch_name			= st.[Филиал]
					,stock_production_date		= st.[Дата выработки]
					,stock_on_date				= st.[Дата]
					,stock_expiration_date		= st.[годен до]
					,stock_kg					= sum(st.[Объем, кг] )
			FROM [Stocks_Test].[dbo].[Остатки] as st
			where st.Дата = @date_stock
				and not st.[Склад]  like '%брак%'
				and st.[Категория] in ('Колбасы сырокопченые')
				and st.[Филиал] in ('Биком', 'ЧМПЗ', 'ТД Черкизово - БИКОМ', 'Кашира')
			group by st.[Код SAP MDG]
					,st.[Склад 2] 
					,st.[Склад] 
					,st.[Филиал]
					,st.[Дата выработки]
					,st.[Дата]
					,st.[годен до];

					


			-- подтягиваем данные ---------------------------------------------------------------
			-- подтягиваем SAP ID к данным SAP
			IF OBJECT_ID('tempdb..#sap_id','U') is not null drop table #sap_id;

			select *, count(s.sap_id) over (partition by s.product_finished_id) as check_double_sap_id
			into #sap_id
			from (
					
					select distinct
							 s1.product_finished_id
							,s2.sap_id 
							,s2.expiration_date_in_days
							,s2.product_status
							,sm2.stuffing_id
					from cherkizovo.info.products_sap													as s1
					join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm1 on s1.sap_id = sm1.sap_id
					join cherkizovo.info.products_sap													as s2  on isnull(sm1.SAP_id_stock_manual, sm1.SAP_id) = s2.sap_id 
					join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm2 on s2.sap_id = sm2.sap_id
				 ) as s;

			update c
			set c.sap_id							= s.SAP_id
				,c.stuffing_id						= s.stuffing_id
				,c.product_status					= s.product_status
			from project_plan_production_finished_products.data_import.stock as c
			join #sap_id as s on c.product_finished_id = s.product_finished_id and s.check_double_sap_id = 1;




			-- пишем ошибки ---------------------------------------------------------------
			update project_plan_production_finished_products.data_import.stock
			Set reason_ignore_in_calculate = 
				nullif(
						  case when sap_id is null then 'Не найден sap id | ' else '' end
						+ case when stock_current_KOS is null then 'КОС некорректный | ' else '' end
						+ case when stock_current_KOS < 0.1 then 'КОС меньше 10% | ' else '' end
						, '');

			-- выгружаем данные ---------------------------------------------------------------
			select 
					 s.reason_ignore_in_calculate	
					,s.product_status
					,s.sap_id_text
					,s.stuffing_id
					,s.product_finished_id	
					,s.stock_warehouse_name
					,s.stock_storage_area_name	
					,s.stock_branch_name
					,s.stock_production_date
					,s.stock_on_date
					,s.stock_expiration_date
					,s.stock_kg	
					,s.stock_current_KOS
					,ie.path_file
					,ie.date_file
					,ie.user_insert
					,ie.dt_tm_insert	
			from project_plan_production_finished_products.data_import.stock as s
			join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table;
		
end;


