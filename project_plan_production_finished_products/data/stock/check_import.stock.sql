use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.stock @date_stock = '20190918'

go

alter procedure check_import.stock @date_stock datetime																		
as
BEGIN

			SET NOCOUNT ON;

			-- ИНФОРМАЦИЯ О ФАЙЛЕ: УДАЛЯЕМ И ВСТАВЛЯЕМ ДАННЫЕ О ДАННЫХ
			begin
						-- удаляем данные
						delete project_plan_production_finished_products.data_import.data_type where data_type = 'stock';
						
						-- добавляем данные
						insert into project_plan_production_finished_products.data_import.data_type
							   (data_type,  source_data,  data_on_date)
						values ('stock',	'mssql',	 @date_stock);
			
						-- удаляем и выгружаем
						delete from project_plan_production_finished_products.data_import.stock where stock_data_type = 'stock';

			end;

			-- вставляем данные
			insert into project_plan_production_finished_products.data_import.stock
			(		
					 stock_data_type	
					,product_finished_id
					,stock_warehouse_name			
					,stock_storage_area_name		
					,stock_branch_name				
					,stock_production_date		
					,stock_on_date					
					,stock_expiration_date				
					,stock_kg										 
			)
			select 			
					 stock_data_type			= 'stock'
					,product_finished_id		= st.[Код SAP MDG]
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

					

			-- ПОДТЯГИВАЕМ SAP ID К ДАННЫМ 
			begin 
					
						update s
						set s.stock_sap_id = p.sap_id
						   ,s.stock_stuffing_id = m.stuffing_id
						from project_plan_production_finished_products.data_import.stock as s
						join cherkizovo.info.products_sap as p on s.product_finished_id = p.product_finished_id
						left join project_plan_production_finished_products.info.finished_products_sap_id_manual as m on p.sap_id = m.sap_id
						where s.stock_data_type = 'stock';


			end;




			---- ПИШЕМ ОШИБКИ ---------------------------------------------------------------
			update project_plan_production_finished_products.data_import.stock
			Set stock_reason_ignore_in_calculate = 
				nullif(
						  case when stock_sap_id is null then 'Не найден sap id | ' else '' end
						+ case when stock_current_KOS is null then 'КОС некорректный | ' else '' end
						--+ case when stock_current_KOS < 0.1 then 'КОС меньше 10% | ' else '' end
						, '')
			where stock_data_type = 'stock' ;



			-- ВЫГРУЖАЕМ ДАННЫЕ ---------------------------------------------------------------
			select 
					 s.stock_reason_ignore_in_calculate	
					,convert(varchar(24), FORMAT(s.stock_sap_id, '000000000000000000000000')) as sap_id
					,sp.product_1C_full_name
					,s.stock_stuffing_id
					,s.product_finished_id	
					,s.stock_warehouse_name
					,s.stock_storage_area_name	
					,s.stock_branch_name
					,s.stock_production_date
					,s.stock_on_date
					,s.stock_expiration_date
					,s.stock_current_KOS
					,s.stock_kg		
			from project_plan_production_finished_products.data_import.stock as s
			left join cherkizovo.info.products_sap as sp on s.stock_sap_id = sp.sap_id
			where s.stock_data_type = 'stock';

		
end;


