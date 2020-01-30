/********************************/
-- Регламент готовой продукции 
-- Автор: Рощин МЮ 
-- Версия: 20190708	
/********************************/


use cherkizovo;

go


--------------------------------------------------
-- cherkizovo.info.products_sap - ОСНОВНАЯ ТАБЛИЦА
--------------------------------------------------

begin

		select * from cherkizovo.info.products_sap;
		--drop table cherkizovo.info.products_sap;



		CREATE TABLE cherkizovo.info.products_sap
		(
			 id									smallint		NOT NULL	IDENTITY(1, 1)
			,sap_id								BIGINT			NOT NULL	-- SAP ID		
			,sap_id_text						as '''' + convert(varchar(24), FORMAT(sap_id, '000000000000000000000000'))
			,category_1_level_id 				INT					NULL	-- Код 1 уровня
			,category_1_level_name 				VARCHAR(50)			NULL	-- Название 1 уровня	
			,category_2_level_id 				INT					NULL	-- Код 2 уровня
			,category_2_level_name 				VARCHAR(50)			NULL	-- Название 2 уровня		
			,category_3_level_id 				INT					NULL	-- Код 3 уровня
			,category_3_level_name 				VARCHAR(50)			NULL	-- Название 3 уровня		
			,category_4_level_id 				INT					NULL	-- Код 4 уровня
			,category_4_level_name 				VARCHAR(50)			NULL	-- Название 4 уровня		
			,category_5_level_id 				INT					NULL	-- Код 5 уровня
			,category_5_level_name 				VARCHAR(50)			NULL	-- Название 5 уровня	
			,category_full_name 				VARCHAR(150)		NULL    -- Категория
		
			,production_id						SMALLINT			NULL	-- Код завода

			,production_sap_id					AS case production_id	
														when 3999 then 99 
														when 3001 then 30
														when 3002 then 31
														when 3003 then 32
														when 3005 then 33 
														when 3004 then 34 
														when 3013 then 35 
													end 
			,production_attribute				AS case 
														when production_id in (3999, 3005, 3004, 3003, 3002, 3001) then 'П' + right(production_id, 1) 
														when production_id = 3013 then 'П7'  
													end
			,production_name					VARCHAR(40)			NULL	-- Площадка
			,production_shop_1C_name			VARCHAR(50)			NULL	-- Цех производства 1С
	
			,UPP_1C_id							VARCHAR(11)			NULL	-- 1С УПП
			,UPP_TM_1C_id						VARCHAR(9)			NULL	-- Код 1С УПП ТМ
			,CSB_id								DEC(6)				NULL	-- Код csb
			,unit_id							DEC(5)				NULL	-- Юнит
			,position_basic_id					INT					NULL	-- Код базовой позиции
			,position_dependent_id				INT					NULL	-- Код зависимой позиции
			,product_finished_id				DEC(14)				NULL	-- Код PIM Z011
			,product_not_packaged_id			DEC(14)				NULL	-- Код PIM Z013

			,individual_marking_id				TINYINT				NULL	-- Код индивидуальной маркировки
			,individual_marking_name			VARCHAR(50)			NULL	-- Индивидуальная маркировка

			,brand_trademark_short_name			VARCHAR(20)			NULL	-- Бренд (Торговая марка)
			,brand_trademark_full_name			VARCHAR(50)			NULL	-- Бренд (Торговая марка)1

			,brand_destination_short_name		VARCHAR(3)			NULL	-- Бренд (назначение)
			,brand_destination_full_name		VARCHAR(50)			NULL	-- Бренд (назначение)1

			,article_nomenclature				VARCHAR(20)			NULL	-- Артикул номенклатуры
			,article_packaging					VARCHAR(25)			NULL	-- Артикул тары
			
			,product_SAP_full_name				VARCHAR(500)		NULL	-- Полное наименование зависимой продукта
			,product_clean_full_name 			VARCHAR(500)		NULL	
			,product_1C_full_name 				VARCHAR(500)		NULL

			,product_GOST_name					VARCHAR(100)		NULL	-- ГОСТ/ТУ продукции
			,GTIN_CU_id							DEC(13)				NULL	-- GTIN (ШК) штуки (CU)	
			,GTIN_SKU_id						DEC(14)				NULL	-- GTIN (ШК) штуки (SKU)
			,FEACN_id							DEC(10)				NULL	-- Код ТНВЭД
			,FEACN_name							VARCHAR(45)			NULL	-- Название ТНВЭД
			,vad_vol							VARCHAR(3)			NULL	-- VAD/VOL

			,product_status						VARCHAR(100)		NULL	-- Статус материала

			,product_storage_type				VARCHAR(2)			NULL	-- Единица хранения остатков
	
			,unit_size_name						VARCHAR(11)			NULL	-- Размер единицы продукции ДхШхВ (мм)
			,unit_length_mm						DEC(6,3)			NULL		
			,unit_width_mm						DEC(6,3)			NULL	
			,unit_height_mm						DEC(6,3)			NULL
			,unit_volume_m						as nullif(unit_length_mm * unit_width_mm * unit_height_mm, 0) / 1000000000
		
			,unit_net_weight_kg					DEC(7,3)			NULL	-- Вес единицы продукции нетто (кг)
			,packaging_unit_net_weight_kg		DEC(7,3)			NULL	-- Вес упаковки с единицы продукции (кг)
			,product_net_weight_in_box_kg		DEC(7,3)			NULL	-- Вес продукции в коробе нетто (кг)
			,packaging_net_weight_in_box_kg		DEC(7,3)			NULL	-- Вес дополнительной упаковки в коробе (кг)
			,product_net_weight_on_pallet_kg	DEC(9,3)			NULL	-- Вес нетто продукции на поддоне (кг)
			,quantity_in_box					SMALLINT			NULL	-- Количество вложений в короб (шт)
			,quantity_box_on_pallet				SMALLINT			NULL	-- Количество коробов на поддоне (шт)
			,box_name							VARCHAR(100)		NULL	-- Наименование и вес тары без продукции


	
			,product_storage_description		VARCHAR(500)		NULL	-- Срок хранения и температурные режимы
			,freezing_type_id					TINYINT				NULL	-- Термосостояние
			,freezing_type_name					VARCHAR(15)			NULL	-- Термосостояние1

			,expiration_date_type				VARCHAR(5)			NULL	-- Срок хранения
			,expiration_date_in_days			SMALLINT			NULL	-- Общий срок годности1
			,KOS_in_day							AS case 
														when expiration_date_type = 'сутки' then 1.0 / nullif(expiration_date_in_days, 0)
													end
			,expiration_date_in_days_from		SMALLINT			NULL
			,expiration_date_in_days_to			SMALLINT			NULL
			,category_residual_expiration_date	SMALLINT			NULL
		
			,vat								DEC(3,2)			NULL	-- Ставка налога

			,update_dt_tm						datetime			DEFAULT		GETDATE()
			,update_user						VARCHAR(50)			DEFAULT		ORIGINAL_LOGIN()

			 CONSTRAINT [PK products_sap | sap_id] PRIMARY KEY CLUSTERED (sap_id) 
		);


end;





use cherkizovo

go

--************************************
-- Обновление данных info.products_sap 
-- Рощин МЮ | 201910					
--************************************
--exec cherkizovo.info.products_sap_update @file_attachments varchar(500)
alter procedure info.products_sap_update @file_attachments varchar(500)
as
begin

			SET NOCOUNT ON; 

			update cherkizovo.info.products_sap
			set  unit_length_mm = case 
										when unit_size_name like '[0-9][0-9]Х%Х%'		then LEFT(unit_size_name, 2)
										when unit_size_name like '[0-9][0-9][0-9]Х%Х%'	then LEFT(unit_size_name, 3) 
									end
				,unit_width_mm  = case 
										when unit_size_name like '%Х[0-9][0-9]Х%'		then SUBSTRING(unit_size_name, CHARINDEX('Х',unit_size_name) + 1, 2)
										when unit_size_name like '%Х[0-9][0-9][0-9]Х%'	then SUBSTRING(unit_size_name, CHARINDEX('Х',unit_size_name) + 1, 3) 
									end
				,unit_height_mm = case 
										when unit_size_name like '%Х%Х[0-9][0-9]'		then RIGHT(unit_size_name,2)
										when unit_size_name like '%Х%Х[0-9][0-9][0-9]'	then RIGHT(unit_size_name,3) 
									end;



			update cherkizovo.info.products_sap
			set  product_storage_description = Trim(Replace(product_storage_description, Char(10), ''))
				,product_SAP_full_name		 = Trim(product_SAP_full_name);
								

			-- название артикула чистим без веса 
			update cherkizovo.info.products_sap
			set product_clean_full_name = CASE 
												WHEN product_SAP_full_name like '%[КK][0-9]' THEN left(product_SAP_full_name, len(product_SAP_full_name) - 2)
												WHEN product_SAP_full_name like '%[КK][0-9][0-9]' THEN  left(product_SAP_full_name, len(product_SAP_full_name) - 3)
												WHEN product_SAP_full_name like '%[КK][0-9][.0-9][0-9]' THEN  left(product_SAP_full_name, len(product_SAP_full_name) - 4)
												WHEN product_SAP_full_name like '%[КK][0-9][.0-9][.0-9][0-9]' THEN left(product_SAP_full_name, len(product_SAP_full_name) - 5)
												WHEN product_SAP_full_name like '%[КK][0-9][.0-9][.0-9][.0-9][0-9]' THEN  left(product_SAP_full_name, len(product_SAP_full_name) - 6)
												ELSE product_SAP_full_name
											END;


			update cherkizovo.info.products_sap
			set  product_clean_full_name = Trim(product_clean_full_name);



			-- название артикула + признак завода + макркировка \ если маркировка черкизова то пусто									
			update cherkizovo.info.products_sap
			set product_1C_full_name = product_clean_full_name + ' ' + production_attribute + CASE WHEN individual_marking_id in (1) or individual_marking_name in ('ЧЕРКИЗОВО') THEN '' ELSE ' ' + individual_marking_name END;



			---------------------------------------------------
			-- ОТПРАВКА ПИСЬМА
			---------------------------------------------------
			declare @recipients varchar(max);
				set @recipients = ''
				set @recipients = @recipients + 'd.konovalov@cherkizovo.com;'
				set @recipients = @recipients + 'd.panchenko@cherkizovo.com;'
				set @recipients = @recipients + 'n.meshcheriakova@cherkizovo.com;'
				set @recipients = @recipients + 'o.s.nechaeva@cherkizovo.com;'
				set @recipients = @recipients + 's.vdovina@cherkizovo.com;'
				set @recipients = @recipients + 'm.e.romanova@cherkizovo.com;'
				set @recipients = @recipients + 'a.sukhanov@cherkizovo.com;'
				set @recipients = @recipients + 'a.forshev@cherkizovo.com;'
				set @recipients = @recipients + 'k.luchnikov@cherkizovo.com;'
				set @recipients = @recipients + 'd.vanovskaia@cherkizovo.com;'
				set @recipients = @recipients + 'v.kosareva@cherkizovo.com;'
				set @recipients = @recipients + 'a.dutov@cherkizovo.com;'
				set @recipients = @recipients + 'e.mazlova@cherkizovo.com;'
				set @recipients = @recipients + 'k.litvinenko@cherkizovo.com;'
				set @recipients = @recipients + 'o.tcoy@cherkizovo.com;'
				set @recipients = @recipients + 'a.faizova@cherkizovo.com;'
				set @recipients = @recipients + 'm.ulitin@cherkizovo.com;'
				set @recipients = @recipients + 'supplydept@cherkizovo.com;' 
				set @recipients = @recipients + 'a.kurilina@cherkizovo.com;'
				set @recipients = @recipients + 's.lopatinskiy@cherkizovo.com;'
				set @recipients = @recipients + 'a.bastrygin@cherkizovo.com;'
				set @recipients = @recipients + 'o.medvedeva@cherkizovo.com;'
				set @recipients = @recipients + 'ya.turik@cherkizovo.com;'
				set @recipients = @recipients + 's.v.smirnova@cherkizovo.com;' 
				set @recipients = @recipients + 'o.zakharova@cherkizovo.com;' 
				set @recipients = @recipients + 'n.solbakova@cherkizovo.com;'
				set @recipients = @recipients + 'm.timurgalieva@cherkizovo.com;'
				set @recipients = @recipients + 'm.votiakova@cherkizovo.com;'
				
				
				

				
			exec monitoring.dbo.send_mail @recipients = @recipients
										 ,@copy_recipients = 'm.roshchin@cherkizovo.com;d.dudorov@cherkizovo.com;'
										 ,@subject = 'Регламент готовой продукции' 
										 ,@file_attachments	= @file_attachments

										 


end;







/******************************************************************************************/
/* Обновление данных из cherkizovo.for_import.products_sap в cherkizovo.info.products_sap */
/* Рощин МЮ | 201907																	  */
/******************************************************************************************/
--exec cherkizovo.info.update_info_products_sap

alter procedure info.update_info_products_sap
as
begin
		SET NOCOUNT ON; 

		begin TRANSACTION
		SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
		BEGIN TRY 




				-- проставляем категрию ОСГ
				update cherkizovo.info.products_sap
				set category_residual_expiration_date =	case when expiration_date_type <> 'сутки'	  then null
															 when expiration_date_in_days between 1   and 30  then 6
															 when expiration_date_in_days between 31  and 35  then 7
															 when expiration_date_in_days between 36  and 45  then 8
															 when expiration_date_in_days between 46  and 60  then 9
															 when expiration_date_in_days between 61  and 90  then 10
															 when expiration_date_in_days between 91  and 365 then 11
															 when expiration_date_in_days between 366 and 999 then 12
														end;

			
				-- проставляем на основание категрию ОСГ дни от и до
				update cherkizovo.info.products_sap
				set  expiration_date_in_days_from =	case when expiration_date_type <> 'сутки'		 then null  
														 when category_residual_expiration_date = 6  then 1   
														 when category_residual_expiration_date = 7  then 31  
														 when category_residual_expiration_date = 8  then 36  
														 when category_residual_expiration_date = 9  then 46  
														 when category_residual_expiration_date = 10 then 61  
														 when category_residual_expiration_date = 11 then 91  
														 when category_residual_expiration_date = 12 then 366 
														end
					,expiration_date_in_days_to =	case when expiration_date_type <> 'сутки'		 then null
														 when category_residual_expiration_date = 6  then 30 
														 when category_residual_expiration_date = 7  then 35  
														 when category_residual_expiration_date = 8  then 45  
														 when category_residual_expiration_date = 9  then 60  
														 when category_residual_expiration_date = 10 then 90  
														 when category_residual_expiration_date = 11 then 365 
														 when category_residual_expiration_date = 12 then 999 
														end;







			
				COMMIT 

				select 0 as err_YN, '' as err_message  


		END TRY  
		BEGIN CATCH 
				ROLLBACK
				--select 1 as err 
				select 1 as err_YN, ERROR_MESSAGE() as err_message  
				--print 
				--	'функция ERROR_NUMBER() возвращает номер ошибки; --- ' + convert(varchar(500), ERROR_NUMBER()) + char(10) +
				--	'функция ERROR_SEVERITY() возвращает степень серьезности ошибки; --- ' + convert(varchar(500),ERROR_SEVERITY())  + char(10) +
				--	'функция ERROR_STATE() возвращает код состояния ошибки; --- ' +  convert(varchar(500),ERROR_STATE()) + char(10) +
				--	'функция ERROR_PROCEDURE() возвращает имя хранимой процедуры или триггера, в котором произошла ошибка; --- ' + convert(varchar(500),isnull(ERROR_PROCEDURE(),''))  + char(10) +
				--	'функция ERROR_LINE() возвращает номер строки, которая вызвала ошибку, внутри подпрограммы; --- ' + convert(varchar(500),ERROR_LINE())  + char(10) +
				--	'функция ERROR_MESSAGE() возвращает полный текст сообщения об ошибке. Текст содержит значения подставляемых параметров, таких как длина, имена объектов или время. --- ' + convert(varchar(1500),ERROR_MESSAGE())
		END CATCH; 


end


















































