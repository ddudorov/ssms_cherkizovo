use project_plan_production_finished_products

-- таблица
drop table project_plan_production_finished_products.info.finished_products_sap_id_manual
create table project_plan_production_finished_products.info.finished_products_sap_id_manual
(
		 sap_id							BIGINT			NOT NULL
		,sap_id_correct_manual			BIGINT				NULL	
		,sap_id_analog_1				BIGINT				NULL
		,sap_id_analog_2				BIGINT				NULL
		,stuffing_id					VARCHAR(40)		NOT NULL	DEFAULT		'укажите код набивки'
		,product_status_manual			VARCHAR(100)		NULL	
		,number_days_normative_stock	smallint			null
		,CONSTRAINT [PK finished_products_sap_id_manual | sap_id] PRIMARY KEY CLUSTERED (SAP_id) 
		,CONSTRAINT CHK_SAP_id CHECK (
											sap_id <> sap_id_correct_manual
										and sap_id <> sap_id_analog_1
										and sap_id <> sap_id_analog_2
										and sap_id_correct_manual <> sap_id_analog_1
										and sap_id_correct_manual <> sap_id_analog_2
										and sap_id_analog_1 <> sap_id_analog_2
										and (   (	 sap_id_analog_1 is null and	 sap_id_analog_2 is null) or
												(not sap_id_analog_1 is null and	 sap_id_analog_2 is null) or 
												(not sap_id_analog_1 is null and not sap_id_analog_2 is null)   )
								     )
)


go
















-- exec project_plan_production_finished_products.report.finished_products_sap_id_manual		
alter procedure report.finished_products_sap_id_manual							
as
BEGIN
			SET NOCOUNT ON;




			-- ПРОВЕРКА НАБИВОК
			IF OBJECT_ID('tempdb..#check_stuffing','U') is not null drop table #check_stuffing;

			select ps.sap_id, 'Отсутствует набивка' as error
			into #check_stuffing
			from cherkizovo.info.products_sap as ps
			left join project_plan_production_finished_products.info.finished_products_sap_id_manual as cm on ps.sap_id = cm.sap_id
			where ps.category_3_level_name in ('Колбасы сырокопченые')
			  and cm.stuffing_id is null;
			



			-- ПРОВЕРКА КОГДА У ИСКЛЮЧЕНИЯ УКАЗАНО ИСКЛЮЧЕНИЕ
			IF OBJECT_ID('tempdb..#check_sap_id_correct_manual','U') is not null drop table #check_sap_id_correct_manual;

			select cm2.SAP_id, 'Исключение имеет исключение' as error
			into #check_sap_id_correct_manual
			from project_plan_production_finished_products.info.finished_products_sap_id_manual as cm
			join project_plan_production_finished_products.info.finished_products_sap_id_manual as cm2 on cm.SAP_id_correct_manual = cm2.SAP_id
			where not cm2.SAP_id_correct_manual is null;
			




			-- НАИМЕНОВАНИЕ ВОЗРОЩАЕТ РАЗНЫЕ SAP ID
			IF OBJECT_ID('tempdb..#check_double_name_1c','U') is not null drop table #check_double_name_1c;

			select cm.sap_id, cm.error
			into #check_double_name_1c
			from (
					select cm.sap_id, iif(COUNT(1) over (partition by cm.product_1C_full_name) > 1, 'Дубликат | ' + cm.product_1C_full_name, null) as error
					from (

							select distinct isnull(cm.SAP_id_correct_manual, ps.sap_id) as sap_id, ps.product_1C_full_name
							from cherkizovo.info.products_sap as ps
							left join project_plan_production_finished_products.info.finished_products_sap_id_manual as cm on ps.sap_id = cm.sap_id
							where ps.category_3_level_name in ('Колбасы сырокопченые')
						 ) as cm
				 ) as cm
			where not cm.error is null;

			
			-- АНАЛОГ ИМЕЕТ ИСКЛЮЧЕНИЕ И УКАЗАН НЕСКОЛЬКО РАЗ
			IF OBJECT_ID('tempdb..#check_sap_id_analog','U') is not null drop table #check_sap_id_analog;

			select cm.sap_id, cm.error
			into #check_sap_id_analog
			from (
				
					select 
							 cm1.sap_id
							,cm1.sap_id_analog_1
							,cm1.sap_id_analog_2
							,case 
									when isnull(a2.count_analog, 1) > 1			then 'Аналог указан больше 1 раза: '	+  convert(varchar(24),FORMAT(cm1.sap_id_analog_2, '000000000000000000000000') ) 
									when isnull(a1.count_analog, 1) > 1			then 'Аналог указан больше 1 раза: '	+  convert(varchar(24),FORMAT(cm1.sap_id_analog_1, '000000000000000000000000') ) 
									when not cm2.sap_id_correct_manual is null	then 'Аналог имеет исключение: '		+  convert(varchar(24),FORMAT(cm1.sap_id_analog_1, '000000000000000000000000') ) 
									when not cm3.sap_id_correct_manual is null	then 'Аналог имеет исключение: '		+  convert(varchar(24),FORMAT(cm1.sap_id_analog_2, '000000000000000000000000') ) 	
							 end as error
							,a1.count_analog as count_analog_1
							,a2.count_analog as count_analog_2	
					from project_plan_production_finished_products.info.finished_products_sap_id_manual as cm1 
					left 
					join project_plan_production_finished_products.info.finished_products_sap_id_manual as cm2 on cm1.sap_id_analog_1 = cm2.sap_id
					left 
					join project_plan_production_finished_products.info.finished_products_sap_id_manual as cm3 on cm1.sap_id_analog_2 = cm3.sap_id
					left
					join (	select sap_id_analog, COUNT(1) as count_analog
							from (  select sap_id_analog_1 as sap_id_analog from project_plan_production_finished_products.info.finished_products_sap_id_manual where not sap_id_analog_1 is null
									union all
									select sap_id_analog_2 as sap_id_analog from project_plan_production_finished_products.info.finished_products_sap_id_manual where not sap_id_analog_2 is null  ) as cm
							group by sap_id_analog  ) as a1 on cm1.sap_id_analog_1 = a1.sap_id_analog
					left
					join (	select sap_id_analog, COUNT(1) as count_analog
							from (  select sap_id_analog_1 as sap_id_analog from project_plan_production_finished_products.info.finished_products_sap_id_manual where not sap_id_analog_1 is null
									union all
									select sap_id_analog_2 as sap_id_analog from project_plan_production_finished_products.info.finished_products_sap_id_manual where not sap_id_analog_2 is null  ) as cm
							group by sap_id_analog  ) as a2 on cm1.sap_id_analog_2 = a2.sap_id_analog
						 
					where not cm1.sap_id_analog_1 is null or not cm1.sap_id_analog_2 is null
				 ) as cm
			where not cm.error is null;





			select 
						 'SAP ID' = convert(varchar(24),FORMAT(s.SAP_id, '000000000000000000000000') )
						,'SAP ID исключение' = convert(varchar(24),FORMAT(sm.SAP_id_correct_manual, '000000000000000000000000') )
						,'SAP ID аналог 1' = convert(varchar(24),FORMAT(sm.sap_id_analog_1, '000000000000000000000000') )
						,'SAP ID аналог 2' = convert(varchar(24),FORMAT(sm.sap_id_analog_2, '000000000000000000000000') )
						,'Проверка справочника' = nullif(
														 isnull((select top 1 cm.error from #check_stuffing as cm				where s.sap_id = cm.SAP_id) + ' | ','')
														+isnull((select top 1 cm.error from #check_sap_id_correct_manual as cm	where s.sap_id = cm.SAP_id) + ' | ','')
														+isnull((select top 1 cm.error from #check_sap_id_analog as cm			where s.sap_id = cm.SAP_id) + ' | ','')
														+isnull((select top 1 cm.error from #check_double_name_1c as cm			where s.sap_id = cm.SAP_id) + ' | ','')
														,'')
						,'Код 1 уровня' = s.category_1_level_id 
						,'Название 1 уровня' = s.category_1_level_name
						,'Код 2 уровня' = s.category_2_level_id 
						,'Название 2 уровня' = s.category_2_level_name
						,'Код 3 уровня' = s.category_3_level_id
						,'Название 3 уровня' = s.category_3_level_name
						,'Код 4 уровня' = s.category_4_level_id
						,'Название 4 уровня' = s.category_4_level_name 
						,'Код 5 уровня' = s.category_5_level_id  
						,'Название 5 уровня' =  s.category_5_level_name
						,'Категория' = s.category_full_name
						,'Статус блокировки SKU' = s.product_status
						,'Статус блокировки SKU ручная' = sm.product_status_manual
						,'Код завода' = s.production_id
						,'Признак завода' = s.production_attribute
						,'Название завода' = s.production_name
						,'Код базовой позиции' = s.position_basic_id
						,'Код зависимой позиции' = s.position_dependent_id
						,'Код индивидуальной маркировки' = s.individual_marking_id
						,'Название индивидуальной маркировки' = s.individual_marking_name
						--,'Код PIM Z011' = FORMAT(s.product_finished_id, '#####################')
						--,'Код PIM Z013' = FORMAT(s.product_not_packaged_id, '#####################')
						,'Код PIM Z011' = convert(varchar(50), s.product_finished_id)
						,'Код PIM Z013' = convert(varchar(50), s.product_not_packaged_id)
						,'Код ТНВЭД' = s.FEACN_id
						,'Название ТНВЭД' = s.FEACN_name
						,'VAD/VOL' = s.vad_vol
						,'Сокращенное название бренда (Торговая марка)' = s.brand_trademark_short_name
						,'Название бренда (Торговая марка)' = s.brand_trademark_full_name
						,'Сокращенное название бренда (Назначение)' = s.brand_destination_short_name
						,'Название бренда (Назначение)' = s.brand_destination_full_name
						,'1С УПП' = s.UPP_1C_id
						,'Код 1С УПП ТМ' = s.UPP_TM_1C_id
						,'Код csb' = s.CSB_id
						,'Юнит' = s.unit_id
						,'Артикул номенклатуры' = s.article_nomenclature
						,'Артикул тары' = s.article_packaging
						,'Название SKU SAP MDG' = s.product_SAP_full_name
						,'Название SKU без завода и индивидуальной маркировки' = s.product_clean_full_name
						,'Название SKU 1С' = s.product_1C_full_name
						,'GTIN (ШК) штуки (CU)' = s.GTIN_CU_id
						,'GTIN (ШК) штуки (SKU)' = s.GTIN_SKU_id
						,'ГОСТ/ТУ продукции' = s.product_GOST_name
						,'Цех производства 1С' = s.production_shop_1C_name
						,'Наименование и вес тары без продукции' = s.box_name
						,'Размер единицы продукции ДхШхВ (мм)' = s.unit_size_name
						,'Количество коробов на поддоне (шт)' = s.quantity_box_on_pallet
						,'Количество вложений в короб (шт)' = s.quantity_in_box
						,'Единица хранения остатков' = s.product_storage_type
						,'Вес продукции в коробе нетто (кг)' = s.product_net_weight_in_box_kg
						,'Вес единицы продукции нетто (кг)' = s.unit_net_weight_kg
						,'Вес упаковки с единицы продукции (кг)' = s.packaging_unit_net_weight_kg
						,'Вес дополнительной упаковки в коробе (кг)' = s.packaging_net_weight_in_box_kg
						,'Вес нетто продукции на поддоне (кг)' = s.product_net_weight_on_pallet_kg
						,'Описание срока хранения и температурного режима' = s.product_storage_description
						,'Термосостояние' = s.freezing_type_name
						,'Общий срок годности' = s.expiration_date_in_days
						,'Тип срока годности' = s.expiration_date_type
						--,'срок годности в днях от' = s.expiration_date_in_days_from
						--,'срок годности в днях до' = s.expiration_date_in_days_to
						--,'Категория ОСГ' = s.category_residual_expiration_date
						,'НДС' = s.vat
						,'Дата обновления' = s.update_dt_tm
						--,'Пользователь обновил' = s.update_user	
						,'Кол-во дней для норматива остатков' = sm.number_days_normative_stock		 
						,'Код набивки' = sm.stuffing_id
						,'MML набивки' = sf.mml
						,'Название набивки' = sf.stuffing_name
						,'Цикл созревания' = sf.maturation_days
						,'Минимальный замес набивки' = sf.minimum_preparation_materials_kg
			from cherkizovo.info.products_sap as s
			left join project_plan_production_finished_products.info.finished_products_sap_id_manual as sm on s.SAP_id = sm.sap_id
			left join project_plan_production_finished_products.info.stuffing as sf on sm.stuffing_id = sf.stuffing_id
			where s.category_3_level_name in ('Колбасы сырокопченые');


			-- ПРОВЕРКА НАБИВОК
			IF OBJECT_ID('tempdb..#check_stuffing','U') is not null drop table #check_stuffing;

			-- ПРОВЕРКА КОГДА У ИСКЛЮЧЕНИЯ УКАЗАНО ИСКЛЮЧЕНИЕ
			IF OBJECT_ID('tempdb..#check_sap_id_correct_manual','U') is not null drop table #check_sap_id_correct_manual;

			-- НАИМЕНОВАНИЕ ВОЗРОЩАЕТ РАЗНЫЕ SAP ID
			IF OBJECT_ID('tempdb..#check_double_name_1c','U') is not null drop table #check_double_name_1c;
			
			-- АНАЛОГ ИМЕЕТ ИСКЛЮЧЕНИЕ И УКАЗАН НЕСКОЛЬКО РАЗ
			IF OBJECT_ID('tempdb..#check_sap_id_analog','U') is not null drop table #check_sap_id_analog;
			
end;



