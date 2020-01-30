select
	 [SAP ID]										as sap_id					
													
	,[Код 1 уровня]									as category_1_level_id 	
	,[Название 1 уровня]							as category_1_level_name 	
	,[Код 2 уровня]									as category_2_level_id 	
	,[Название 2 уровня]							as category_2_level_name 		
	,[Код 3 уровня]									as category_3_level_id 	
	,[Название 3 уровня]							as category_3_level_name 		
	,[Код 4 уровня]									as category_4_level_id 	
	,[Название 4 уровня]							as category_4_level_name 		
	,[Код 5 уровня]									as category_5_level_id 	
	,[Название 5 уровня]							as category_5_level_name 	
	,[Категория]									as category_full_name 	
												
	,[Код завода]									as production_id			
	,[Площадка]										as production_name
	,[Цех производства 1С]							as production_shop_1C_name
												
	,[1С УПП]										as upp_1C_id		
	,[Код 1С УПП ТМ]								as upp_tm_1C_id				
	,[Код csb]										as csb_id	
	,[Юнит]											as unit_id	

	,[Код базовой позиции]							as position_basic_id		
	,[Код зависимой позиции]						as position_dependent_id	
	,[Код PIM Z011]									as product_finished_id	
	,[Код PIM Z013]									as product_not_packaged_id
	

	,[Код индивидуальной маркировки]				as individual_marking_id			
	,[Индивидуальная маркировка]					as individual_marking_name		

	,[Бренд (Торговая марка)]						as brand_trademark_short_name		
	,[Бренд (Торговая марка)1]						as brand_trademark_full_name		

	,[Бренд (назначение)]							as brand_destination_short_name	
	,[Бренд (назначение)1]							as brand_destination_full_name	

	,[Артикул номенклатуры]							as article_nomenclature
	,[Артикул тары]									as article_packaging	
														   		
	,[Полное наименование зависимой продукта]		as product_sap_full_name			

	,[ГОСТ/ТУ продукции]							as product_gost_name	
	,[GTIN (ШК) штуки (CU)]							as gtin_cu_id			
	,[GTIN (ШК) штуки (SKU)]						as gtin_sku_id		
	,[Код ТНВЭД]									as feacn_id			
	,[Название ТНВЭД]								as feacn_name			
	,[VAD/VOL]										as vad_vol			

	,[Статус материала]								as product_status

	,[Единица хранения остатков]					as product_storage_type
	
	,[Размер единицы продукции ДхШхВ (мм)]			as unit_size_name

	,[Вес единицы продукции нетто (кг)]				as unit_net_weight_kg				
	,[Вес упаковки с единицы продукции (кг)]		as packaging_unit_net_weight_kg	
	,[Вес продукции в коробе нетто (кг)]			as product_net_weight_in_box_kg	
	,[Вес дополнительной упаковки в коробе (кг)]	as packaging_net_weight_in_box_kg	
	,[Вес нетто продукции на поддоне (кг)]			as product_net_weight_on_pallet_kg
	,[Количество вложений в короб (шт)]				as quantity_in_box				
	,[Количество коробов на поддоне (шт)]			as quantity_box_on_pallet				
	,[Наименование и вес тары без продукции]		as box_name						

	,[Срок хранения и температурные режимы]			as product_storage_description
	,[Термосостояние]								as freezing_type_id			
	,[Термосостояние1]								as freezing_type_name			

    ,[Срок хранения]								as expiration_date_type
    ,[Общий срок годности1]							as expiration_date_in_days

    ,[Ставка налога]								as vat	

from [Sheet1$]
