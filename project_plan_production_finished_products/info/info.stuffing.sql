/***************/
/*** набивка ***/
/***************/
use project_plan_production_finished_products

-- таблица
select * from project_plan_production_finished_products.info.stuffing
--drop table info.stuffing
CREATE TABLE project_plan_production_finished_products.info.stuffing
(
		 stuffing_id						VARCHAR(40)		NOT NULL	
		,stuffing_name						VARCHAR(200)	NOT NULL	
		,stuffing_id_box					varchar(40)			NULL
		,stuffing_share_in_box				decimal(8, 7)		NULL
		,stuffing_type						VARCHAR(50)			NULL
		,mml								VARCHAR(3)			null
		,production_name					VARCHAR(15)			null
		,stuffing_group						tinyint				null

		,expiration_date_in_days			SMALLINT			NULL
		,KOS_for_shipment					DEC(3,2)			NULL

		,fermentation_chamber_days			tinyint				null
		,maturation_days					TINYINT				NULL
		,maturation_and_packaging_days		TINYINT				NULL
		,transit_from_production_days		TINYINT				NULL
		
		,minimum_preparation_materials_kg	dec(9,3)			null
		,count_chamber						tinyint				null
		,minimum_volume_for_chamber_kg		dec(9,3)			null
		,minimum_volume_for_marking_kg		dec(9,3)			null
		,step_marking_kg					dec(9,3)			null
	
		,marking_line_type					VARCHAR(20)			null	
		,marking_line_productivity_kg		dec(11,5)			null

		CONSTRAINT [PK stuffing | stuffing_id] PRIMARY KEY CLUSTERED (stuffing_id) 
);


-- набивка
ALTER VIEW info_view.stuffing
AS

	SELECT 'Код набивки' = s.stuffing_id
		  ,'Название набивки' = s.stuffing_name
		  ,'Код набивки коробка' = s.stuffing_id_box
		  ,'Доля набивки в коробке' = s.stuffing_share_in_box
		  ,'Тип набивки' = s.stuffing_type
		  ,'MML' = s.mml
		  ,'Производитель' = s.production_name
		  ,'Группа набивок' = s.stuffing_group
		  ,'Cрок годности в днях' = s.expiration_date_in_days
		  ,'ОСГ для отгрузки набивки' = s.KOS_for_shipment
		  ,'Цикл созревания' = s.maturation_days
		  ,'Цикл созревания + упаковка' = s.maturation_and_packaging_days
		  ,'Транзит с производства' = s.transit_from_production_days
		  ,'Дней в камере копчения' = s.fermentation_chamber_days
		  ,'Минимальный замес набивки' = s.minimum_preparation_materials_kg
		  ,'Минимальный объем закладки (камеры)' = s.minimum_volume_for_chamber_kg
		  ,'Ограничения по камерам' = s.count_chamber
		  ,'Тип упаковочной линии' = s.marking_line_type
		  ,'Упаковочная линия кг в час' = s.marking_line_productivity_kg
		  ,'Минимальный квант маркировки' = s.minimum_volume_for_marking_kg
		  ,'Кратность маркировки' = s.step_marking_kg
	  FROM project_plan_production_finished_products.info.stuffing as s

