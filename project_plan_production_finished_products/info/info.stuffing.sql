/***************/
/*** набивка ***/
/***************/
use project_plan_production_finished_products

-- таблица
select * from info.stuffing
--drop table info.stuffing
CREATE TABLE info.stuffing
(
		 stuffing_id						VARCHAR(40)		NOT NULL																					
		,stuffing_name						VARCHAR(200)	NOT NULL																				
		
		,stuffing_box_1						varchar(100)		NULL
		,stuffing_share_box_1				decimal(8, 7)		NULL
		,stuffing_box_2						varchar(100)		NULL
		,stuffing_share_box_2				decimal(8, 7)		NULL
		,stuffing_box_3						varchar(100)		NULL
		,stuffing_share_box_3				decimal(8, 7)		NULL
																																			
		,stuffing_type						VARCHAR(50)			NULL																					
		,mml								VARCHAR(3)			null																					
		,production_name					VARCHAR(15)			null																					
		,stuffing_group						tinyint				null																					
																																						
		,fermentation_and_maturation_days	tinyint				null																						
		,packaging_days						TINYINT				NULL																					
		,transit_days						TINYINT				NULL																					
																																						
		,stuffing_minimum_volume_kg			dec(9,3)			null																					
		,chamber_count						tinyint				null																					
		,chamber_minimum_volume_kg			dec(9,3)			null																					
																																						
		,prepack_marking_minimum_kg			dec(9,3)			null																					
		,prepack_marking_step_kg			dec(9,3)			null																					
																																						
		,marking_minimum_kg					dec(9,3)			null																					
		,marking_step_kg					dec(9,3)			null																					
																																						
		,marking_line_type					VARCHAR(20)			null																					
		,marking_line_productivity_kg		dec(11,5)			null																					

		,CONSTRAINT [PK stuffing | stuffing_id] PRIMARY KEY CLUSTERED (stuffing_id)
);




select * from info_view.stuffing
-- набивка
ALTER VIEW info_view.stuffing
AS

	SELECT 
		   'Дейcтвие'						= ''
		  ,'Код набивки'					= s.stuffing_id
		  ,'Название набивки'				= s.stuffing_name
		  ,'Коробка 1'						= s.stuffing_box_1			
		  ,'Доля 1'							= s.stuffing_share_box_1	
		  ,'Коробка 2'						= s.stuffing_box_2			
		  ,'Доля 2'							= s.stuffing_share_box_2	
		  ,'Коробка 3'						= s.stuffing_box_3			
		  ,'Доля 3'							= s.stuffing_share_box_3
		  ,'Тип набивки'					= s.stuffing_type
		  ,'MML'							= s.mml
		  ,'Производитель'					= s.production_name
		  ,'Группа набивок'					= s.stuffing_group
		  
		  ,'Созревание'						= s.fermentation_and_maturation_days
		  ,'Упаковка'						= s.packaging_days
		  ,'Транзит'						= s.transit_days

		  ,'Мин замес набивки'				= s.stuffing_minimum_volume_kg
		  ,'Ограничения по камерам'			= s.chamber_count
		  ,'Мин объем закладки в камеру'	= s.chamber_minimum_volume_kg
		  
		  ,'ПФ Мин квант'					= s.prepack_marking_minimum_kg
		  ,'ПФ Кратность'					= s.prepack_marking_step_kg
		  
		  ,'ГП Мин квант'					= s.marking_minimum_kg		  
		  ,'ГП Кратность'					= s.marking_step_kg

		  ,'Тип упаковочной линии'			= s.marking_line_type
		  ,'Упаковочная линия кг в час'		= s.marking_line_productivity_kg
		 
	  FROM info.stuffing as s






