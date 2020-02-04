use project_smoked_sausages_production_planning

--select * from info.finished_products_sap_id_manual
--select * from info.sap_id
/*
-- TRUNCATE TABLE info.sap_id
-- drop table info.sap_id
create table info.sap_id
(
		 sap_id_type				varchar(20)		not null
		,sap_id						bigint			not null
		,sap_id_corrected			bigint				null 
		,need_before_date			datetime			null
		,count_days_normative_stock	smallint			null
		,stuffing_id_group			tinyint				NULL
		,stuffing_id_share			dec(7,5)			NULL
		,stuffing_id				varchar(40)			NULL

		
		,product_status				varchar(100)		NULL
		,product_1C_full_name		varchar(200)		NULL
		,product_clean_full_name	varchar(500)		NULL
		,product_name_analog		varchar(200)		NULL

		,production_name			varchar(40)			NULL
		,production_attribute		varchar(2)			NULL	 
		,production_sap_id			varchar(2)			NULL
		,production_full_name		as production_name + ' | ' + production_attribute + ' | ' + production_sap_id 

		,CSB_id						decimal(6, 0)		NULL
		
		,position_dependent_id		int					NULL
		,product_finished_id		DEC(14)				NULL	-- Код PIM Z011
		,individual_marking_id		tinyint				NULL
		,position_dependent_and_individual_marking as convert(dec(12,0), position_dependent_id) * 100 +  individual_marking_id
		,individual_marking_name	varchar(50)			NULL

		,article_nomenclature		VARCHAR(20)			NULL	-- Артикул номенклатуры
		,article_packaging			VARCHAR(25)			NULL	-- Артикул тары

		,expiration_date_in_days	smallint			NULL

		,check ((sap_id_type = 'Основной' and sap_id_corrected is null) or (sap_id_type in ('Потребность', 'Полуфабрикат')))
		,CONSTRAINT [UQ sap_id | sap_id_type, sap_id, sap_id_corrected] UNIQUE(sap_id_type, sap_id, sap_id_corrected)
)



*/

--alter table info.sap_id
--add product_clean_full_name varchar(500)		NULL
	


go

-- exec report.sap_id
alter procedure report.sap_id @CheckBoxHideSapIdCorrected bit = 0
as
BEGIN
			SET NOCOUNT ON;

			begin -- обновляем справочник		
					
					-- добавляем данные
					insert into info.sap_id (sap_id_type, sap_id , sap_id_corrected)
					select 'Основной', sap_id , null
					from cherkizovo.info.products_sap
					where category_3_level_name in ('Колбасы сырокопченые')
						and not sap_id in (select sap_id from info.sap_id);

					
					update si
					set 
						 si.product_status				= sp.product_status						
						,si.product_1C_full_name		= sp.product_1C_full_name
						,si.product_clean_full_name		= sp.product_clean_full_name		
						,si.production_name				= sp.production_name				
						,si.production_attribute		= sp.production_attribute		
						,si.production_sap_id			= sp.production_sap_id			
						,si.CSB_id						= sp.CSB_id						
						,si.position_dependent_id		= sp.position_dependent_id
						,si.product_finished_id			= sp.product_finished_id		
						,si.individual_marking_id		= sp.individual_marking_id		
						,si.individual_marking_name		= sp.individual_marking_name	
						,si.article_nomenclature		= sp.article_nomenclature				
						,si.article_packaging			= sp.article_packaging				
						,si.expiration_date_in_days		= sp.expiration_date_in_days		

					from info.sap_id as si
					join cherkizovo.info.products_sap as sp on si.sap_id = sp.sap_id
					where si.sap_id_type = 'Основной';
					
					
					-- берем потребность из старой таблицы
					if 1<>1
					begin
							insert into info.sap_id (sap_id_type, sap_id , sap_id_corrected)
							select 'Потребность', sap_id_shipment_manual, sap_id 
							from info.finished_products_sap_id_manual
							where not sap_id_shipment_manual is null;
					
					
							update s
							set s.stuffing_id = p.stuffing_id
								,s.count_days_normative_stock = p.number_days_normative_stock
							from info.sap_id as s
							join (
									select sap_id, stuffing_id, number_days_normative_stock
									from info.finished_products_sap_id_manual
								 ) as p on s.sap_id = p.sap_id and s.sap_id_type = 'Основной';


							insert into info.sap_id (sap_id_type, sap_id , sap_id_corrected)
							values('Полуфабрикат', 000000001030628915300108, 000000001030629093301301)
								 ,('Полуфабрикат', 000000001030628915300108, 000000001030629193301301);
								 
							insert into info.sap_id (sap_id_type, sap_id , sap_id_corrected, need_before_date)
							select 'Потребность', 000000001030634921301301 ,000000001030639871300101, '20200114';
							
							update info.sap_id
							set product_name_analog = 'СК САЛЯМИ ТРАДИЦИОННАЯ ФИБ ВУ ШТ 0.3КГ (ТМ СТМ)'
							where sap_id_type = 'Основной'
							  and sap_id in (1030606036300118, 1030606036300418, 1030637036300418)







					end;


					-- если заблокировано, то меняем набивку на снято с производства и удаляем ПФ
					delete from info.sap_id
					where sap_id_type = 'Полуфабрикат'
					  and sap_id in (select sap_id
									 from info.sap_id as s
									 where isnull(s.product_status, '') in ('БлокирДляЗаготов/Склада', 'Устаревший')
									   and s.sap_id_type = 'Основной'
									);

					update info.sap_id
					set stuffing_id = 'Снята с производства'
					where sap_id_type = 'Основной'
					  and isnull(product_status, '') in ('БлокирДляЗаготов/Склада', 'Устаревший');


					update info.sap_id
					set stuffing_id = 'Укажите код набивки'
					where sap_id_type = 'Основной'
					  and stuffing_id is null;

			end;


			begin -- выгружаем справочник	

					select 
							 'Дейcтвие'				= ''
							,'Тип SAP ID'			= si.sap_id_type
							,'SAP ID'				= convert(varchar(24), FORMAT(si.sap_id				, '000000000000000000000000') )			
							,'SAP ID корректировка'	= convert(varchar(24), FORMAT(iif(si.sap_id_type <> 'Основной', si.sap_id_corrected, null), '000000000000000000000000') )			
							,'Потребность до'		= si.need_before_date
							,'Дни норм ост'			= si.count_days_normative_stock					
							,'Набивка'				= si.stuffing_id_manual	
							--,'Группа набивок'		= si.stuffing_id_group	
							,'Доля набивки'			= si.stuffing_id_share	


							,'Ошибки в справочнике' = case
														when si.sap_id_type = 'Полуфабрикат' and isnull(si.product_status, '') in ('БлокирДляЗаготов/Склада', 'Устаревший')		then 'ПФ заблокирован'
														when si.sap_id_type = 'Полуфабрикат' and ISNUMERIC(si.stuffing_id) = 0													then 'ПФ укажите корретный номер набивки'
														when si.sap_id_type = 'Полуфабрикат' and not isnull(si.stuffing_id_share,0) between 0.1 and 1 							then 'ПФ укажите долю набивки'
														when si.sap_id_type = 'Основной'	 and isnull(si.stuffing_id, '') in ('Укажите код набивки')							then 'Укажите код набивки'
														when si.sap_id_type = 'Основной'	 and ISNUMERIC(si.stuffing_id) = 1 and isnull(si.count_days_normative_stock,0) = 0	then 'Укажите кол-во дней для норматива остатков'
													  end
													  
													  						
							,'Набивка/ПФ'			= si.stuffing_id
							,'Статус артикула'		= si.product_status

							,'1С название'			= si.product_1C_full_name				

							,'Завод'				= si.production_full_name
							,'CSB код'				= si.csb_id
							,'Код PIM Z011'			= si.product_finished_id
							,'Код завис поз и ИМ'	= si.position_dependent_and_individual_marking 
							,'ИМ название'			= si.individual_marking_name 
							  
							,'Артикул номенклатуры'	= si.article_nomenclature
							,'Артикул тары'			= si.article_packaging
							,'Срок годности'		= si.expiration_date_in_days
					
					from info_view.sap_id as si	
					where iif(@CheckBoxHideSapIdCorrected = 0, null, si.sap_id_corrected_need) is null
					order by 
							 si.sap_id
							,case  si.sap_id_type	
									when 'Основной'		then 1
									when 'Потребность'	then 2
									when 'Полуфабрикат' then 3
							 end
							,iif(si.sap_id_type <> 'Основной', si.sap_id_corrected, null);

			end;	
				

end;





/*


select * from info_view.sap_id


ALTER VIEW info_view.sap_id
AS
		select 			 
			 sap_id_for_join							= iif(si.sap_id_type = 'Основной', si.sap_id, null)
			,si.sap_id_type
			,si.sap_id					
			,sap_id_corrected							= iif(si.sap_id_type = 'Основной', si.sap_id, si.sap_id_corrected)
			,si.need_before_date
			,si.count_days_normative_stock					
			,stuffing_id_manual							= si.stuffing_id
			,si.stuffing_id_group	
			,si.stuffing_id_share	
			,si.product_name_analog
							  						
			,stuffing_id								= iif(si.sap_id_type = 'Основной', si.stuffing_id, sp.stuffing_id)
			,product_status								= iif(si.sap_id_type = 'Основной', si.product_status, sp.product_status)

			,product_1C_full_name						= iif(si.sap_id_type = 'Основной', si.product_1C_full_name, sp.product_1C_full_name)
			,product_clean_full_name					= iif(si.sap_id_type = 'Основной', si.product_clean_full_name, sp.product_clean_full_name)						

			,production_full_name						= iif(si.sap_id_type = 'Основной', si.production_full_name, sp.production_full_name)	 
			,csb_id										= iif(si.sap_id_type = 'Основной', si.csb_id, sp.csb_id)

			
			,product_finished_id						= iif(si.sap_id_type = 'Основной', si.product_finished_id, sp.product_finished_id)
			,position_dependent_id						= iif(si.sap_id_type = 'Основной', si.position_dependent_id, sp.position_dependent_id)
			,individual_marking_id						= iif(si.sap_id_type = 'Основной', si.individual_marking_id, sp.individual_marking_id)

			,position_dependent_and_individual_marking	= iif(si.sap_id_type = 'Основной', si.position_dependent_and_individual_marking, sp.position_dependent_and_individual_marking)   
			,individual_marking_name					= iif(si.sap_id_type = 'Основной', si.individual_marking_name, sp.individual_marking_name)    
	
			,article_nomenclature						= iif(si.sap_id_type = 'Основной', si.article_nomenclature, sp.article_nomenclature)  
			,article_packaging							= iif(si.sap_id_type = 'Основной', si.article_packaging, sp.article_packaging)  

			,expiration_date_in_days					= iif(si.sap_id_type = 'Основной', si.expiration_date_in_days, sp.expiration_date_in_days)  

			,(select max(1) 
			  from info.sap_id as nd
			  where si.sap_id = nd.sap_id_corrected
			    and si.sap_id_type = 'Основной'
				and nd.sap_id_type = 'Потребность') as sap_id_corrected_need
		from info.sap_id as si
		left join info.sap_id as sp on si.sap_id_corrected = sp.sap_id and sp.sap_id_type = 'Основной';




*/
