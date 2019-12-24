select *
from project_plan_production_finished_products.info.finished_products_sap_id_manual


-- drop table project_plan_production_finished_products.info.sap_id_manual
create table project_plan_production_finished_products.info.sap_id_manual
(
		 sap_id_type			varchar(20)		not null
		,sap_id					bigint			not null
		,sap_id_correction		bigint				null
		,stuffing_id_group		tinyint				NULL
		,stuffing_id			varchar(40)			NULL
		,stuffing_id_share		dec(7,5)			NULL
)



insert into project_plan_production_finished_products.info.sap_id_manual (sap_id_type, sap_id)

select 'Основной', sap_id
from cherkizovo.info.products_sap
where category_3_level_name in ('Колбасы сырокопченые');


 select * from project_plan_production_finished_products.info.sap_id_manual

exec project_plan_production_finished_products.report.sap_id_manual




alter procedure report.sap_id_manual @sap_id_type			varchar(20)		= null
									,@sap_id				bigint			= null	
									,@sap_id_correction		bigint			= null	
									,@stuffing_id			varchar(20)		= null		
									,@stuffing_id_share		dec(7,5)		= null		
as
BEGIN
			SET NOCOUNT ON;


			if @sap_id_type			is null and 			
			   @sap_id				is null and 
			   @sap_id_correction	is null and 	
			   @stuffing_id			is null and
			   @stuffing_id_share	is null 
			begin

					begin -- обновляем справочник		
					
							insert into project_plan_production_finished_products.info.sap_id_manual (sap_id_type, sap_id , sap_id_correction)
							select 'Основной', sap_id , null
							from cherkizovo.info.products_sap
							where category_3_level_name in ('Колбасы сырокопченые')
							  and not sap_id in (select sap_id from project_plan_production_finished_products.info.sap_id_manual)

							union all

							select 'Полуфабрикат', sap_id, sap_id
							from cherkizovo.info.products_sap
							where category_3_level_name in ('Колбасы сырокопченые')
							  and not sap_id in (select sap_id from project_plan_production_finished_products.info.sap_id_manual);
					
					end;


					
					begin -- выгружаем справочник	

					
							select 
									 'Тип SAP ID'			= sm.sap_id_type
									,'SAP ID'				= convert(varchar(24), FORMAT(sm.sap_id				, '000000000000000000000000') )			
									,'SAP ID корректировка' = convert(varchar(24), FORMAT(sm.sap_id_correction	, '000000000000000000000000') )						
									,'Набивка'				= sm.stuffing_id			
									,'Доля набивки'			= sm.stuffing_id_share
									
									,'CSB код'				= COALESCE(p.csb_id ,	s.csb_id)
									,'SAP MDG'				= COALESCE(p.position_dependent_id ,	s.position_dependent_id)
									,'ИМ'					= COALESCE(p.individual_marking_id,		s.individual_marking_id)
									,'ИМ название'			= COALESCE(p.individual_marking_name,	s.individual_marking_name)
									,'1С название'			= COALESCE(p.product_1C_full_name,		s.product_1C_full_name)
								
							from project_plan_production_finished_products.info.sap_id_manual as sm
							left join cherkizovo.info.products_sap as p on sm.sap_id = p.sap_id and sm.sap_id_type = 'Основной'	
							left join cherkizovo.info.products_sap as s on sm.sap_id_correction = s.sap_id and sm.sap_id_type = 'Полуфабрикат'	
							order by sm.sap_id
									,case  sm.sap_id_type	
											when 'Основной'		then 1
											when 'Полуфабрикат' then 3
							
											else 99

									 end
									,sm.sap_id_correction
					end;	
					
					return(0);						
			end;








			declare @err varchar(500);
				set @err = '';


				set @err = case 
								when isnull(@sap_id, 0) = 0								then 'Столбец [SAP ID] должен содержать код SAP ID'
								when not @sap_id_type in ('Основной', 'Полуфабрикат')	then 'Столбец [Тип SAP ID] должен содержать: Основной / Полуфабрикат'
								else ''
						   end


			if @err <> ''
			begin
					select @err as err
					return(1);
			end;



			IF EXISTS (SELECT 1 
					   FROM project_plan_production_finished_products.info.sap_id_manual 
					   WHERE sap_id_type = @sap_id_type
					     and sap_id = @sap_id
					     and isnull(sap_id_correction, 0) = isnull(@sap_id_correction, 0))
			BEGIN
					UPDATE project_plan_production_finished_products.info.sap_id_manual 
					set	 stuffing_id = @stuffing_id
						,stuffing_id_share = @stuffing_id_share
					WHERE sap_id_type = @sap_id_type
					  and sap_id = @sap_id
					  and isnull(sap_id_correction, 0) = isnull(@sap_id_correction, 0);
			END
			ELSE
			begin
					INSERT project_plan_production_finished_products.info.sap_id_manual 
						  ( sap_id_type,  sap_id,  sap_id_correction,  stuffing_id,  stuffing_id_share)
					values(@sap_id_type, @sap_id, @sap_id_correction, @stuffing_id, @stuffing_id_share);
			END
			
			select 'ok' as err

end;