use project_plan_production_finished_products



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




		select 
				 si.sap_id_type	
				,si.sap_id
				,iif(si.sap_id_type = 'Основной', si.sap_id, null) as sap_id_join
				,si.sap_id_corrected
		from info.sap_id as si
		join cherkizovo.info.products_sap as s on si.sap_id = s.sap_id
		left
		join cherkizovo.info.products_sap as c on si.sap_id_corrected = c.sap_id