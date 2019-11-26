use project_plan_production_finished_products

--exec project_plan_production_finished_products.check_import.stuffing_fact

go

alter procedure check_import.stuffing_fact			
as
BEGIN
			SET NOCOUNT ON;

			-- ����������� ���� ��������
			Update h
			Set h.stuffing_expiration_date = DateAdd(Day, i.expiration_date_in_days, h.stuffing_production_date_to)
			from project_plan_production_finished_products.data_import.stuffing_fact as h
			join project_plan_production_finished_products.info.stuffing as i on h.stuffing_id = i.stuffing_id;

					
			-- ����� ������
			Update h
			Set h.reason_ignore_in_calculate = 
				nullif(
							case when i.stuffing_id is null then '������� ����������� | ' else '' end 
						+ case when not h.stuffing_production_name is null and not i.production_name is null
								and h.stuffing_production_name <> i.production_name
															then '������������� ���������� � ����������� | ' else '' end 
						, '')
			from project_plan_production_finished_products.data_import.stuffing_fact as h
			left join project_plan_production_finished_products.info.stuffing as i on h.stuffing_id = i.stuffing_id;
						
						
			-- ����������� ���� �� ������ ��������� �������		
			update s
			set s.stuffing_before_next_available_date = l.stuffing_before_next_available_date
			from project_plan_production_finished_products.data_import.stuffing_fact as s
			join (
					select l.row_id, isnull(lead(l.stuffing_available_date) over (partition by l.stuffing_id order by l.stuffing_available_date) - 1, '29990101')  as stuffing_before_next_available_date
					from project_plan_production_finished_products.data_import.stuffing_fact as l
					) as l on s.row_id = l.row_id;


			-- ��������� ���������� ���� �������, ������� ����� ������������� ��� ��������
			update project_plan_production_finished_products.data_import.stuffing_fact
			set stuffing_row_id = row_id;

			-- ��������� ������ � excel
			select 
					 reason_ignore_in_calculate
					,stuffing_id
					,stuffing_production_name
					,stuffing_production_date_from
					,stuffing_production_date_to
					,stuffing_available_date
					,stuffing_before_next_available_date
					,stuffing_expiration_date
					,stuffing_kg
					,'����/��� �����' = ie.path_file
					,'������ �� ����' = ie.date_file
					,'��� ��������' = ie.user_insert
					,'���� � ����� ��������' = ie.dt_tm_insert
			from project_plan_production_finished_products.data_import.stuffing_fact as s
			join project_plan_production_finished_products.data_import.info_excel as ie on s.name_table = ie.name_table;

end;
