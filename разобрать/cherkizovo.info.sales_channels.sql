/********************************/
-- ����� �����
-- �����: ����� �� 
-- ������: 20190925
/********************************/
sales channels
select * from cherkizovo.info.sales_channels

drop table cherkizovo.info.sales_channels

create table cherkizovo.info.sales_channels
(
	 sales_channel_id		smallint	not null
	,sales_channel_name		varchar(50)	not null	
	 CONSTRAINT [PK sales_channels | sales_channel_id] PRIMARY KEY CLUSTERED (sales_channel_id)
)

insert into cherkizovo.info.sales_channels
(sales_channel_id, sales_channel_name)
select 10, '�����������������'		union all
select 11, '����'					union all
select 12, '�������'				union all
select 13, 'HoReCa'					union all
select 14, '���'					union all
select 15, '�������'				union all
select 16, '������ �������'			union all
select 17, '������������'			union all
select 98, '��������� �������'		union all
select 99, '�� �������'				



