���006_1-30 ����
���007_31-35 ����
���008_36-45 ����
���009_46-60 ����
���010_61-90 ����
���011_91-365 ����
���012_366-999 ����

-- ��� ���� �������� ��� SKU, � SKU ���� ���� ��������, �������� 45 ����, ������ ��������� ����� 8
drop table cherkizovo.information.[��������� ���]
create table cherkizovo.information.[��������� ���]
(
	 [�� ��������� ���]	tinyint not null
	,[���� ��]			smallint not null
	,[���� ��]			smallint not null	
	CONSTRAINT [�� ��������� ���] PRIMARY KEY CLUSTERED ([�� ��������� ���]) 
)


insert into cherkizovo.information.[��������� ���]
values(6,	1,		30)
	 ,(7,	31,		35)
	 ,(8,	36,		45)
	 ,(9,	46,		60)
	 ,(10,	61,		90)
	 ,(11,	91,		365)
	 ,(12,	366,	999)

select * from cherkizovo.information.[��������� ���]
