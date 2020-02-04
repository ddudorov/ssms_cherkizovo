----------------------
-- data create: 201912
-- name send mail
-- author: RoshchinMU
-- description: отправка письма
---------------------

use monitoring;

exec monitoring.dbo.send_mail @subject = 'тест 1'	,@body = '123123'
							




alter procedure dbo.send_mail @recipients		varchar(max) = 'm.roshchin@cherkizovo.com;d.dudorov@cherkizovo.com;'
							 ,@copy_recipients	varchar(500) = null
							 ,@subject			varchar(500)
							 ,@body				varchar(max) = null
							 ,@file_attachments	varchar(500) = null
as
BEGIN
			SET NOCOUNT ON;


			set @body = '<!DOCTYPE html>
						<html>
							<head>
							<meta charset="utf-8">								   
							<style type="text/css">
								html,body{margin: 0;padding: 0;font-size: 12px;font-family: Verdana, Geneva, Tahoma, sans-serif; }								
								a{color: #B22222;}
								table, tr, th, td{border: 1px solid #B22222;border-collapse: collapse ;margin: 3px;}
								#select_row{background: rgb(170, 255, 168);}
							</style>
							</head>
							<body>
							<header>
								Добрый день, получатель!<br>
								<b>Данное письмо было отправлено автоматически.</b>
							</header>
							<b> ' + iif(@body is null, '<br><br>', '<br><br>' + @body + '<br><br>') +  '</b>

							<footer>
								Данную рассылку осуществляет:<br>
								Дирекция закупок и логистики<br>
								Управление цепями поставок<br>
								Направление развития логистики и цепи поставок<br><br>
								<a href="mailto:m.roshchin@cherkizovo.com;d.dudorov@cherkizovo.com?subject=Укажите тему&body=Укажите вопрос"><b>Написать нам</b></a><br><br>

								Наш портал: <a href="http://automatization.cherkizovsky.net/">automatization.cherkizovsky.net</a>

							</footer>
							</body> 
						</html>';
			
					  
			exec msdb.dbo.sp_send_dbmail @recipients		= @recipients
										,@copy_recipients	= @copy_recipients
										,@subject			= @subject
										,@file_attachments	= @file_attachments
										,@body				= @body   
										,@body_format		= 'html' 

end











