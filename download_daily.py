import clickhouse_driver
import datetime
import os
import time

user = "default"
password = ""
visits_table = "visits_all"
hits_table = "hits_all"
database = "db_metrica"
path = "/home/ubuntu/api_metrica/metrica_logs_api.py"

client = clickhouse_driver.Client(host='localhost', user=user, password=password)

end_date = datetime.date.today() - datetime.timedelta(days=1)
delta = datetime.timedelta(days=3)
start_date = client.execute('''SELECT MAX(Date) FROM {}.{} UNION ALL SELECT Max(Date) FROM {}.{}'''.format(database,visits_table,database,hits_table))
start_date = start_date[1][0] if start_date[0][0] >= start_date[1][0] else start_date[0][0]


if end_date - start_date > datetime.timedelta(days=3):
    tmp_date = start_date + delta
    while True:
        if tmp_date >= end_date:
            os.system(
                'python2 /home/ntboy/Загрузки/logs_api_integration-master/metrica_logs_api.py -source hits -start_date '
                '{} -end_date {}'.format(start_date, end_date))
            os.system(
                'python2 /home/ntboy/Загрузки/logs_api_integration-master/metrica_logs_api.py -source visits '
                '-start_date {} -end_date {}'.format(start_date, end_date))
            break
        else:
            os.system(
                'python2 /home/ntboy/Загрузки/logs_api_integration-master/metrica_logs_api.py -source hits -start_date '
                '{} -end_date {}'.format(start_date, tmp_date))
            os.system(
                'python2 /home/ntboy/Загрузки/logs_api_integration-master/metrica_logs_api.py -source visits '
                '-start_date {} -end_date {}'.format(start_date, tmp_date))
        tmp_date += delta
        start_date += delta
else:
    os.system('python2 /home/ntboy/Загрузки/logs_api_integration-master/metrica_logs_api.py -source hits -start_date '
              '{} -end_date {}'.format(start_date, end_date))
    os.system('python2 /home/ntboy/Загрузки/logs_api_integration-master/metrica_logs_api.py -source visits -start_date '
              '{} -end_date {}'.format(start_date, end_date))

