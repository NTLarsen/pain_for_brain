import clickhouse_driver
import datetime
import calendar

user = "default"
password = ""
database = "db_metrica"

client = clickhouse_driver.Client(host='localhost', user=user, password=password, database=database)

last_partition_vm = client.execute(
    '''
        SELECT
            partition 
        FROM system.parts
        WHERE table = 'visits_goals_view'
            AND active
        ORDER BY partition desc
        LIMIT 1;
    '''
)
last_partition_vm = last_partition_vm[0][0]
print(last_partition_vm)

max_partition_va = client.execute(
    '''
        SELECT
            partition 
        FROM system.parts
        WHERE table = 'visits_all'
            AND active
        ORDER BY partition desc
        LIMIT 1;
    '''
)
max_partition_va = max_partition_va[0][0]
print(max_partition_va)

parts_to_download = int(max_partition_va) - int(last_partition_vm)
print(parts_to_download)
if parts_to_download > 1:
    # find min max dates
    date1 = last_partition_vm[:4] + '-' + last_partition_vm[4:] + '-01'
    date = datetime.datetime.strptime(date1, "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime(max_partition_va[:4] + '-' + max_partition_va[4:] + '-01', "%Y-%m-%d").date()
    with open('tmp_visits_goals_sql/tmp_metrica_visits.sql') as file:
        query = file.read()
        print("DATE BETWEEN")
        print(date1,str(date2 + datetime.timedelta(days=31)))
        client.execute(query.format(date1, str(date2 + datetime.timedelta(days=31))))
    partition = client.execute(
        '''
            SELECT
                partition 
            FROM system.parts
            WHERE table = 'tmp_visits_goals_view'
                AND active
            ORDER BY partition desc
            LIMIT {};
        '''.format(parts_to_download + 1)
    )
    print(partition)
    for i in range(parts_to_download + 1):
        # Загружаем партиции во Вью
        print("IN VIEW")
        print(i)
        with open('tmp_visits_goals_sql/tmp_mv_replace_parts.sql') as file:
            query = file.read()
            client.execute(query.format(partition[i][0]))


else:
    # Скачиваем последние 2 месяца
    partition = client.execute(
        '''
            SELECT
                partition 
            FROM system.parts
            WHERE table = 'visits_goals_view'
                AND active
            ORDER BY partition desc
            LIMIT 2;
        '''
    )
    print("PARTIT")
    print(partition[1][0], partition[0][0])
    with open('tmp_visits_goals_sql/tmp_metrica_visits.sql') as file:
        query = file.read()
        client.execute(query.format(partition[1][0][:4]+'-'+partition[1][0][4:6]+'-01',
                                    str(datetime.datetime.strptime(partition[0][0][:4]+'-'+partition[0][0][4:6]+'-01',
                                                                   "%Y-%m-%d").date() + datetime.timedelta(days=31))))
    # Загружаем партиции во Вью
    with open('tmp_visits_goals_sql/tmp_mv_replace_parts.sql') as file:
        query = file.read()
        client.execute(query.format(partition[0][0]))
        client.execute(query.format(partition[1][0]))

# Дроп tmp
client.execute('DROP TABLE IF EXISTS tmp_visits_goals_view')
