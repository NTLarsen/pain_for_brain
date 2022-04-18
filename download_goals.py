import clickhouse_driver
import requests
import os
import csv
from time import sleep

user = "default"
password = ""
goals_table = "goals_all"
database = "db_metrica"

client = clickhouse_driver.Client(host='localhost', user=user, password=password)
client.execute("DROP TABLE IF EXISTS {}.{}".format(database, goals_table))
sleep(10)

counter_id = "46297389"
token = "AQAAAAANZFHBAAdFlTN7nOklKkFEpDNrxkJhIJg"
host = 'https://api-metrika.yandex.net'
url = '{host}/management/v1/counter/{counter_id}/goals' \
    .format(counter_id=counter_id, host=host)
headers = {'Authorization': 'OAuth ' + token}

client.execute('''
CREATE TABLE IF NOT EXISTS {}.{}
(
	"Goal_id" UInt32,
	"Goal_name" String,
	"Goal_type" String,
	"Goal_is_retargeting" UInt8,
	"Goal_prev_goal_id" UInt32,
	"Goal_couner_id" UInt32
)ENGINE = TinyLog;'''.format(database, goals_table))

data_req = requests.get(url, headers=headers).json()
list = []
for goals in data_req["goals"]:
    goal_id = goals.get("id")
    goal_name = goals.get("name").encode('utf-8')
    goal_type = goals.get("type").encode('utf-8')
    goal_is_retargeting = goals.get("is_retargeting")
    goal_prev_goal_id = goals.get("prev_goal_id")
    list.append([goal_id, goal_name, goal_type, goal_is_retargeting, goal_prev_goal_id, counter_id])

print(list)
with open('goals.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(list)

os.system('''cat goals.csv | clickhouse-client  --query="INSERT INTO {}.{} FORMAT CSV"'''.format(database, goals_table))
