import clickhouse_driver

user = "default"
password = ""
goals_table = "visits_goals_view"
database = "db_metrica"

client = clickhouse_driver.Client(host='localhost', user=user, password=password,database= database)
with open('visits_goals_sql/b2c_visits_goals.sql') as file:
    query = file.read()
    client.execute(query)

