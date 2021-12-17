from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import json
import uuid

def create_connection():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['10.101.36.165'], auth_provider=auth_provider)
    return cluster.connect('test_pravin')

def create_user(session, first_name, last_name, age, city, email):
    user_id = uuid.uuid1()
    session.execute("INSERT INTO users (user_id, first_name, last_name, age, city, email, created_date) VALUES (%s,%s,%s,%s,%s,%s,%s)", 
    [user_id, first_name, last_name, age, city, email, datetime.now()])
    return user_id

def get_user(session, user_id):
    result = session.execute("SELECT * FROM users WHERE user_id = %s", [user_id]).one()
    print(json.dumps(result, indent=4, sort_keys=True, default=str))
    
def update_user(session, new_age, user_id):
    session.execute("UPDATE users SET age =%s WHERE user_id = %s", [new_age, user_id])

def delete_user(session, user_id):
    session.execute("DELETE FROM users WHERE user_id = %s", [user_id])

def main():
    firstname = "Pravin"
    lastname = "Bhat"
    age = 20
    city = "Concord"
    email = "pravin@bhat.com"
    new_age = 21

    session = create_connection()
    user_id = create_user(session, firstname, lastname, age, city, email)
    get_user(session, user_id)
    update_user(session, new_age, user_id)
    get_user(session, user_id)
    delete_user(session, user_id)

if __name__ == "__main__":
    main()
