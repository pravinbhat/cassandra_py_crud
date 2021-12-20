from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from datetime import datetime
import json
import uuid
import sys


def create_prep_stmt(session, qry):
    try:
        p_stmt = session.prepare(qry)
    except:
        print("Error: ", sys.exc_info()[0], " occurred!!")
        return

    p_stmt.consistency_level = ConsistencyLevel.QUORUM
    return p_stmt

def create_connection():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['10.101.36.165'], auth_provider=auth_provider)
    return cluster.connect('test_pravin')

def create_user(session, first_name, last_name, age, city, email):
    if not first_name or not last_name or not email:
        print("first_name and last_name & email are required fields, however one or more of those are missing!!")
        return
    user_id = -1
    p_stmt = create_prep_stmt(session, "INSERT INTO users (user_id, first_name, last_name, age, city, email, created_date) VALUES (?,?,?,?,?,?,?)")
    if not p_stmt:
        return
    user_id = uuid.uuid1()
    session.execute(p_stmt, [user_id, first_name, last_name, age, city, email, datetime.now()])
    
    return user_id

def get_user(session, user_id):
    p_stmt = create_prep_stmt(session, "SELECT * FROM users WHERE user_id = ?")
    if not p_stmt:
        return    
    result = session.execute(p_stmt, [user_id]).one()
    print(json.dumps(result, indent=4, sort_keys=True, default=str))
    
def update_user(session, new_age, user_id):
    p_stmt = create_prep_stmt(session, "UPDATE users SET age =? WHERE user_id = ?")
    if not p_stmt:
        return    
    session.execute(p_stmt, [new_age, user_id])

def delete_user(session, user_id):
    p_stmt = create_prep_stmt(session, "DELETE FROM users WHERE user_id = ?")
    if not p_stmt:
        return    
    session.execute(p_stmt, [user_id])

def main():
    firstname = "Pravin"
    lastname = "Bhat"
    age = 20
    city = "Concord"
    email = "pravin@bhat.com"
    new_age = 21

    session = create_connection()
    user_id = create_user(session, firstname, lastname, age, city, email)
    if not user_id:
        print("Exception while inserting record, Exitting !!")
        return
    
    get_user(session, user_id)
    update_user(session, new_age, user_id)
    get_user(session, user_id)
    delete_user(session, user_id)

if __name__ == "__main__":
    main()
