from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def create_connection():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['10.101.36.165'], auth_provider=auth_provider)
    return cluster.connect('test_pravin')

def create_user(session, first_name, last_name, age, city, email):
    session.execute("INSERT INTO users (first_name, last_name, age, city, email) VALUES (%s,%s,%s,%s,%s)", [first_name, last_name, age, city, email])

def get_user(session, first_name):
    result = session.execute("SELECT * FROM users WHERE first_name = %s", [first_name]).one()
    print(result)
    
def update_user(session, new_age, first_name):
    session.execute("UPDATE users SET age =%s WHERE first_name = %s", [new_age, first_name])

def delete_user(session, first_name):
    session.execute("DELETE FROM users WHERE first_name = %s", [first_name])

def main():
    firstname = "Pravin"
    lastname = "Bhat"
    age = 20
    city = "Concord"
    email = "pravin@bhat.com"
    new_age = 21

    session = create_connection()
    create_user(session, firstname, lastname, age, city, email)
    get_user(session, firstname)
    update_user(session, new_age, firstname)
    get_user(session, firstname)
    delete_user(session, firstname)

if __name__ == "__main__":
    main()
