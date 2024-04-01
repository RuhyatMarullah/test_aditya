from datetime import datetime

import psycopg2
from flask import Flask, jsonify, request
from psycopg2 import sql

app = Flask(__name__)


# Fungsi untuk membuat koneksi ke database
def create_db_connection():
    DB_HOST = 'localhost'
    DB_NAME = 'test_aditya'
    DB_USER = 'ruhyat'
    DB_PASSWORD = 'ruhyat'
    
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    return conn

def hitung_pph21(gaji_bulanan):
    ptkp = 54000000  

    gaji_tahunan = gaji_bulanan * 12
    pkp = gaji_tahunan - ptkp

    if pkp <= 50000000:
        pph21 = pkp * 0.05
    elif pkp <= 250000000:
        pph21 = 2500000 + (pkp - 50000000) * 0.15
    elif pkp <= 500000000:
        pph21 = 32500000 + (pkp - 250000000) * 0.25
    else:
        pph21 = 95000000 + (pkp - 500000000) * 0.30

    return pph21/12

GENDER = {
    "M":"Male",
    "F":"Female"
}


def get_last_emp_no():
    conn = create_db_connection()
    cur = conn.cursor()

    sql_query = "SELECT MAX(emp_no) FROM employees;"
    cur.execute(sql_query)
    last_emp_no = cur.fetchone()[0]

    cur.close()
    conn.close()

    return last_emp_no

# Fungsi untuk memasukkan data karyawan ke database
def insert_employee_to_db(birth_date, first_name, last_name, gender):
    conn = create_db_connection()
    cur = conn.cursor()

    hire_date = datetime.now().date()

    last_emp_no = get_last_emp_no()
    new_emp_no = last_emp_no + 1 if last_emp_no is not None else 1

    sql_query = """
        INSERT INTO employees (emp_no,birth_date, first_name, last_name, gender, hire_date) 
        VALUES (%s,%s, %s, %s, %s, %s) RETURNING emp_no;
    """
    
    cur.execute(sql_query, (new_emp_no,birth_date, first_name, last_name, gender, hire_date))
    emp_no = cur.fetchone()[0]
    
    conn.commit()
    cur.close()
    conn.close()

    return emp_no

# Fungsi untuk memasukkan data karyawan ke database
def insert_slary_to_db(emp_no, salary, from_date, to_date):
    conn = create_db_connection()
    cur = conn.cursor()

    sql_query = """
        INSERT INTO salaries (emp_no,salary, from_date, to_date) 
        VALUES (%s,%s, %s, %s) RETURNING emp_no;
    """
    
    cur.execute(sql_query, (emp_no, salary, from_date, to_date))
    emp_no = cur.fetchone()[0]
    
    conn.commit()
    cur.close()
    conn.close()

    return emp_no


@app.route('/api/employees', methods=['GET'])
def get_employees():
    conn = create_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM employees;")
    query = cur.fetchall()
    data = []
    for row in query:
        data.append(
        {
            'emp_no':row[0],
            'birth_date':row[1],
            'first_name':row[2],
            'last_name':row[3],
            'gender':GENDER[row[4]],
            'hire_date':row[5]
        }
        )

    return jsonify(data)

@app.route('/api/employees/<int:id>', methods=['GET'])
def get_employee(id):
    conn = create_db_connection()
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT * FROM employees WHERE emp_no = %s;"), [id])
    query = cur.fetchone()
    if query is None:
        return jsonify({'error': 'Not Found'}), 404
    
    data = {
        'emp_no':query[0],
        'birth_date':query[1],
        'first_name':query[2],
        'last_name':query[3],
        'gender':GENDER[query[4]],
        'hire_date':query[5]
    }
    return jsonify(data)

@app.route('/api/employees', methods=['POST'])
def create_employee():
    data = request.json
    
    birth_date = data.get('birth_date')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    gender = data.get('gender')

    if not (birth_date and first_name and last_name and gender):
        return jsonify({'error': 'Data tidak lengkap'}), 400
    emp_no = insert_employee_to_db(birth_date, first_name, last_name, gender)

    return jsonify({'emp_no': emp_no}), 201

@app.route('/api/salaries', methods=['GET'])
def get_salaries():
    conn = create_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT s.salary,s.from_date,s.to_date,e.first_name || ' ' || e.last_name AS employee_name FROM salaries AS s join employees AS e ON s.emp_no=e.emp_no;")
    query = cur.fetchall()
    data = []
    for row in query:
        data.append(
            {
                "salary":row[0],
                "pph21":hitung_pph21(row[0]),
                "name":row[3]
            }
        )
    return jsonify({'tasks': data})

@app.route('/api/salaries/<int:id>', methods=['GET'])
def get_salary(id):
    conn = create_db_connection()
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT s.salary,s.from_date,s.to_date,e.first_name || ' ' || e.last_name AS employee_name FROM salaries AS s join employees AS e ON s.emp_no=e.emp_no WHERE s.emp_no=%s;"),[id])
    query = cur.fetchone()
    data = {
                "salary":query[0],
                "pph21":hitung_pph21(query[0]),
                "name":query[3]
            }
    return jsonify(data)

@app.route('/api/salaries', methods=['POST'])
def create_salary():
    data = request.json
    emp_no = data.get('emp_no')
    from_date = data.get('from_date')
    to_date = data.get('to_date')
    salary = data.get('salary')


    emp_no = insert_slary_to_db(emp_no,salary , from_date,to_date)

    return jsonify({'emp_no': emp_no}), 201

if __name__ == '__main__':
    app.run(debug=True)
