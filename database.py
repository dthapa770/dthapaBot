# database.py
# Handles all the methods interacting with the database of the application.
# Students must implement their own methods here to meet the project requirements.

import os
import pymysql.cursors

db_host = os.environ['DB_HOST']
db_username = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']


def connect():
    try:
        conn = pymysql.connect(host=db_host,
                               port=3306,
                               user=db_username,
                               password=db_password,
                               db=db_name,
                               charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)
        print("Bot connected to database {}".format(db_name))
        return conn
    except:
        print("Bot failed to create a connection with your database because your secret environment variables " +
              "(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME) are not set".format(db_name))
        print("\n")
# your code here

def response_message(msg): 
  # check for null msg
      response = None
      db_conn = connect()
      if db_conn:
        parse_msg = msg.split("|");
       
        #business problem 1
        if "stock" == parse_msg[0]: 
            response = book_stock(parse_msg[1],db_conn)

        #business problem 2
        elif "comment" == parse_msg[0]:
            response = book_comment(parse_msg[1],parse_msg[2],db_conn)

        #business problem 3
        elif "penalty" == parse_msg[0]:
          response= book_penalty_rent(parse_msg[1], parse_msg[2],db_conn) 
          
        #business problem 4
        elif "account expiration date" == parse_msg[0]:
          response= user_expdate (db_conn)

        #business problem 5
        elif "reviews" == parse_msg[0]:
          response= reviews_range(parse_msg[1], parse_msg[2],db_conn)

        #business problem 6
        elif "reservation" == parse_msg[0]:
          response= reservation_range(parse_msg[1], parse_msg[2],parse_msg[3],db_conn)

        #business problem extra
        elif "employee" == parse_msg[0]:
          response = employee_salary(parse_msg[1],parse_msg[2],db_conn)
        #business problem 8
        else:
          print("not recognized")
        db_conn.close()
      return response

def book_stock(title,db_conn):
  cursor = db_conn.cursor();
  stock_query = """SELECT book_stock FROM book WHERE book.book_title=%s"""
  cursor.execute(stock_query,(title,))
  db_conn.commit();
  result = cursor.fetchone()
  for key,values in result.items():
    return values

  

def book_comment (title,comment, db_conn):

  cursor = db_conn.cursor();
  select_query = """SELECT book_ISBN  FROM book WHERE book_title =%s"""
  cursor.execute(select_query,(title,))
  db_conn.commit();
  result = cursor.fetchone();
  book_id = result["book_ISBN"]
  int(book_id)

  comment_query = """INSERT INTO Reviews (review_comments, book_id) VALUES (%s, %s)"""
  cursor.execute(comment_query,(comment,book_id,))
  db_conn.commit();

  display_query = """SELECT review_comments FROM Reviews where book_id =%s"""
  cursor.execute(display_query,(book_id,))
  result_display = cursor.fetchone()
  db_conn.commit();
  for key,values in result_display.items():
    return "Your comment '"+ values+" '- saved in Database"
  

def book_penalty_rent(state,book_genre, db_conn):
  
  cursor = db_conn.cursor();
  rent_query = """SELECT DISTINCT user.user_name AS "Username", account.account_penalty AS "penalty", book.book_genre AS "genre", Address.state AS "state" FROM user, account, book, Address 
  WHERE user.user_id = account.account_id AND book.book_genre = %s AND Address.state = %s;"""
  cursor.execute(rent_query,(book_genre,state))
  db_conn.commit()
  result = cursor.fetchall()
  return result
        

def user_expdate(db_conn):
  cursor = db_conn.cursor();
  count_query = """ SELECT DISTINCT user.user_name AS "User", account.account_expdate AS "Expiration Date" FROM user, account 
  WHERE user.user_id = account.user_id 
  HAVING DATE(account.account_expdate) < curdate()"""
  cursor.execute(count_query)
  db_conn.commit()
  result = cursor.fetchall()
  return result


def reviews_range(start_date,end_date,db_conn):
  cursor = db_conn.cursor();
  reviews_date_query = """SELECT book.book_title, Reviews.review_timestamp AS "TIME" FROM book, Reviews 
  WHERE book.book_ISBN = Reviews.book_id 
  HAVING date(Reviews.review_timestamp) BETWEEN date(%s) AND date(%s)"""
  cursor.execute(reviews_date_query,(start_date,end_date))
  db_conn.commit()
  result = cursor.fetchall()
  return result


def reservation_range(state, start_Date,end_date,db_conn):
  cursor = db_conn.cursor();
  count_query = """SELECT DISTINCT user.user_name AS "User Name",  Address.state AS "State", book.book_title AS "Book Title", reservation.reservation_startdate AS "reservation start", reservation.reservation_enddate AS "reservation end" 
  FROM user,Address,book,reservation 
  JOIN book b1 ON b1.book_ISBN = reservation.book_isbn
  WHERE Address.state = %s
  HAVING reservation.reservation_startdate BETWEEN %s AND %s"""
  cursor.execute(count_query,(state,start_Date,end_date))
  db_conn.commit()
  result = cursor.fetchall()
  return result

def employee_salary(dept_name,salary,db_conn):
  cursor = db_conn.cursor();
  int(salary)
  count_query = """ SELECT DISTINCT employee.employee_name AS "Employee", department.department_name AS "Department", payroll.employee_salary 
  FROM employee, department, payroll 
  WHERE employee.department_id = department.department_id
  AND employee.employee_id = payroll.employee_id
  AND department.department_name = %s
  HAVING payroll.employee_salary> %s """
  cursor.execute(count_query,(dept_name,salary,))
  db_conn.commit()
  result = cursor.fetchone()
  return result




