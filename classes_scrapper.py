# import libraries
import os
import re
from datetime import datetime
from urllib.request import urlopen

import math
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(override=True)


####################################################


def visit_subject(term, subject):
    # interpolate term, subject, start_num, and end_num
    base_url = "https://wl11gp.neu.edu/udcprod8/bwckctlg.p_display_courses?term_in={0}&one_subj={1}&sel_crse_strt={2}&sel_crse_end={3}&sel_subj=&sel_levl=&sel_schd=&sel_coll=&sel_divs=&sel_dept=&sel_attr=".format(
        term, subject, "0000", "9999")
    # query the website and return the html to the variable 'page'
    page = urlopen(base_url)
    # parse the html using beautiful soup and store in variable 'soup'
    soup = BeautifulSoup(page, 'html.parser')
    # get the website body
    body = soup.body
    # find the content part of the website in the body (div with class 'pagebodydiv')
    content = body.find('div', attrs={'class': 'pagebodydiv'})
    # find the 'sections found' table on the content
    courses_table = content.find('table', attrs={
        'summary': 'This table lists all course detail for the selected term.'})

    if str(type(courses_table)) == "<class 'NoneType'>":
        print("No courses found for subject {1} during term {0}".format(
            term, subject))
        return []

    courses = courses_table.findAll("tr")
    processed_courses = []
    header = ""
    main = ""
    for part in courses:
        if header == "":
            header = process_header(part)
        else:
            main = process_main(part)
            processed_courses.append(make_course(header, main))
            header = ""
            main = ""
    return processed_courses


def remove_whitespace(s):
    return s.strip()


def cleanup(s):
    return re.sub('\n', '', s)


def find_index_of_part_containing(arr, targets):
    for i in range(len(arr)):
        for target in targets:
            if target in arr[i]:
                return i
    return -1


def process_header(header):
    header_text = header.td.a.text
    header_parts = header_text.split(" - ")

    course_name = header_parts[1]

    course_name = course_name.encode('ascii', 'ignore')
    course_name = course_name.decode('ascii')

    course_code_parts = header_parts[0].split(" ")

    course_subject_code = course_code_parts[0]

    course_number = course_code_parts[1]
    return {
        "course_dept": course_subject_code,
        "course_number": course_number,
        "course_name": course_name
    }


def process_main(main):
    main_contents = main.td.contents

    description = cleanup(main_contents[0])

    description = description.encode('ascii', 'ignore')
    description = description.decode('ascii')

    index_offset = find_index_of_part_containing(
        main_contents, ['Credit hours', 'Units']) - 2

    try:
        credit_hours = cleanup(main_contents[2 + index_offset])
        credit_hours = remove_whitespace(credit_hours)
        try:
            if credit_hours == "1.000 TO     4.000 Credit hours":
                credit_hours = 4
            else:
                credit_hours = int(math.ceil(float(credit_hours[0:3])))
        except:
            credit_hours = 4
    except:
        credit_hours = 0

    if find_index_of_part_containing(main_contents, ["Lecture", "Lab"]) == -1:
        index_offset = index_offset - 2

    class_level = remove_whitespace(cleanup(main_contents[10 + index_offset]))

    course_attributes = ""
    try:
        course_attributes = cleanup(main_contents[25 + index_offset])
    except:
        try:
            course_attributes = cleanup(main_contents[26 + index_offset])
        except:
            course_attributes = course_attributes
            # thats fineeeeee

    course_attributes = course_attributes[0:len(
        course_attributes) - 1]
    course_attributes = course_attributes.split(", ")
    return {
        "course_desc": description,
        "credits": credit_hours,
        "attributes": course_attributes,
        "class_level": class_level
    }


def make_course(header, main):
    return {**header, **main}


def connect_db():
    print('Trying to connect to database...')
    db_conn = get_db().connect()
    print('Connected to database.')
    return db_conn


def get_db():
    settings = {
        'userName': os.getenv("DB_USERNAME"),  # The name of the MySQL account to use (or empty for anonymous)
        'password': os.getenv("DB_PASSWORD"),  # The password for the MySQL account (or empty for anonymous)
        'serverName': os.getenv("DB_SERVER"),  # The name of the computer running MySQL
        'portNumber': os.getenv("DB_PORT"),  # The port of the MySQL server (default is 3306)
        'dbName': os.getenv("DB_NAME"),
        # The name of the database we are testing with (this default is installed with MySQL)
    }
    db_engine = create_engine(
        'mysql+mysqldb://{0[userName]}:{0[password]}@{0[serverName]}:{0[portNumber]}/{0[dbName]}'.format(settings))
    return db_engine


def create_classes(found_courses):
    print("PREPARING TO CREATE {0} CLASSES".format(len(found_courses)))

    for c in found_courses:
        db_conn = get_db().raw_connection()
        try:
            cursor = db_conn.cursor()
            cursor.callproc("create_class_procedure", [
                c["course_dept"], c["course_number"], c["class_level"], c["course_name"], c["course_desc"],
                c["credits"]])
            results = list(cursor.fetchall())
            cursor.close()
            db_conn.commit()
        except UnicodeEncodeError:
            print(c)
        except:
            continue
        finally:
            db_conn.close()
    print("OFFLOADED {0} CLASSES".format(len(found_courses)))


def run(term, startDept):
    print("RUNNING WEB CHECK (VERSION 10)")
    print("START TIME: %(timestamp)s" % {"timestamp": datetime.now()})
    print("------------------------------------------------------------------")

    db_conn = connect_db()

    get_all_departments = "SELECT * FROM department WHERE short_name >= '{0}'".format(
        startDept)

    depts = db_conn.execute(get_all_departments)
    found_courses = []
    total_proccessed = 0

    for dept in depts:
        print("Looking for courses on: {0} ({1})".format(
            dept["long_name"], dept["short_name"]))
        found_courses = found_courses + visit_subject(term, dept["short_name"])
        print("CURRENT: {0}; TOTAL: {1};".format(
            len(found_courses), total_proccessed))
        total_proccessed = total_proccessed + len(found_courses)
        create_classes(found_courses)
        found_courses = []
        print("CURRENT: {0}; TOTAL: {1};".format(
            len(found_courses), total_proccessed))

    print("------------------------------------------------------------------")
    print("END TIME: %(timestamp)s" % {"timestamp": datetime.now()})


def run_all(terms):
    for term in terms:
        print("RUNNING FOR TERM {0}".format(term))
        run(term, "AAAA")


run_all(["202010", "201940", "201960", "201950", "201930"])
