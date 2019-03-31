# import libraries
import smtplib
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import math
import sqlalchemy
from sqlalchemy import create_engine, types, exc

####################################################


def visitSubject(term, subject):
    # interpolate term, subject, start_num, and end_num
    BASE_URL = """
        https://wl11gp.neu.edu/udcprod8/bwckctlg.p_display_courses?term_in={0}&one_subj={1}&sel_crse_strt={2}&sel_crse_end={3}&sel_subj=&sel_levl=&sel_schd=&sel_coll=&sel_divs=&sel_dept=&sel_attr="
        """.format(term, subject, "0000", "9999")
    # query the website and return the html to the variable 'page'
    page = urlopen(BASE_URL)
    # parse the html using beautiful soup and store in variable 'soup'
    soup = BeautifulSoup(page, 'html.parser')
    # get the website body
    body = soup.body
    # find the content part of the website in the body (div with class 'pagebodydiv')
    content = body.find('div', attrs={'class': 'pagebodydiv'})
    # find the 'sections found' table on the content
    courses_table = content.find('table', attrs={
        'summary': 'This table lists all course detail for the selected term.'})

    if (str(type(courses_table)) == "<class 'NoneType'>"):
        print("No courses found for subject {1} during term {0}".format(
            term, subject))
        return []

    courses = courses_table.findAll("tr")
    processed_courses = []
    header = ""
    main = ""
    for part in courses:
        if (header == ""):
            header = processHeader(part)
        else:
            main = processMain(part)
            processed_courses.append(makeCourse(header, main))
            header = ""
            main = ""
    return processed_courses


def processHeader(header):
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


def remove_whitespace(s):
    return s.strip()


def processMain(main):
    main_contents = main.td.contents

    description = cleanup(main_contents[0])

    description = description.encode('ascii', 'ignore')
    description = description.decode('ascii')

    index_offset = 0
    if "Coreq" in main_contents[1].text:
        index_offset = 2

    credit_hours = cleanup(main_contents[2 + index_offset])
    credit_hours = remove_whitespace(credit_hours)

    if (credit_hours == "1.000 TO     4.000 Credit hours"):
        credit_hours = 4
    else:
        credit_hours = int(math.ceil(float(credit_hours[0:3])))

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


def cleanup(s):
    return re.sub('\n', '', s)


def makeCourse(header, main):
    return {**header, **main}


def connect_DB():
    print('Trying to connect to database...')
    db_conn = get_DB().connect()
    print('Connected to database.')
    return db_conn


def get_DB():
    settings = {
        # The name of the MySQL account to use (or empty for anonymous)
        'userName': "root",
        # The password for the MySQL account (or empty for anonymous)
        'password': "rycbar12345",
        'serverName': "127.0.0.1",    # The name of the computer running MySQL
        # The port of the MySQL server (default is 3306)
        'portNumber': 3306,
        # The name of the database we are testing with (this default is installed with MySQL)
        'dbName': "projectcs3200",
    }
    db_engine = create_engine(
        'mysql+mysqldb://{0[userName]}:{0[password]}@{0[serverName]}:{0[portNumber]}/{0[dbName]}'.format(settings))
    return db_engine


def run(term):
    print("RUNNING WEB CHECK (VERSION 4)")
    print("START TIME: %(timestamp)s" % {"timestamp": datetime.now()})
    print("------------------------------------------------------------------")

    db_conn = connect_DB()

    get_all_departments = "SELECT * FROM department"

    # depts = db_conn.execute(get_all_departments)
    depts = [{"long_name": 'Computer Sciences',
              "short_name": "CS", "college_id": 5}]
    found_courses = []
    prev_l = 0

    for dept in depts:
        print("-------")
        print("Looking for courses on: {0} ({1})".format(
            dept["long_name"], dept["short_name"]))
        found_courses = found_courses + visitSubject(term, dept["short_name"])
        print("Found {0} courses of {1} so far".format(
            len(found_courses) - prev_l, len(found_courses)))
        prev_l = len(found_courses)
        print("-------")

    db_conn.execute('SET NAMES utf8;')
    db_conn.execute('SET CHARACTER SET utf8;')
    db_conn.execute('SET character_set_connection=utf8;')

    for c in found_courses:
        db_conn = get_DB().raw_connection()
        try:
            cursor = db_conn.cursor()
            cursor.callproc("create_class_procedure", [
                            c["course_dept"], c["course_number"], c["class_level"], c["course_name"], c["course_desc"], c["credits"]])
            results = list(cursor.fetchall())
            cursor.close()
            db_conn.commit()
        except UnicodeEncodeError:
            print(c)
        except exc.IntegrityError:
            continue
        finally:
            db_conn.close()
    print("------------------------------------------------------------------")
    print("END TIME: %(timestamp)s" % {"timestamp": datetime.now()})


run("202010")
