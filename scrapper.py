# import libraries
import smtplib
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

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
    course_code_parts = header_parts[0].split(" ")
    course_subject_code = course_code_parts[0]
    course_number = course_code_parts[1]
    return {
        "course_dept": course_subject_code,
        "course_number": course_number,
        "course_name": course_name
    }


def processMain(main):
    main_contents = main.td.contents

    description = cleanup(main_contents[0])

    credit_hours = cleanup(main_contents[2])
    if (credit_hours == "    1.000 TO     4.000 Credit hours"):
        credit_hours = 4
    else:
        credit_hours = int(credit_hours[4:5])

    class_level = cleanup(main_contents[10])

    course_attributes = ""
    try:
        course_attributes = cleanup(main_contents[25])
    except:
        try:
            course_attributes = cleanup(main_contents[26])
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


def run(term):
    print("RUNNING WEB CHECK (VERION 1)")
    print("START TIME: %(timestamp)s" % {"timestamp": datetime.now()})
    print("------------------------------------------------------------------")
    found_courses = visitSubject(term, "DS")
    for c in found_courses:
        procedure = """
            CALL create_class_procedure('{0}', {1}, '{2}', '{3}', '{4}', {5});
            """.format(
            c["course_dept"],    # {0} : class_dept
            c["course_number"],  # {1} : class_number
            c["class_level"],    # {2} : class_level
            c["course_name"],    # {3} : name
            c["course_desc"],    # {4} : description
            c["credits"])        # {5} : credit_hours

        print(procedure)
    print("------------------------------------------------------------------")
    print("END TIME: %(timestamp)s" % {"timestamp": datetime.now()})


run("202010")
