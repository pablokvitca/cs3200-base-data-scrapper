# import libraries
import os
import re
from datetime import datetime
from urllib.request import urlopen

from bs4 import BeautifulSoup, NavigableString, Tag
from dotenv import load_dotenv
from pymysql import err
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv(override=True)


####################################################

class SectionRestriction(object):
    crn: int = None
    r_type: str = None

    def __init__(self, crn: int, r_type: str):
        self.crn = crn
        self.r_type = r_type

    def write_to_db(self, cursor):
        cursor.callproc("create_class_restriction",
                        [self.crn,
                         self.r_type])


class MTTime(object):
    hour: int
    min: int
    ampm: str

    def __init__(self, hour: int, min: int, ampm: str):
        self.hour = hour
        self.min = min
        self.ampm = ampm

    def __str__(self):
        return "{0}:{1} {2}".format(self.hour, self.min, self.ampm)

    def to_sql(self):
        def pad_zeros(s):
            s = str(s)
            return "0" + s if len(s) == 1 else s

        h = self.hour
        h = h if self.ampm == "am" else h + 12
        m = self.min
        return "{0}:{1}:00".format(pad_zeros(h), pad_zeros(m))


class MeetingTime(object):
    crn: int = None
    start_time: MTTime = None
    end_time: MTTime = None
    meeting_days: [bool] = [False, False, False, False, False, False, False]

    def __init__(self, crn: int, start_time: MTTime, end_time: MTTime):
        self.crn = crn
        self.start_time = start_time
        self.end_time = end_time
        self.meeting_days = [False, False, False, False, False, False, False]

    def add_day(self, day):
        day_to_pos = {
            "M": 0,
            "T": 1,
            "W": 2,
            "R": 3,
            "F": 4,
            "S": 5,
            "U": 6
        }
        self.meeting_days[day_to_pos[day]] = True

    def meets_on_day(self, d):
        day_to_pos = {
            "M": 0,
            "T": 1,
            "W": 2,
            "R": 3,
            "F": 4,
            "S": 5,
            "U": 6
        }
        return self.meeting_days[day_to_pos[d]]

    def meeting_days_sql(self):
        m = str(1 if self.meets_on_day("M") else 0)
        t = str(1 if self.meets_on_day("T") else 0)
        w = str(1 if self.meets_on_day("W") else 0)
        r = str(1 if self.meets_on_day("R") else 0)
        f = str(1 if self.meets_on_day("F") else 0)
        s = str(1 if self.meets_on_day("S") else 0)
        u = str(1 if self.meets_on_day("U") else 0)
        return m + t + w + r + f + s + u

    def write_to_db(self, cursor):
        cursor.callproc("create_class_mt_times",
                        [self.crn,
                         self.start_time.to_sql(),
                         self.end_time.to_sql(),
                         self.meeting_days_sql()])

    @staticmethod
    def make_meeting_time(crn, time, days):
        time_parts = time.split(" - ")
        start_time_parts = time_parts[0].split()
        start_time_time_parts = start_time_parts[0].split(":")
        start_time = MTTime(int(start_time_time_parts[0]), int(start_time_time_parts[1]), start_time_parts[1])

        end_time_parts = time_parts[1].split()
        end_time_time_parts = end_time_parts[0].split(":")
        end_time = MTTime(int(end_time_time_parts[0]), int(end_time_time_parts[1]), end_time_parts[1])
        mt = MeetingTime(crn, start_time, end_time)
        for d in days:
            mt.add_day(d)
        return mt

    def __str__(self):
        return "[{0}{1}{2}{3}{4}{5}{6} at {7} - {8}]".format(
            "M" if self.meeting_days[0] else "",
            "T" if self.meeting_days[1] else "",
            "W" if self.meeting_days[2] else "",
            "R" if self.meeting_days[3] else "",
            "F" if self.meeting_days[4] else "",
            "S" if self.meeting_days[5] else "",
            "U" if self.meeting_days[6] else "",
            str(self.start_time),
            str(self.end_time)
        )


class Section(object):
    crn: int = None
    class_dept: str = None
    class_number: int = None
    professor: str = None
    capacity: int = None
    registered: int = None
    semester_id: int = None
    meeting_times = []
    restrictions = []

    def __init__(self, crn: int, class_dept: str, class_number: int, semester_id: int):
        self.crn: int = crn
        self.class_dept: str = class_dept
        self.class_number: int = class_number
        self.semester_id: int = semester_id
        self.meeting_times = []
        self.meeting_times = []

    def add_professor(self, professor: str):
        self.professor = professor

    def add_capacity(self, capacity: int):
        self.capacity = capacity

    def add_registered(self, registered: int):
        self.registered = registered

    def add_meeting_time(self, meeting_time: MeetingTime):
        self.meeting_times.append(meeting_time)

    def add_restriction(self, restriction: SectionRestriction):
        self.restrictions.append(restriction)

    def __str__(self):
        return "[ CRN: {} ({} {}) ]".format(self.crn, self.class_dept, self.class_number)

    def write_to_db(self, eng: Engine):
        conn = eng.raw_connection()
        cursor = conn.cursor()

        try:
            cursor.callproc("create_class_section",
                            [self.crn,
                             self.class_dept,
                             self.class_number,
                             self.professor,
                             self.capacity,
                             self.registered,
                             self.semester_id])
            for meeting_time in self.meeting_times:
                meeting_time.write_to_db(cursor)
            for restriction in self.restrictions:
                restriction.write_to_db(cursor)
        except err.IntegrityError as e:
            code = e.args[0]
            if code == 1062:
                print("IGNORING.")
            else:
                conn.rollback()
                print("ERROR CREATING SECTION", e)
        except err.InternalError as e:
            conn.rollback()
            print("ERROR CREATING SECTION", e)
        finally:
            conn.commit()
            cursor.close()
            conn.close()


def visit_course_sections(term, subject, class_number, schedule_type="LEC") -> [Section]:
    # interpolate term, subject, start_num, and end_num
    "?term_in=202010&subj_in=EEMB&crse_in=3465&schd_in=LEC"
    main_url = "https://wl11gp.neu.edu/udcprod8/bwckctlg.p_disp_listcrse"
    base_url = "{0}?term_in={1}&subj_in={2}&crse_in={3}&schd_in={4}".format(
        main_url, term, subject, class_number, schedule_type)
    # query the website and return the html to the variable 'page'
    page = urlopen(base_url)
    # parse the html using beautiful soup and store in variable 'soup'
    soup = BeautifulSoup(page, 'html.parser')
    # get the website body
    body = soup.body
    # find the content part of the website in the body (div with class 'pagebodydiv')
    content = body.find('div', attrs={'class': 'pagebodydiv'})
    # find the 'sections found' table on the content
    sections_table = content.find('table', attrs={
        'summary': 'This layout table is used to present the sections found'})

    if str(type(sections_table)) == "<class 'NoneType'>":
        print("No courses found for subject {1} {2} during term {0}".format(term, subject, class_number))
        if schedule_type == "LEC":
            # RETRYING AS LAB
            return visit_course_sections(term, subject, class_number, schedule_type="LAB")
        elif schedule_type == "LAB":
            # RETRYING AS SEMINAR
            return visit_course_sections(term, subject, class_number, schedule_type="SEM")
        else:
            return []

    rows = sections_table.findAll('tr')
    sections = []
    for i in range(0, len(rows) - 1):
        header_table = rows[i]
        main_table = rows[i + 1]
        section = process_section(header_table, main_table, term)
        if not isinstance(section, type(None)):
            sections.append(section)
    return sections


def clear_empty_conts(l):
    def f(i):
        return not isinstance(i, NavigableString)

    return list(filter(f, l))


def process_section(header_table, main_table, semester_id):
    if isinstance(header_table.contents[1].contents[0], Tag):
        header_table = header_table.contents[1].contents[0].text
        header_parts = header_table.split(" - ")

        crn = header_parts[len(header_parts) - 3]

        course_data = header_parts[len(header_parts) - 2].split()
        class_dept = course_data[0]
        class_number = course_data[1]

        section = Section(crn, class_dept, class_number, semester_id)

        professor = "TBA"
        capacity = 0
        registered = 0
        mt_times = main_table.findAll('tr')
        for row_i in range(1, len(mt_times)):
            row_cont = clear_empty_conts(mt_times[row_i].contents)
            ctype = row_cont[0].text
            if ctype == "Class":
                professor = re.sub('<.*?>', '', str(row_cont[6]))
                time_range = str(row_cont[1].text)
                days = str(row_cont[2].text)
                capacity = int(str(row_cont[7].text))
                registered = int(str(row_cont[7].text))
                if time_range != "TBA":
                    section.add_meeting_time(MeetingTime.make_meeting_time(crn, time_range, days))

        section.add_professor(professor)
        section.add_capacity(capacity)
        section.add_registered(registered)

        return section
    else:
        return None


def remove_whitespace(s):
    return s.strip()


def cleanup(s):
    return re.sub('\n', '', s)


def find_index_of_part_containing(arr, targets):
    for i in range(len(arr)):
        for target in targets:
            if target in str(arr[i]):
                return i
    return -1


def find_index_of_double_breaks(arr, start=0):
    for i in range(start, len(arr)):
        break_tag = '<br/>'
        cur = str(arr[i])
        prev = str(arr[i - 2])
        if (i > 1) and (break_tag in cur) and (break_tag in prev):
            return i - 1
    return len(arr)


def connect_db():
    print('Trying to connect to database...')
    db_conn = get_db().connect()
    print('Connected to database.')
    return db_conn


def get_db() -> Engine:
    settings = {
        'userName': os.getenv("DB_USERNAME"),  # The name of the MySQL account to use (or empty for anonymous)
        'password': os.getenv("DB_PASSWORD"),  # The password for the MySQL account (or empty for anonymous)
        'serverName': os.getenv("DB_SERVER"),  # The name of the computer running MySQL
        'portNumber': os.getenv("DB_PORT"),  # The port of the MySQL server (default is 3306)
        'dbName': os.getenv("DB_NAME"),
        # The name of the database we are testing with (this default is installed with MySQL)
    }
    db_engine: Engine = create_engine(
        'mysql+pymysql://{0[userName]}:{0[password]}@{0[serverName]}:{0[portNumber]}/{0[dbName]}'.format(settings))
    return db_engine


def process_course(term, course):
    print("SECTIONS for {0} {1}".format(course["class_dept"], course["class_number"]))
    sections = visit_course_sections(term, course["class_dept"], course["class_number"])
    for section in sections:
        add_section(section)


def add_section(section: Section):
    section.write_to_db(get_db())


def run(term, start_at=0, do_one=False):
    print("RUNNING WEB CHECK (VERSION 10)")
    print("START TIME: %(timestamp)s" % {"timestamp": datetime.now()})
    print("------------------------------------------------------------------")

    db_conn = connect_db()

    get_all_courses = "SELECT class_dept, class_number FROM class;"
    courses = db_conn.execute(get_all_courses)
    cnt = 0
    for course in courses:
        if cnt >= start_at:
            process_course(term, course)
            print("PROCESSED", cnt)
        else:
            print("SKIPPED", cnt)
        cnt += 1

    print("------------------------------------------------------------------")
    print("END TIME: %(timestamp)s" % {"timestamp": datetime.now()})


def run_all(terms):
    for term in terms:
        print("RUNNING FOR TERM {0}".format(term))
        run(term)


run("202010")
run("201940")
run("201960")
run("201950")
run("201930")
