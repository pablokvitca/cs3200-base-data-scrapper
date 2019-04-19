# import libraries
import re
from datetime import datetime
from urllib.request import urlopen

from bs4 import BeautifulSoup
from pymysql import err
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy.engine import Engine


####################################################


class Course(object):
    c_subject: str = None
    c_number: int = None

    def __init__(self, c_subject, c_number):
        self.c_subject = c_subject
        self.c_number = int(c_number)

    def is_valid(self):
        return ((isinstance(self.c_subject, str) and (2 <= len(self.c_subject) <= 4))
                and (isinstance(self.c_number, int) and (0 <= self.c_number <= 9999)))

    def __str__(self):
        valid = "" if self.is_valid() else " !not valid!"
        return "[{} {}{}]".format(self.c_subject, self.c_number, valid)

    @staticmethod
    def is_empty():
        return False

    def write_to_db(self, eng: Engine, for_course, join_type, parent_group=-1, conn=None, cursor=None):
        try:
            cursor.callproc("create_prereq_for_class",
                            [self.c_subject,
                             self.c_number,
                             join_type,
                             parent_group])
        except err.IntegrityError as e:
            code = e.args[0]
            if int(code) == 1452:
                print("CLASS ({} {}) DID NOT EXIST. IGNORING.".format(self.c_subject, self.c_number), e)
            else:
                print("UNKNOWN ERROR", e)


class Prereq(object):

    def __init__(self):
        self.join_with = ""
        self.groups = []

    def add_class(self, c):
        try:
            while '<CLASS>' in c:
                c = re.sub(r'(.*?)<CLASS>', '', c, 1)
                c = re.sub(r'<CLASS>(.*?)', '', c, 1)
            c = c.split()
            c = Course(c[0], int(c[1]))
            if c.is_valid():
                self.add_group(c)
            else:
                print("WARNING: PROVIDED INVALID CLASS. IGNORING.", c)
        except IndexError as e:
            print("WARNING: TRIED CREATING CLASS BUT COULDN'T. IGNORING.", e, c)
        except ValueError as e:
            print("WARNING: TRIED CREATING CLASS BUT COULDN'T. IGNORING.", e, c)

    def add_group(self, g):
        if g not in self.groups:
            self.groups.append(g)

    def __str__(self):
        res = "("
        for i in range(len(self.groups)):
            g = self.groups[i]
            res += str(g)
            if i < len(self.groups) - 1:
                res += " {} ".format(str(self.join_with))
        res += ")"
        return res

    def is_empty(self):
        return len(self.groups) == 0

    def clean_empty_groups(self):
        self.groups = list(filter(lambda g: not g.is_empty(), self.groups))
        for group in self.groups:
            if isinstance(group, Prereq):
                group.clean_empty_groups()

        if len(self.groups) is 1 and isinstance(self.groups[0], Prereq):
            single_sub_group: Prereq = self.groups[0]
            self.groups = single_sub_group.groups
            self.join_with = single_sub_group.join_with

    def write_to_db(self, eng: Engine, for_course: Course, join_type, parent_group=-1, conn=None, cursor=None):
        p_group = parent_group
        if len(self.groups) > 0:
            if isinstance(cursor, type(None)):
                conn = eng.raw_connection()
                cursor = conn.cursor()

            try:
                cursor.callproc("create_group_prereq_for_class",
                                [for_course.c_subject if p_group == -1 else None,
                                 for_course.c_number if p_group == -1 else None,
                                 self.join_with if join_type is "" else join_type,
                                 p_group])
                for r in cursor.fetchall():
                    p_group = r[0]
                for group in self.groups:
                    group.write_to_db(eng, for_course, self.join_with, p_group, conn, cursor)
            except err.IntegrityError as e:
                code = e.args[0]
                if code == 1062:
                    print("IGNORING.")
                else:
                    conn.rollback()
                    print("ERROR CREATING PREREQS", e)
            finally:
                if parent_group is -1:
                    conn.commit()
                    cursor.close()
                    conn.close()


class Coreq(object):
    courses = []

    def __init__(self):
        self.courses = []

    def add_course(self, c):
        if c.is_valid() and c not in self.courses:
            self.courses.append(c)

    def __str__(self):
        res = "("
        for i in range(len(self.courses)):
            c = self.courses[i]
            res += str(c)
            if i < len(self.courses) - 1:
                res += " {} ".format("and")
        res += ")"
        return res

    def is_empty(self):
        return len(self.courses) == 0

    def clean_empty_courses(self):
        filter(lambda c: not c.is_empty(), self.courses)

    def write_to_db(self, eng: Engine, for_course: Course = None):
        conn = eng.raw_connection()
        cursor = conn.cursor()
        try:
            gid = -1
            for course in self.courses:
                cursor.callproc("create_coreq_for_class",
                                [for_course.c_subject,
                                 for_course.c_number,
                                 course.c_subject,
                                 course.c_number,
                                 gid])
                for r in cursor.fetchall():
                    gid = r[0]
        except exc.IntegrityError as e:
            print("ERROR CREATING COREQS", e)
        finally:
            conn.commit()
            cursor.close()
            conn.close()


class VisitCourseResult(object):
    corequisites = Coreq()
    prerequisites = Prereq()

    def __init__(self, corequisites=Coreq(), prerequisites=Prereq()):
        corequisites.clean_empty_courses()
        prerequisites.clean_empty_groups()
        self.corequisites = corequisites
        self.prerequisites = prerequisites


def visit_course(term, subject, class_number) -> VisitCourseResult:
    # interpolate term, subject, start_num, and end_num
    main_url = "https://wl11gp.neu.edu/udcprod8/bwckctlg.p_disp_course_detail"
    base_url = "{0}?cat_term_in={1}&subj_code_in={2}&crse_numb_in={3}".format(
        main_url, term, subject, class_number)
    # query the website and return the html to the variable 'page'
    page = urlopen(base_url)
    # parse the html using beautiful soup and store in variable 'soup'
    soup = BeautifulSoup(page, 'html.parser')
    # get the website body
    body = soup.body
    # find the content part of the website in the body (div with class 'pagebodydiv')
    content = body.find('div', attrs={'class': 'pagebodydiv'})
    # find the 'sections found' table on the content
    course_table = content.find('table', attrs={
        'summary': 'This table lists the course detail for the selected term.'})

    if str(type(course_table)) == "<class 'NoneType'>":
        print("No courses found for subject {1} {2} during term {0}".format(term, subject, class_number))
        return VisitCourseResult()

    course = course_table.findAll("tr")
    main = course[1]
    return VisitCourseResult(process_corequisites(main), process_prerequisites(main))


def process_corequisites(course):
    course_contents = course.contents[1].contents
    index = find_index_of_part_containing(course_contents, ["Corequisites"])

    if index == -1:  # break here if no coreqs found
        return Coreq()

    start_i = index + 4
    end_i = find_index_of_double_breaks(course_contents, start_i)
    coreqs = Coreq()
    for i in range(start_i, end_i, 2):
        coreq_str = str(course_contents[i])
        if ("\n" not in coreq_str) and ("<br/>" not in coreq_str):
            coreq_str = re.sub('\n', '', coreq_str)  # remove new lines
            coreq_str = re.sub('<br/>', '', coreq_str)  # remove html break tags
            coreq_str = re.sub('<.*?>', '', coreq_str)  # remove other html tags
            words = coreq_str.split()
            coreqs.add_course(Course(words[0], words[1]))
    return coreqs


def parse_prereq_nested_groups(expr):
    def _helper(iter):
        items = []
        for item in iter:
            if item == '(':
                result, closeparen = _helper(iter)
                if not closeparen:
                    raise ValueError("bad expression -- unbalanced parentheses")
                items.append(item)
                items.append(result)
            elif item == ')':
                items.append(item)
                return ''.join(items), True
            else:
                items.append(item)
        return items, False

    def re_join_joints(g_temp):
        g_res = []
        i_temp = None
        for i in g_temp:
            if len(i) > 1 or i is '(':
                if i_temp is not None:
                    g_res.append(i_temp)
                    i_temp = None
                g_res.append(i)
            else:
                if i_temp is not None:
                    i_temp += i
                else:
                    i_temp = i
        return g_res

    def re_join_group_openings(g_temp):
        g_res = []
        should_add_parens = False
        for i in g_temp:
            if i is '(':
                should_add_parens = True
            else:
                if should_add_parens:
                    i = '(' + i
                    should_add_parens = False
                g_res.append(i)
        return g_res

    if "(" in expr and ")" in expr:
        g_temp1 = _helper(iter(expr))[0]
        g_temp2 = re_join_joints(g_temp1)
        return re_join_group_openings(g_temp2)
    else:
        return [expr]


def parse_group(group: str) -> Prereq:
    prereqs = Prereq()
    nested_groups = parse_prereq_nested_groups(group)
    for g in nested_groups:
        g = g[1:]  # remove first character
        g = g[:-1]  # remove last character
        if (g == 'and') or (g == 'or'):
            prereqs.join_with = g
        else:
            if '(' in g and ')' in g:
                prereqs.add_group(parse_group(g))
            else:
                prereqs.add_group(process_plain_prereq_group(g))
    return prereqs


def process_prerequisites(course) -> Prereq:
    course_contents = course.contents[1].contents
    index = find_index_of_part_containing(course_contents, ["Prerequisites"])

    if index == -1:  # break here if no coreqs found
        return Prereq()

    start_i = index + 2
    end_i = find_index_of_double_breaks(course_contents, start_i)
    prereqs_str = ""
    for i in range(start_i, end_i):
        prereqs_str = prereqs_str + str(course_contents[i])
    prereqs_str = std_prereq_str(prereqs_str)

    return parse_group("(" + prereqs_str + ")")


def process_plain_prereq_group(ppg):
    prereqs = Prereq()
    classes = []
    if ("and" in ppg) and ("or" not in ppg):
        classes = ppg.split(" and ")
        prereqs.join_with = "and"
    elif ("and" not in ppg) and ("or" in ppg):
        classes = ppg.split(" or ")
        prereqs.join_with = "or"
    elif ("and" not in ppg) and ("or" not in ppg):
        prereqs.add_class(ppg)

    if ("and" in ppg) and ("or" in ppg):
        pass  # TODO: THROW ERROR, NON PLAIN GROUP
    else:
        for c in classes:
            prereqs.add_class(c)
    return prereqs


def std_prereq_str(prereqs_str):
    prereqs_str = re.sub('\n', '', prereqs_str)  # remove new lines
    prereqs_str = re.sub('<br/>', '', prereqs_str)  # remove html break tags
    prereqs_str = re.sub('<.*?>', '<CLASS>', prereqs_str)  # remove other html tags
    prereqs_str = re.sub('[(]', '( ', prereqs_str)
    prereqs_str = re.sub('[)]', ' )', prereqs_str)

    valid_strs = ["(", ")", "<CLASS>", "and", "or"]
    res = []
    for s in prereqs_str.split():
        if contains_valid(s, valid_strs):
            res.append(s)
    open_parens = True
    for i in range(len(res)):
        if '<CLASS>' in res[i]:
            if open_parens:
                res[i] = "(" + res[i]
            else:
                res[i] = res[i] + ")"
            open_parens = not open_parens
    return " ".join(res)


def contains_valid(s, valid_strs):
    for valid in valid_strs:
        if valid in s:
            return True


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


def add_coreqs(coreqs, eng, for_course: Course):
    print("CREATING COREQS FOR COURSE: {} {}".format(for_course.c_subject, for_course.c_number))
    coreqs.write_to_db(eng, for_course)
    print("DONE!")


def add_prereqs(prereqs, eng, for_course: Course):
    print("CREATING PREREQS FOR COURSE: {} {}".format(for_course.c_subject, for_course.c_number))
    prereqs.write_to_db(eng, for_course, "")
    print("DONE!")


def add_requisites(reqs, for_course: Course):
    eng: Engine = get_db()
    try:
        add_prereqs(reqs.prerequisites, eng, for_course)
    except:
        pass
    try:
        add_coreqs(reqs.corequisites, eng, for_course)
    except:
        pass


def connect_db():
    print('Trying to connect to database...')
    db_conn = get_db().connect()
    print('Connected to database.')
    return db_conn


def get_db() -> Engine:
    settings = {
        'userName': "root",  # The name of the MySQL account to use (or empty for anonymous)
        'password': "rycbar12345",  # The password for the MySQL account (or empty for anonymous)
        'serverName': "127.0.0.1",  # The name of the computer running MySQL
        'portNumber': 3306,  # The port of the MySQL server (default is 3306)
        'dbName': "projectcs3200",
        # The name of the database we are testing with (this default is installed with MySQL)
    }
    db_engine: Engine = create_engine(
        'mysql+pymysql://{0[userName]}:{0[password]}@{0[serverName]}:{0[portNumber]}/{0[dbName]}'.format(settings))
    return db_engine


def process_course(term, course):
    print("Requisites for {0} {1}".format(course["class_dept"], course["class_number"]))
    reqs = visit_course(term, course["class_dept"], course["class_number"])
    add_requisites(reqs, Course(course["class_dept"], course["class_number"]))


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


run_all(["202010", "201940", "201960", "201950", "201930"])
