import os
import time
import json

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils import logging

from model.base import Base
from model.stg import Course
from model.stg import Session
from model.stg import StudentFollowSubject
from model.stg import Student
from model.stg import Subject
from model.stg import Subscription
from model.stg import University
from model.stg import get_stg_tables

from settings import STG_DATABASE_NAME


def fill_courses(engine, session):
    file_name = "./data/files/courses.json"
    logging.info("running fill %s", file_name)

    try:
        with open(file_name) as f:
            data = json.load(f)

            for line in data:
                session.add(Course(id=line["Id"], name=line["Name"]))
            session.commit()

    except Exception as e:
        logging.error(e)
        session.rollback()


def fill_sessions(engine, session):
    file_name = "./data/files/sessions.json"
    logging.info("running fill %s", file_name)

    try:
        with open(file_name) as f:
            data = json.load(f)

            for line in data:
                session.add(Session(student_id=line["StudentId"],
                                    session_start_time=datetime.strptime(line["SessionStartTime"], "%Y-%m-%d %H:%M:%S"),
                                    student_client=line["StudentClient"]))
            session.commit()

    except Exception as e:
        logging.error(e)
        session.rollback()


def fill_student_follow_subjects(engine, session):
    file_name = "./data/files/student_follow_subject.json"
    logging.info("running fill %s", file_name)

    try:
        with open(file_name) as f:
            data = json.load(f)

            for line in data:
                session.add(StudentFollowSubject(student_id=line["StudentId"],
                                                 subject_id=line["SubjectId"],
                                                 follow_date=datetime.strptime(line["FollowDate"], "%Y-%m-%d %H:%M:%S.%f")))
            session.commit()

    except Exception as e:
        logging.error(e)
        session.rollback()


def fill_students(engine, session):
    file_name = "./data/files/students.json"
    logging.info("running fill %s", file_name)

    try:
        with open(file_name) as f:
            data = json.load(f)

            for line in data:
                session.add(Student(id=line["Id"],
                                    registered_date=datetime.strptime(line["RegisteredDate"], "%Y-%m-%d %H:%M:%S.%f"),
                                    state=line.get("State", ""),
                                    city=line.get("City", ""),
                                    university_id=line["UniversityId"],
                                    course_id=line["CourseId"],
                                    signup_source=line["SignupSource"],
                                    student_client=line.get("StudentClient", "")))
            session.commit()

    except Exception as e:
        logging.error(e)
        session.rollback()


def fill_subjects(engine, session):
    file_name = "./data/files/subjects.json"
    logging.info("running fill %s", file_name)

    try:
        with open(file_name) as f:
            data = json.load(f)

            for line in data:
                session.add(Subject(id=line["Id"], name=line["Name"]))
            session.commit()

    except Exception as e:
        logging.error(e)
        session.rollback()


def fill_subscriptions(engine, session):
    file_name = "./data/files/subscriptions.json"
    logging.info("running fill %s", file_name)

    try:
        with open(file_name) as f:
            data = json.load(f)

            for line in data:
                session.add(Subscription(student_id=line["StudentId"],
                                         plan_type=line["PlanType"],
                                         payment_date=datetime.strptime(line["PaymentDate"], "%Y-%m-%d %H:%M:%S.%f")))
            session.commit()

    except Exception as e:
        logging.error(e)
        session.rollback()


def fill_universities(engine, session):
    file_name = "./data/files/universities.json"
    logging.info("running fill %s", file_name)

    try:
        with open(file_name) as f:
            data = json.load(f)

            for line in data:
                session.add(University(id=line["Id"], name=line["Name"]))
            session.commit()
    except Exception as e:
        logging.error(e)
        session.rollback()

def run():
    engine = create_engine("sqlite:///{}".format(STG_DATABASE_NAME))
    session = sessionmaker(bind=engine)()

    Base.metadata.drop_all(engine, tables=get_stg_tables())
    Base.metadata.create_all(engine, tables=get_stg_tables())

    start_time = time.process_time()
    logging.info(">>>starting %s", os.path.basename(__file__))

    fill_courses(engine, session)
    fill_sessions(engine, session)
    fill_student_follow_subjects(engine, session)
    fill_students(engine, session)
    fill_subjects(engine, session)
    fill_subscriptions(engine, session)
    fill_universities(engine, session)

    elapsed_time = time.process_time() - start_time
    logging.info(">>>finished %s, elapsed time: %0.2fs",
                 os.path.basename(__file__),
                 elapsed_time)

if __name__ == '__main__':
    run()