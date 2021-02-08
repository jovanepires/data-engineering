import pandas as pd
import math
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exists

from utils import logging

from model.base import Base
from model.stg import Course
from model.stg import Student
from model.stg import Session
from model.stg import Subscription
from model.stg import StudentFollowSubject
from model.stg import Subject
from model.stg import University

from model.dw import get_dw_tables
from model.dw import DimCourse
from model.dw import DimAddress
from model.dw import DimTime
from model.dw import DimStudentFollowSubject
from model.dw import DimSubject
from model.dw import DimUniversity
from model.dw import DimPlanType
from model.dw import FactLoggedStudent
from model.dw import FactRegisteredStudent
from model.dw import FactSubscribedStudent
from settings import STG_DATABASE_NAME, DW_DATABASE_NAME


def fill_dim_address(dw_session, stg_session):
    try:
        logging.info("running fill dim address")

        for item in stg_session.query(Student).all():
            data = dict(city=item.city, state=item.state)
            is_update = dw_session.query(
                dw_session.query(DimAddress)\
                          .filter(DimAddress.city==item.city, 
                                  DimAddress.state==item.state)\
                          .exists()
            ).scalar()

            if not is_update:
                logging.debug("inserting address %s/%s", item.city, item.state)
                dw_session.add(DimAddress(**data))
            else:
                pass

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_courses(dw_session, stg_session):
    try:
        total = stg_session.query(Course).count()
        logging.info("running fill dim courses %s items", total)

        for course in stg_session.query(Course).all():
            data = dict(course_id=course.id, name=course.name)
            is_update = dw_session.query(
                exists().where(DimCourse.course_id==course.id)
            ).scalar()

            if not is_update:
                logging.debug("inserting course %s", course.id)
                dw_session.add(DimCourse(**data))

            else:
                logging.debug("updating course %s", course.id)
                curr = dw_session.query(DimCourse).filter_by(course_id=course.id).first()
                curr.name = course.name

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_student_follow_subject(dw_session, stg_session):
    try:
        logging.info("running fill dim_student_follow_subject")

        for item in stg_session.query(StudentFollowSubject).all():
            is_update = dw_session.query(
                dw_session.query(DimStudentFollowSubject)\
                          .filter(DimStudentFollowSubject.student_id==item.student_id, 
                                  DimStudentFollowSubject.subject_id==item.subject_id)\
                          .exists()
            ).scalar()

            if not is_update:
                date = item.follow_date.date()
                time_id = lookup_time(dw_session, date)
                logging.debug("inserting dim time %s/%s", date, time_id)
                dw_session.add(DimStudentFollowSubject(student_id=item.student_id,
                                                       subject_id=item.subject_id,
                                                       follow_date_id=time_id))
            else:
                pass

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_subject(dw_session, stg_session):
    try:
        total = stg_session.query(Subject).count()
        logging.info("running fill dim subjects %s items", total)

        for item in stg_session.query(Subject).all():
            data = dict(subject_id=item.id, name=item.name)
            is_update = dw_session.query(
                exists().where(DimSubject.subject_id==item.id)
            ).scalar()

            if not is_update:
                logging.debug("inserting subject %s", item.id)
                dw_session.add(DimSubject(**data))

            else:
                logging.debug("updating subject %s", item.id)
                curr = dw_session.query(DimSubject).filter_by(subject_id=item.id).first()
                curr.name = item.name

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_university(dw_session, stg_session):
    try:
        total = stg_session.query(Subject).count()
        logging.info("running fill dim universities %s items", total)

        for item in stg_session.query(University).all():
            data = dict(university_id=item.id, name=item.name)
            is_update = dw_session.query(
                exists().where(DimUniversity.university_id==item.id)
            ).scalar()

            if not is_update:
                logging.debug("inserting subject %s", item.id)
                dw_session.add(DimUniversity(**data))

            else:
                logging.debug("updating subject %s", item.id)
                curr = dw_session.query(DimUniversity).filter_by(university_id=item.id).first()
                curr.name = item.name

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_time_registered_date(dw_session, stg_session):
    try:
        logging.info("running fill dim time from registered_date")

        for item in stg_session.query(Student).all():
            date = item.registered_date.date()
            quarter = math.ceil(date.month/3.)
            data = dict(date=date, month=date.month, year=date.year, quarter=quarter)

            is_update = dw_session.query(
                dw_session.query(DimTime)\
                          .filter(DimTime.date==date)\
                          .exists()
            ).scalar()

            if not is_update:
                logging.debug("inserting time %s", date)
                dw_session.add(DimTime(**data))
            else: 
                pass

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_time_session_start_time(dw_session, stg_session):
    try:
        logging.info("running fill dim time from session_start_time")

        for item in stg_session.query(Session).all():
            date = item.session_start_time.date()
            quarter = math.ceil(date.month/3.)
            data = dict(date=date, month=date.month, year=date.year, quarter=quarter)

            is_update = dw_session.query(
                dw_session.query(DimTime)\
                          .filter(DimTime.date==date)\
                          .exists()
            ).scalar()

            if not is_update:
                logging.debug("inserting time %s", date)
                dw_session.add(DimTime(**data))
            else: 
                pass

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_time_payment_date(dw_session, stg_session):
    try:
        logging.info("running fill dim time from payment_date")

        for item in stg_session.query(Subscription).all():
            date = item.payment_date.date()
            quarter = math.ceil(date.month/3.)
            data = dict(date=date, month=date.month, year=date.year, quarter=quarter)

            is_update = dw_session.query(
                dw_session.query(DimTime)\
                          .filter(DimTime.date==date)\
                          .exists()
            ).scalar()

            if not is_update:
                logging.debug("inserting time %s", date)
                dw_session.add(DimTime(**data))
            else: 
                pass

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_dim_time_follow_date(dw_session, stg_session):
    try:
        logging.info("running fill dim time from follow_date")

        for item in stg_session.query(StudentFollowSubject).all():
            date = item.follow_date.date()
            quarter = math.ceil(date.month/3.)
            data = dict(date=date, month=date.month, year=date.year, quarter=quarter)

            is_update = dw_session.query(
                dw_session.query(DimTime)\
                          .filter(DimTime.date==date)\
                          .exists()
            ).scalar()

            if not is_update:
                logging.debug("inserting time %s", date)
                dw_session.add(DimTime(**data))
            else: 
                pass

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_fact_logged_student(dw_session, stg_session):
    try:
        logging.info("running fill fact_logged_student")

        for item in stg_session.query(Session).all():
            time_id = lookup_time(dw_session, item.session_start_time.date())
            address_id = lookup_address(dw_session, stg_session, item.student_id)
            university_id = lookup_university(dw_session, stg_session, item.student_id)
            course_id = lookup_course(dw_session, stg_session, item.student_id)

            logging.debug("inserting fact_logged_student %s", item.id)
            dw_session.add(FactLoggedStudent(student_id=item.student_id,
                                             time_id=time_id,
                                             address_id=address_id,
                                             university_id=university_id,
                                             course_id=course_id))

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_fact_registered_student(dw_session, stg_session):
    try:
        logging.info("running fill fact_logged_student")

        for item in stg_session.query(Student).all():
            time_id = lookup_time(dw_session, item.registered_date.date())
            address_id = lookup_address(dw_session, stg_session, item.id)
            university_id = lookup_university(dw_session, stg_session, item.id)
            course_id = lookup_course(dw_session, stg_session, item.id)

            logging.debug("inserting fill_fact_registered_student %s", item.id)
            dw_session.add(FactRegisteredStudent(student_id=item.id,
                                                 time_id=time_id,
                                                 address_id=address_id,
                                                 university_id=university_id,
                                                 course_id=course_id))

        dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def fill_fact_subscribed_student(dw_session, stg_session):
    try:
        logging.info("running fill fact_subscribed_student")

        for item in stg_session.query(Subscription).all():
            time_id = lookup_time(dw_session, item.payment_date.date())
            address_id = lookup_address(dw_session, stg_session, item.student_id)
            university_id = lookup_university(dw_session, stg_session, item.student_id)
            course_id = lookup_course(dw_session, stg_session, item.student_id)
            plan_type_id = lookup_plan_type(dw_session, str(item.plan_type).strip())

            logging.debug("inserting fill_fact_subscribed_student %s", item.id)
            dw_session.add(FactSubscribedStudent(student_id=item.id,
                                                 time_id=time_id,
                                                 address_id=address_id,
                                                 university_id=university_id,
                                                 course_id=course_id,
                                                 plan_type_id=plan_type_id))

            dw_session.commit()

    except Exception as e:
        logging.error(e)
        dw_session.rollback()


def lookup_time(dw_session, date):
    try:
        time_query = dw_session.query(DimTime)\
                  .filter(DimTime.date==date)

        if dw_session.query(time_query.exists()).scalar():
            time_id = time_query.first().id
            logging.debug("lookup_time(%s, %s): %s", 
                          dw_session,
                          date,
                          time_id) 
            return time_id

        else:
            logging.debug("inserting time %s", date)
            quarter = math.ceil(date.month/3.)
            data = dict(date=date, month=date.month, year=date.year, quarter=quarter)
            dw_session.add(DimTime(**data))
            dw_session.commit()

        return lookup_time(dw_session, date)

    except Exception as e:
        logging.error(e)
        logging.error("lookup_time(%s, %s)", dw_session, date)


def lookup_address(dw_session, stg_session, student_id, state=None, city=None):
    try:
        if state or city:
            address_query = dw_session.query(DimAddress)\
                    .filter(DimAddress.state==state, 
                            DimAddress.city==city)

            if dw_session.query(address_query.exists()).scalar():
                address_id = address_query.first().address_id
                logging.debug("lookup_address(%s, %s, %s, %s, %s): %s", 
                              dw_session,
                              stg_session,
                              student_id,
                              state,
                              city,
                              address_id) 
                return address_id

            else:
                logging.debug("inserting address %s/%s", city, state)
                dw_session.add(DimAddress(city=city, state=state))
                dw_session.commit()

                return lookup_address(dw_session, stg_session, student_id, state, city)

        elif student_id:
            student_query = stg_session.query(Student)\
                                       .filter(Student.id==student_id)
            
            if stg_session.query(student_query.exists()).scalar():
                student = student_query.first()
                return lookup_address(dw_session,
                                      stg_session,
                                      None,
                                      student.state,
                                      student.city)

        else:
            logging.debug("lookup_address: row not found for %s, %s, %s",
                         student_id,
                         state,
                         city)

    except Exception as e:
        logging.error("lookup_address: %s", e)


def lookup_university(dw_session, stg_session, student_id):
    try:
        student_query = stg_session.query(Student)\
                                   .filter(Student.id==student_id)
            
        if stg_session.query(student_query.exists()).scalar():
            student = student_query.first()
            return student.university_id

        else:
            logging.error("lookup_university: row not found for %s",
                         student_id)

    except Exception as e:
        logging.error(e)


def lookup_course(dw_session, stg_session, student_id):
    try:
        student_query = stg_session.query(Student)\
                                   .filter(Student.id==student_id)
            
        if stg_session.query(student_query.exists()).scalar():
            student = student_query.first()
            return student.course_id

        else:
            logging.error("lookup_course: row not found for %s",
                         student_id)

    except Exception as e:
        logging.error("lookup_course: %s", e)


def lookup_plan_type(dw_session, plan_type):
    try:
        plan_type_query = dw_session.query(DimPlanType)\
                  .filter(DimPlanType.name==plan_type)

        if dw_session.query(plan_type_query.exists()).scalar():
            plan_type_id = plan_type_query.first().plan_type_id
            logging.debug("lookup_plan_type(%s, %s): %s", 
                          dw_session,
                          plan_type,
                          plan_type_id) 
            return plan_type_id

        else:
            dw_session.add(DimPlanType(name=plan_type))
            dw_session.commit()

        return lookup_plan_type(dw_session, plan_type)

    except Exception as e:
        logging.error("lookup_plan_type: %s", e)
        logging.error("lookup_plan_type(%s, %s)", dw_session, plan_type)


def run():
    # stg init
    stg_engine = create_engine("sqlite:///{}".format(STG_DATABASE_NAME))
    stg_session = sessionmaker(bind=stg_engine)()

    # dw init
    dw_engine = create_engine("sqlite:///{}".format(DW_DATABASE_NAME))
    dw_session = sessionmaker(bind=dw_engine)()

    Base.metadata.create_all(dw_engine, tables=get_dw_tables())

    start_time = time.process_time()
    logging.info(">>>starting %s", os.path.basename(__file__))
  
    fill_dim_courses(dw_session, stg_session)
    fill_dim_address(dw_session, stg_session)
    fill_dim_time_payment_date(dw_session, stg_session)
    fill_dim_time_registered_date(dw_session, stg_session)
    fill_dim_time_session_start_time(dw_session, stg_session)
    fill_dim_time_follow_date(dw_session, stg_session)
    fill_dim_student_follow_subject(dw_session, stg_session)
    fill_dim_university(dw_session, stg_session)
    fill_fact_logged_student(dw_session, stg_session)
    fill_fact_registered_student(dw_session, stg_session)
    fill_fact_subscribed_student(dw_session, stg_session)

    elapsed_time = time.process_time() - start_time
    logging.info(">>>finished %s, elapsed time: %0.2fs",
                 os.path.basename(__file__),
                 elapsed_time)
  

if __name__ == '__main__':
    run()