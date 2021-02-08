from model.stg.course import Course
from model.stg.session import Session
from model.stg.student_follow_subject import StudentFollowSubject
from model.stg.student import Student
from model.stg.subject import Subject
from model.stg.subscription import Subscription
from model.stg.university import University

def get_stg_tables():
    return [
        Course.__table__,
        Session.__table__,
        StudentFollowSubject.__table__,
        Student.__table__,
        Subject.__table__,
        Subscription.__table__,
        University.__table__,
    ]