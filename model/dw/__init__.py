from model.dw.dim_course import DimCourse
from model.dw.dim_address import DimAddress
from model.dw.dim_student_follow_subject import DimStudentFollowSubject
from model.dw.dim_subject import DimSubject
from model.dw.dim_time import DimTime
from model.dw.dim_university import DimUniversity
from model.dw.dim_plan_type import DimPlanType
from model.dw.fact_logged_student import FactLoggedStudent
from model.dw.fact_registered_student import FactRegisteredStudent
from model.dw.fact_subscribed_student import FactSubscribedStudent

def get_dw_tables():
    return [
        DimCourse.__table__,
        DimAddress.__table__,
        DimStudentFollowSubject.__table__,
        DimSubject.__table__,
        DimTime.__table__,
        DimUniversity.__table__,
        DimPlanType.__table__,
        FactLoggedStudent.__table__,
        FactRegisteredStudent.__table__,
        FactSubscribedStudent.__table__,
    ]