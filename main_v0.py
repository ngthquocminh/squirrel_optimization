from collections import namedtuple

from tqdm import tqdm
import pandas as pd
from docplex.mp.model import Model
from docplex.util.environment import get_environment
from functools import reduce
import numpy as np
from pandas.core.algorithms import mode

# ----------------------------------------------------------------------------
# Initialize the problem data
# ----------------------------------------------------------------------------

TMemberAvailability = namedtuple(
    "TMemberAvailability", ["ContactID", "TimeFrom", "TimeTo"]
)
TMemberWorksiteReference = namedtuple(
    "TMemberWorksiteReference", ["ContactID", "Worksite"]
)
TMemberShiftReference = namedtuple(
    "TMemberShiftReference", ["ContactID", "Shift", "StartRange", "EndRange"]
)
TMemberMeasurement = namedtuple("TMemberMeasurement", ["ContactID", "Measurement"])
TVacancyDetail = namedtuple(
    "TVacancyDetail",
    [
        "StartDate",
        "EndDate",
        "Quantity",
        "Worksite",
        "Position",
        "WorksiteID",
        "PositionID",
        "Measurement",
    ],
)
TVacancyObjectTime = namedtuple(
    "TVacancyObjectTime", ["Title", "DateFrom", "DateTo", "Position", "Worksite"]
)
TShiftConstraints = namedtuple(
    "TShiftContraints",
    [
        "MinPeopleWorking",
        "ScheduledBreakHours",
        "MaxHoursPerDay",
        "MaxHoursPerWeek",
        "MaxConsecutiveShift",
        "MinBreakBetweenShift",
        "MinHoursPerShift",
    ],
)
TRange = namedtuple("TRange", ["From", "To"])

file_name = "Squirrel_Optimization.xlsx"
excel_data_file = pd.ExcelFile(file_name)

MAX_SHIFT_PER_OBJECTTIME = 3
MAX_BREAK_PER_SHIFT = 4
DEFAULT_BREAK_LENGTH = 0.5
MIN_SHIFT_LENGTH = 4
NUM_OBJECTTIME_PER_DAY = 12


def lookup(lst, func):
    # print("Lookup ")
    for i in lst:
        # print(i)
        # print(func(i))
        if func(i):
            return i


def load_data(model, excel, verbose):

    df_vacancy_detail = excel.parse("Vacancy")
    df_vacancy_objectTime = excel.parse("Vacancy Object Time")
    df_teamMember_measurement = excel.parse("Team Member Measurement")
    df_teamMember_worksiteReference = excel.parse("Team Member Worksite Preference")
    df_teamMember_availability = excel.parse("Team Member Availability")
    df_teamMember_shiftReference = excel.parse("Team Member Shift Preference")
    df_shift_constraints = excel.parse("Shift Constraints")

    anchor_date = df_vacancy_detail["StartDate"][0]
    # print(anchor_date)
    date2num = lambda dt: int((dt - anchor_date).total_seconds() / 60)
    # print(list(map(date2num, df_vacancy_objectTime["DateFrom"])))

    df_vacancy_objectTime.loc[:, "DateFrom"] = list(
        map(date2num, df_vacancy_objectTime["DateFrom"])
    )
    df_vacancy_objectTime.loc[:, "DateTo"] = list(
        map(date2num, df_vacancy_objectTime["DateTo"])
    )
    df_teamMember_availability.loc[:, "TimeFrom"] = list(
        map(date2num, df_teamMember_availability["TimeFrom"])
    )
    df_teamMember_availability.loc[:, "TimeTo"] = list(
        map(date2num, df_teamMember_availability["TimeTo"])
    )

    del df_teamMember_availability["Team Member"]
    del df_teamMember_worksiteReference["Team Member"]
    del df_teamMember_shiftReference["Team Member"]
    del df_teamMember_measurement["Team Member"]

    MEM_AVAILAVILITY = [
        TMemberAvailability(*row) for _, row in df_teamMember_availability.iterrows()
    ]
    MEM_WORKSITE_REFERENCE = [
        TMemberWorksiteReference(*row)
        for _, row in df_teamMember_worksiteReference.iterrows()
    ]
    MEM_SHIFT_REFERENCE = [
        TMemberShiftReference(*row)
        for _, row in df_teamMember_shiftReference.iterrows()
    ]
    MEM_MEASUREMENT = [
        TMemberMeasurement(*row) for _, row in df_teamMember_measurement.iterrows()
    ]
    VACANCY_DETAIL = [TVacancyDetail(*row) for _, row in df_vacancy_detail.iterrows()][
        0
    ]
    VACANCY_OBJECTTIME = [
        TVacancyObjectTime(*row) for _, row in df_vacancy_objectTime.iterrows()
    ]
    SHIFT_CONSTRAINTS = TShiftConstraints(
        *tuple(
            [
                i if i != "04:00 to 06:00" else TRange(4.0*60, 6.0*60)
                for i in df_shift_constraints["Value"].tolist()
            ]
        )
    )

    # model.number_of_overlaps = 0
    model.availabilities = MEM_AVAILAVILITY
    model.objecttimes = VACANCY_OBJECTTIME
    model.worksite_refs = MEM_WORKSITE_REFERENCE
    model.shift_refs = MEM_SHIFT_REFERENCE
    model.member_measurement = MEM_MEASUREMENT
    model.shift_constraints = SHIFT_CONSTRAINTS
    model.vacancy_detail = VACANCY_DETAIL


def setup_data(model: Model):
    model.members = {m.ContactID: m for m in model.member_measurement}
    model.objecttime_ids = {i: o for i, o in enumerate(model.objecttimes)}


def setup_variables(model: Model):

    # MemberAssignment_contactId
    model.member_assignment_vars = model.binary_var_dict(
        keys=model.members.keys(), name="MemberAssignment"
    )

    # if a shift is assigned -> 1
    model.shift_assignment_vars = model.binary_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=range(0, MAX_SHIFT_PER_OBJECTTIME),
        name="MemberAssignment",
    )
    # print(model.shift_assignment_vars, "\n")

    # ShiftStart_contactId_objectId_shift
    model.shift_start_vars = model.integer_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=range(0, MAX_SHIFT_PER_OBJECTTIME),
        lb=0,
        name="ShiftStart",
    )

    # print(model.shift_start_vars,"\n")
    # ShiftEnd
    model.shift_end_vars = model.integer_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=range(0, MAX_SHIFT_PER_OBJECTTIME),
        lb=0,
        name="ShiftEnd",
    )

    # BreakStart
    model.break_start_vars = model.integer_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=[
            "{0}_br{1}".format(i, j)
            for i in range(0, MAX_SHIFT_PER_OBJECTTIME)
            for j in range(0, MAX_BREAK_PER_SHIFT)
        ],
        lb=0,
        name="BreakStart",
    )

    # BreakDuration
    # print(model.break_start_vars)
    model.break_duration_vars = model.integer_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=[
            "{0}_br{1}".format(i, j)
            for i in range(0, MAX_SHIFT_PER_OBJECTTIME)
            for j in range(0, MAX_BREAK_PER_SHIFT)
        ],
        lb=0,
        name="BreakDuration",
    )

    return


def setup_constraints(model: Model):
    getDate = lambda x: int(x / (24 * 60))
    numDayOfVacancy = (
        model.vacancy_detail.EndDate - model.vacancy_detail.StartDate
    ).days + 1

    # IF any partial shift of a member is assigned => this member is assigned
    for ctactId, assignmendVar in model.member_assignment_vars.items():
        model.add_constraint(
            model.if_then(
                model.sum(
                    model.shift_assignment_vars[(ctactId, objecttimeId, shiftId)]
                    for objecttimeId in model.objecttime_ids.keys()
                    for shiftId in range(0, MAX_SHIFT_PER_OBJECTTIME)
                )
                >= 1,
                assignmendVar == 1,
            ),
            "ShiftAssignedToMemberAssigned",
        )

    # Vacancy-quantiy Requirement: the number of assigned members that vacancy needs
    model.add_constraint(
        model.sum(
            assignment_var for _, assignment_var in model.member_assignment_vars.items()
        )
        == model.vacancy_detail.Quantity,
        "VacancyQuantiyRequirement",
    )

    # Limit of conseque day-shift
    model._worked_day_vars = model.binary_var_matrix(
        model.members.keys(), range(0, numDayOfVacancy), "_WorkedDay"
    )
    getObjecttimesByDay = lambda day: reduce(
        lambda lst, objt: lst
        + ([objt] if getDate(objt[1].DateFrom / 2 + objt[1].DateTo / 2) == day else []),
        model.objecttime_ids.items(),
        list(),
    )
    maxConsecutiveShift = model.shift_constraints.MaxConsecutiveShift
    for ctactId in model.members.keys():
        for day in range(0, numDayOfVacancy):
            model.add_constraint(
                model.if_then(
                    model.sum(
                        model.shift_assignment_vars[ctactId, objtId, shift]
                        for objtId, objt in getObjecttimesByDay(day)
                        for shift in range(0, MAX_SHIFT_PER_OBJECTTIME)
                    )
                    >= 1,  # there is atleast 1 assigned shift
                    model._worked_day_vars[(ctactId, day)] == 1,
                ),
                "WorkedDay",
            )

            model.add_constraint(
                model.le_constraint(
                    model.sum(
                        (
                            model.shift_end_vars[(ctactId, objtId, shft)]
                            - model.shift_start_vars[(ctactId, objtId, shft)]
                        )
                        for objtId, objt in getObjecttimesByDay(day)
                        for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                    )
                    - model.sum(
                        model.break_duration_vars[
                            (ctactId, objtId, "{0}_br{1}".format(shft, brk))
                        ]
                        for objtId, objt in getObjecttimesByDay(day)
                        for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                        for brk in range(0, MAX_BREAK_PER_SHIFT)
                    ),
                    model.shift_constraints.MaxHoursPerDay,
                    "MaxHoursPerDay",
                )
            )

        model.add_constraint(
            model.le_constraint(
                model.sum(
                    (
                        model.shift_end_vars[(ctactId, objtId, shft)]
                        - model.shift_start_vars[(ctactId, objtId, shft)]
                    )
                    for objtId, objt in getObjecttimesByDay(day)
                    for day in range(0, numDayOfVacancy)
                    for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                )
                - model.sum(
                    model.break_duration_vars[
                        (ctactId, objtId, "{0}_br{1}".format(shft, brk))
                    ]
                    for objtId, objt in getObjecttimesByDay(day)
                    for day in range(0, numDayOfVacancy)
                    for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                    for brk in range(0, MAX_BREAK_PER_SHIFT)
                ),
                model.shift_constraints.MaxHoursPerWeek,
                "MaxHoursPerWeek",
            )
        )

        for days in range(maxConsecutiveShift, numDayOfVacancy):
            model.add_constraint(
                model.if_then(
                    model.sum(
                        model._worked_day_vars[(ctactId, day)]
                        for day in range(days - maxConsecutiveShift, days)
                    )
                    >= maxConsecutiveShift,
                    model._worked_day_vars[(ctactId, days)] == 0,
                ),
                "LimitConsequeWorkedDays",
            )

        for objecttimeId, objectTime in model.objecttime_ids.items():
            for shft in range(0, MAX_SHIFT_PER_OBJECTTIME):
                objtDate = getDate(objectTime.DateFrom / 2 + objectTime.DateTo / 2)
                model.add_constraint(
                    model.if_then(
                        model._worked_day_vars[(ctactId, days)] == 0,
                        model.shift_assignment_vars[ctactId, objecttimeId, shft] == 0,
                    ),
                    "WorkedDayToShiftAssignment",
                )

    for ctactId in model.members.keys():
        for objecttimeId in model.objecttime_ids.keys():
            for shft in range(0, MAX_SHIFT_PER_OBJECTTIME):

                varKey = (ctactId, objecttimeId, shft)
                shiftStart_var = model.shift_start_vars[varKey]
                shiftEnd_var = model.shift_end_vars[varKey]

                objectTime = model.objecttime_ids[objecttimeId]

                model.add_constraint(
                    shiftStart_var >= objectTime.DateFrom,
                    "ShiftStartRightAfterDateFrom",
                )
                model.add_constraint(
                    shiftEnd_var <= objectTime.DateTo, "ShiftEndBeforeObjectTimeEnding",
                )

                # print(shiftStart_var,objectTime)
                timeAvailability = lookup(
                    model.availabilities,
                    lambda i: (ctactId == i.ContactID)
                    and getDate((i.TimeFrom + i.TimeTo) / 2)
                    == getDate((objectTime.DateFrom + objectTime.DateTo) / 2),
                )

                # print(shiftStart_var,timeAvailability)
                if timeAvailability:

                    # Keep shift_Start inside objectTime_range and memAvailavility_range
                    model.add_constraint(
                        shiftStart_var >= timeAvailability.TimeFrom,
                        "ShiftStartRightAfterAvailability",
                    )

                    # Keep shift_end inside objectTime_range
                    model.add_constraint(
                        shiftEnd_var <= timeAvailability.TimeTo,
                        "ShiftEndBeforeAvailabilityEnding",
                    )

                    # End - Start >= MIN_SHIFT_LENGTH
                    model.add_constraint(
                        (
                            shiftEnd_var - shiftStart_var
                            >= MIN_SHIFT_LENGTH * model.shift_assignment_vars[varKey]
                        ),
                        "ShiftStartSmallerThanShiftEnd",
                    )
                else:
                    # If a shift_var of a member who is not availabe -> Start == End
                    model.add_constraint(
                        shiftStart_var == shiftEnd_var, "ShiftStart==ShiftEnd",
                    )

    for ctactId in model.members.keys():
        for objecttimeId in model.objecttime_ids.keys():
            for shft in range(1, MAX_SHIFT_PER_OBJECTTIME):
                shift_end = model.shift_end_vars[(ctactId, objecttimeId, shft - 1)]
                nextShift_start = model.shift_start_vars[(ctactId, objecttimeId, shft)]
                # There is at least a 30-min break between 2 shift
                model.add_constraint(
                    nextShift_start - shift_end >= DEFAULT_BREAK_LENGTH,
                    "NextShiftOfOneDay",
                )

        # prevent shift overlaping of each member from different objecttime
        # also set Aleast 1 30-min break between 2 shift if both are assigned
        # example:
        # shiftA: [a1..a2], shiftB: [b1..b2]
        # abs((a1 + a2)/2 - (b1 + b2)/2) >= (a2-a1)/2 + (b2-b1)/2 + (minBreakLen if both shiftA and shiftB are assigned)
        # for day in range(0, numDayOfVacancy):
        #     lstObjt = getObjecttimesByDay(day)
        #     for i in range(0,len(lstObjt)):
        #         objtId, objt = lstObjt[i]
        #         for otherObjtId, _ in lstObjt[i+1:]:
        #             for shft in range(0, MAX_SHIFT_PER_OBJECTTIME):
        #                 for otherShft in range(0, MAX_SHIFT_PER_OBJECTTIME):
        #                     model.add_constraint(
        #                         model.abs(
        #                             (
        #                                 model.shift_start_vars[(ctactId, objtId, shft)]
        #                                 + model.shift_end_vars[(ctactId, objtId, shft)]
        #                             )
        #                             - (
        #                                 model.shift_start_vars[(ctactId, otherObjtId, otherShft)]
        #                                 + model.shift_end_vars[(ctactId, otherObjtId, otherShft)]
        #                             )
        #                         )
        #                         >= (
        #                             (
        #                                 model.shift_end_vars[(ctactId, objtId, shft)]
        #                                 - model.shift_start_vars[(ctactId, objtId, shft)]
        #                             )
        #                             + (
        #                                 model.shift_end_vars[(ctactId, otherObjtId, otherShft)]
        #                                 - model.shift_start_vars[(ctactId, otherObjtId, otherShft)]
        #                             )
        #                             + 2 * DEFAULT_BREAK_LENGTH
        #                             * model.shift_assignment_vars[(ctactId, objtId, shft)]
        #                             * model.shift_assignment_vars[
        #                                 (ctactId, otherObjtId, otherShft)
        #                             ]
        #                         )
        #                     )

        # on the same day, each member only work in one object time
        for day in range(0, numDayOfVacancy):
            lstObjt = getObjecttimesByDay(day)
            for i in range(0,):
                if i + 1 == len(lstObjt):
                    break
                objtId, objt = lstObjt[i]
                model.add_constraint(
                    model.if_then(
                        model.sum(
                            model.shift_assignment_vars[(ctactId, objtId, shft)]
                            for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                        )
                        >= 1,
                        model.sum(
                            model.shift_assignment_vars[(ctactId, otherObjtId, shft)]
                            for otherObjtId, _ in lstObjt[i + 1 :]
                            for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                        )
                        == 0,
                    )
                )
    n = 0
    for day in range(0, numDayOfVacancy):
        lstObjt = getObjecttimesByDay(day)
        for objtId, objt in lstObjt:
            for moment in range(int(objt.DateFrom), int(objt.DateTo), 30):
                working_vars = []
                for contactId in model.members.keys():
                    for shft in range(0, MAX_SHIFT_PER_OBJECTTIME):
                        key = (contactId,objtId, shft)
                        working_Var = model.binary_var()
                        start = model.shift_start_vars[key]
                        end = model.shift_end_vars[key]
                        n+=1
                        a = model.binary_var()
                        b = model.binary_var()
                        model.add_constraint(model.if_then(start >= moment,a==1))
                        model.add_constraint(model.if_then(moment <= end,b==1))
                        arr = [a,b]
                        for brk in range(0, MAX_BREAK_PER_SHIFT):
                            _a = model.binary_var()
                            _b = model.binary_var()
                            _key = (contactId,objtId,"{0}_br{1}".format(shft,brk))
                            _start = model.break_start_vars[_key]
                            _duration = model.break_start_vars[_key]
                            model.add_constraint(model.if_then(moment >= _start, _a==1))
                            model.add_constraint(model.if_then(moment <= (_start + _duration), _b==1))
                            arr.append(_a)
                            arr.append(_b)
                            
                        model.add_if_then( 
                            model.logical_and(*arr)==1,
                            working_Var == 1,
                        )
                    print(n)
                
            # model.add_constraint(
            #     model.sum(
            #         model.
            #     )
            #     <=3
            # )
    print(n)
    return


def setup_objective(model: Model):
    pass


def print_information(model: Model):
    print("#member=%d" % len(model.availabilities))
    model.print_information()
    model.report_kpis()


def print_solution(model: Model):
    print("*************************** Solution ***************************")


def solve(model: Model, **kwargs):
    # Here, we set the number of threads for CPLEX to 2 and set the time limit to 2mins.
    model.parameters.threads = 16
    model.parameters.timelimit = 600  # nurse should not take more than that !
    sol = model.solve(log_output=True, **kwargs)
    if sol is not None:
        print("solution for a cost of {}".format(model.objective_value))
        print_information(model)
        print_solution(model)
        return model.objective_value
    else:
        print("* model is infeasible")
        return None


def build(context=None, verbose=False, **kwargs):
    mdl = Model("Nurses", context=context, **kwargs)
    load_data(mdl, excel_data_file, verbose=verbose)
    setup_data(mdl)
    setup_variables(mdl)
    setup_constraints(mdl)
    setup_objective(mdl)
    return mdl


def displayModel(model: Model):
    df = pd.DataFrame()
    # df["ContactId"] = [
    #     key[0] for key,var in model.shift_assignment_vars.items()
    # ]
    df["Assigned"] = [
        model.solution.get_value(var.name)
        for key, var in model.shift_assignment_vars.items()
        if model.solution.get_value(var.name) == 1
    ]
    print(df)
    # print(model.solution.get_value("ShiftEnd_92764A21-6456-432B-B126-FA53152BBC3D_76_0"))


# ----------------------------------------------------------------------------
# Solve the model and display the result
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    # Build model
    model = build()

    # Solve the model and print solution
    solve(model)

    # Save the CPLEX solution as "solution.json" program output
    with get_environment().get_output_stream("solution.json") as fp:
        model.solution.export(fp, "json")
    displayModel(model)
