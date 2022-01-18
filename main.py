from collections import namedtuple
import datetime
from tqdm import tqdm
import pandas as pd
from docplex.mp.model import Model
from docplex.util.environment import get_environment
from functools import reduce
import numpy as np

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
TMemberMeasurement = namedtuple(
    "TMemberMeasurement", ["ContactID", "Measurement"])
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
    "TVacancyObjectTime", ["Title", "DateFrom",
                           "DateTo", "Position", "Worksite"]
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

MAX_SHIFT_PER_OBJECTTIME = 2
MAX_BREAK_PER_SHIFT = 3
DEFAULT_BREAK_LENGTH = 0.5 * 60
MIN_SHIFT_LENGTH = 4 * 60
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
    df_teamMember_worksiteReference = excel.parse(
        "Team Member Worksite Preference")
    df_teamMember_availability = excel.parse("Team Member Availability")
    df_teamMember_shiftReference = excel.parse("Team Member Shift Preference")
    df_shift_constraints = excel.parse("Shift Constraints")

    anchor_date = df_vacancy_detail["StartDate"][0]
    # print(anchor_date)
    def date2num(dt): return int((dt - anchor_date).total_seconds() / 60)
    model.num2date = lambda n: anchor_date + datetime.timedelta(
        days=int(n / (60 * 24)), hours=int((n % (60 * 24))) / 60
    )
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
                i if i != "04:00 to 06:00" else TRange(4 * 60, 7 * 60)
                for i in df_shift_constraints["Value"].tolist()
            ]
        )
    )

    # model.number_of_overlaps = 0
    model.availabilities = MEM_AVAILAVILITY
    model.objecttimes = VACANCY_OBJECTTIME
    model.worksite_refs = MEM_WORKSITE_REFERENCE
    model.shift_refs = MEM_SHIFT_REFERENCE
    # Question: Why 25 observations??
    model.member_measurement = MEM_MEASUREMENT[:25]
    model.shift_constraints = SHIFT_CONSTRAINTS
    model.vacancy_detail = VACANCY_DETAIL


def setup_data(model: Model):
    model.members = {m.ContactID: m for m in model.member_measurement}
    lst = list(set(model.objecttimes))
    lst.sort(key=lambda x: x.DateFrom)
    model.objecttime_ids = {i: o for i, o in enumerate(lst[:1])}


def setup_variables(model: Model):

    # MemberAssignment_contactId
    print("Num of Members: ", len(model.members))
    model.member_assignment_vars = model.binary_var_dict(
        keys=model.members.keys(), name="MemberAssignment"
    )

    # if a shift is assigned -> 1
    model.shift_assignment_vars = model.binary_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=range(0, MAX_SHIFT_PER_OBJECTTIME),
        name="ShiftAssignment",
    )
    # print(model.shift_assignment_vars, "\n")

    # ShiftStart_contactId_objectId_shift
    model.shift_start_vars = model.integer_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=range(0, MAX_SHIFT_PER_OBJECTTIME),
        lb=0,
        ub=20000,  # Limit for 10 days
        name="ShiftStart",
    )

    # print(model.shift_start_vars,"\n")
    # ShiftEnd
    model.shift_end_vars = model.integer_var_cube(
        keys1=model.members.keys(),
        keys2=model.objecttime_ids.keys(),
        keys3=range(0, MAX_SHIFT_PER_OBJECTTIME),
        lb=0,
        ub=20000,  # Limit for 10 days
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
        ub=60 * 24 * 10,  # Limit for 10 days
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
        ub=DEFAULT_BREAK_LENGTH,  # maximum break length (in minute)
        name="BreakDuration",
    )

    # model.member_over_average_time_vars = model.continuous_var_dict(
    #     model.members.keys(), lb=0, name="MemberOverAverageWorkTime"
    # )
    # model.member_under_average_time_vars = model.continuous_var_dict(
    #     model.members.keys(), lb=0, name="MemberUnderAverageWorkTime"
    # )
    # model.average_member_work_time = model.continuous_var(lb=0, name="AverageWorkTime")
    return


def setup_constraints(model: Model):
    def getDate(x): return int(x / (24 * 60))
    numDayOfVacancy = (
        model.vacancy_detail.EndDate - model.vacancy_detail.StartDate
    ).days + 1

    # If any partial shift of a member is assigned => this member is assigned
    for ctactId, assignmendVar in model.member_assignment_vars.items():
        lstShift = [
            model.shift_assignment_vars[(ctactId, objecttimeId, shiftId)]
            for objecttimeId in model.objecttime_ids.keys()
            for shiftId in range(0, MAX_SHIFT_PER_OBJECTTIME)
        ]
        # if assigned
        model.add_constraint(
            model.equivalence_constraint(
                assignmendVar, model.sum(lstShift) >= 1, true_value=1
            ),
            "ShiftAssignedToMemberAssigned",
        )
        # else
        model.add_constraint(
            model.equivalence_constraint(
                assignmendVar, model.sum(lstShift) == 0, true_value=0
            ),
            "ShiftAssignedToMemberAssigned",
        )

    # ## CONSTRAINT: LIMIT THE NUMBER OF CONSECUTIVE day-SHIFT
    # _working_day_vars = model.binary_var_matrix(
    #     model.members.keys(), range(0, numDayOfVacancy), "_WorkedDay"
    # )
    # maxConsecutiveShift = model.shift_constraints.MaxConsecutiveShift
    # for ctactId in model.members.keys():
    #     for objtId, objt in model.objecttime_ids.items():
    #         # Flag a day is a working day if it has atleast 1 shift
    #         model.add_constraint(
    #             model.equivalence_constraint(
    #                 _working_day_vars[(ctactId, objtId)],
    #                 model.sum(
    #                     model.shift_assignment_vars[ctactId, objtId, shift]
    #                     for shift in range(0, MAX_SHIFT_PER_OBJECTTIME)
    #                 )
    #                 >= 1,  # there is atleast 1 assigned shift
    #             ),
    #             "CheckingWorkingDay",
    #         )
    #         # Flag a day is NOT a working day if it has NO shift
    #         model.add_constraint(
    #             model.equivalence_constraint(
    #                 _working_day_vars[(ctactId, objtId)],
    #                 model.sum(
    #                     model.shift_assignment_vars[ctactId, objtId, shift]
    #                     for shift in range(0, MAX_SHIFT_PER_OBJECTTIME)
    #                 )
    #                 == 0,  # there is no assigned shift
    #                 true_value=0,
    #             ),
    #             "CheckingWorkingDay",
    #         )

    #     # Set a day off for the 7th day if the 6 days before are consecutive
    #     for days in range(maxConsecutiveShift, numDayOfVacancy):
    #         model.add_constraint(
    #             model.if_then(
    #                 model.sum(
    #                     _working_day_vars[(ctactId, day)]
    #                     for day in range(days - maxConsecutiveShift, days)
    #                 )
    #                 >= maxConsecutiveShift,
    #                 _working_day_vars[(ctactId, days)] == 0,
    #             ),
    #             "LimitConsecutiveWorkedDays",
    #         )

    # CONSTRAINT : LIMIT WORKING HOUR PER DAY
    for ctactId in model.members.keys():
        for objtId, objt in model.objecttime_ids.items():
            model.add_constraint(
                model.le_constraint(
                    model.sum(
                        (
                            model.shift_end_vars[(ctactId, objtId, shft)]
                            - model.shift_start_vars[(ctactId, objtId, shft)]
                        )
                        for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                    )
                    - model.sum(
                        model.break_duration_vars[
                            (ctactId, objtId, "{0}_br{1}".format(shft, brk))
                        ]
                        for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                        for brk in range(0, MAX_BREAK_PER_SHIFT)
                    ),
                    model.shift_constraints.MaxHoursPerDay * 60,  # Minutes
                    "MaxHoursPerDay",
                )
            )

    # CONSTRAINT : LIMIT WORKING HOUR PER WEEEK
    # currently, considering the whole vacancy is a week
    # model.work_time_var = {}
    # for ctactId in model.members.keys():
    #     model.work_time_var[ctactId] = model.sum(
    #         (
    #             model.shift_end_vars[(ctactId, objtId, shft)]
    #             - model.shift_start_vars[(ctactId, objtId, shft)]
    #         )
    #         for objtId in model.objecttime_ids.keys()
    #         for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
    #     )
    #     # -model.sum(
    #     #     model.break_duration_vars[(ctactId, objtId, "{0}_br{1}".format(shft, brk))]
    #     #     for objtId in model.objecttime_ids.keys()
    #     #     for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
    #     #     for brk in range(0, MAX_BREAK_PER_SHIFT)
    #     # )
    #     model.add_constraint(
    #         model.le_constraint(
    #             model.work_time_var[ctactId],
    #             model.shift_constraints.MaxHoursPerWeek * 60,  # Minutes
    #             "MaxHoursPerWeek",
    #         )
    #     )

    # Normal shift constraints
    for ctactId, objtId, shft in model.shift_assignment_vars.keys():
        objt = model.objecttime_ids[objtId]
        varKey = (ctactId, objtId, shft)
        shiftStart_var = model.shift_start_vars[varKey]
        shiftEnd_var = model.shift_end_vars[varKey]
        
        
        # Question: All values of objt.DateFrom will be 300? As 
        # model.objecttime_ids = {i: o for i, o in enumerate(lst[:1])}
        
        # Set range for shift_start according to objectTime
        model.add_constraint(
            shiftStart_var >= objt.DateFrom, "Shift.Start>=Date.From",
        )
        
        # Question: All values of objt.DateFrom will be 1320? As 
        # model.objecttime_ids = {i: o for i, o in enumerate(lst[:1])}

        # Set range for shift_end according to objectTime
        model.add_constraint(
            shiftEnd_var <= objt.DateTo, "Shift.End<=Date.To",
        )

        # Question: if shift is not assigned, shiftStart_var == shiftEnd_var can still
        # be non-zero values
        
        # if shift is not assigned
        model.add_equivalence(
            model.shift_assignment_vars[varKey],
            shiftStart_var == shiftEnd_var,
            true_value=0,
            name="ShiftAssignment",
        )
        # else
        model.add_equivalence(
            model.shift_assignment_vars[varKey],
            shiftEnd_var - shiftStart_var >= MIN_SHIFT_LENGTH,
            true_value=1,
            name="ShiftAssignment",
        )

        isScheduleBreak = model.binary_var()
        model.add_equivalence(
            isScheduleBreak,
            shiftEnd_var - shiftStart_var
            >= model.shift_constraints.ScheduledBreakHours.From,
        )

        for brk in range(0, MAX_BREAK_PER_SHIFT):
            brk_key = (ctactId, objtId, "{0}_br{1}".format(shft, brk))
            model.add_constraint(
                model.break_start_vars[brk_key] >= shiftStart_var,
                "Break.Start>=Shift.Start",
            )
            
        # Note: You can replace break_duration_vars with DEFAULT_BREAK_LENGTH 
        # to reduce the number of constraints
        
            model.add_constraint(
                model.break_duration_vars[brk_key] +
                model.break_start_vars[brk_key]
                <= shiftEnd_var,
                "Break.Start+Duration<=Shift.End",
            )

            model.add_constraint(
                model.indicator_constraint(
                    isScheduleBreak,
                    model.break_start_vars[brk_key]
                    >= shiftStart_var
                    + model.shift_constraints.ScheduledBreakHours.From,
                )
            )

            model.add_constraint(
                model.indicator_constraint(
                    isScheduleBreak,
                    model.break_start_vars[brk_key] + DEFAULT_BREAK_LENGTH
                    <= shiftStart_var + model.shift_constraints.ScheduledBreakHours.To,
                )
            )

            model.add_constraint(
                model.indicator_constraint(
                    isScheduleBreak,
                    model.break_duration_vars[brk_key] == DEFAULT_BREAK_LENGTH,
                )
            )
            
            # Question: In the previous indicator constraint, you are already setting 
            # break_duration_vars == DEFAULT_BREAK_LENGTH, then this constraint might be 
            # redundant?
            
            model.add_constraint(
                model.if_then(
                    model.break_duration_vars[brk_key] >= 1,  # > 0
                    model.break_duration_vars[brk_key] == DEFAULT_BREAK_LENGTH,
                ),
                "Break.Duration==0_Or_>=MIN",
            )


            if brk < MAX_BREAK_PER_SHIFT - 1:
                next_brk_key = (ctactId, objtId,
                                "{0}_br{1}".format(shft, brk + 1))
                model.add_constraint(
                    model.break_duration_vars[brk_key] +
                    model.break_start_vars[brk_key]
                    <= model.break_start_vars[next_brk_key],
                    "Break.Start+Duration<=NextBreak.Start",
                )
        # timeAvailability = lookup(
        #     model.availabilities,
        #     lambda i: (ctactId == i.ContactID)
        #     and getDate((i.TimeFrom + i.TimeTo) / 2)
        #     == getDate((objt.DateFrom + objt.DateTo) / 2),
        # )

        # # print(shiftStart_var,timeAvailability)
        # if timeAvailability:
        #     "If this member is availabilities for this objecttime"
        #     # Set range for shift_end according to member Availability
        #     model.add_constraint(
        #         shiftStart_var >= timeAvailability.TimeFrom,
        #         "Shift.Start>=Availability.Start",
        #     )

        #     # Set range for shift_end according to member Availability
        #     model.add_constraint(
        #         shiftEnd_var <= timeAvailability.TimeTo,
        #         "Shift.End<=Availability.End",
        #     )
        # else:
        #     "If a shift_var of a member who is not availabe -> Start == End"
        #     model.add_constraint(
        #         shiftStart_var == shiftEnd_var, "ShiftStart==ShiftEnd",
        #     )
        #     model.add_constraint(model.shift_assignment_vars[varKey] == 0)

    # CONSTRAINT: MINIMUM BREAK BETWEEN SHIFTS = 1
    # * also set each shift on a day cannot must not be overlap
    for ctactId in model.members.keys():
        for objtId in model.objecttime_ids.keys():
            for shft in range(1, MAX_SHIFT_PER_OBJECTTIME):
                keyCurrentShift = (ctactId, objtId, shft - 1)
                keyNextShift = (ctactId, objtId, shft)
                thisShift_end = model.shift_end_vars[keyCurrentShift]
                nextShift_start = model.shift_start_vars[keyNextShift]

                # By default, shift1.end <= shift2.start
                # Note: Can remove this constraint as this would be redundant due to the 
                # next constraint
                model.add_constraint(
                    thisShift_end <= nextShift_start,
                    "Shift{0}.End<=Shift{1}.Start".format(shft - 1, shft),
                )

                # There is atleast a 30-min break between 2 assigned consecutive-shift
                model.add_constraint(
                    model.indicator_constraint(
                        model.shift_assignment_vars[keyCurrentShift],
                        nextShift_start - thisShift_end >= DEFAULT_BREAK_LENGTH * 1,
                    ),
                    "TwoAdjacentShift",
                )

    # CONSTRAINT: MAKE SURE THERE ARE ALWAYS 'Minimum People Working' AT ANY MOMENT
    minPeopleWorking = model.shift_constraints.MinPeopleWorking
    vacancyQuantiyRequirement = model.vacancy_detail.Quantity
    vacancyQuantiyRequirement = 12
    minPeopleWorking = 9
    for objtId, objt in tqdm(model.objecttime_ids.items()):
        # check for every moment with offset = 30min
        getShiftKeys = [
            key for key in model.shift_assignment_vars.keys() if key[1] == objtId
        ]

        # --------------------------------------
        # Check Object-time Start
        # Question: Is this constraint to check if shift starts at checkedStartTime or not?
        
        checkedStartTime = objt.DateFrom
        shiftStartCheckingVarList = []
        for thisShiftKey in getShiftKeys:
            shiftCheckingVar = model.binary_var(
                "ShiftCheckingVar_{0}_{1}".format(thisShiftKey, "START")
            )
            shiftStartCheckingVarList.append(shiftCheckingVar)

            timeCheckingVar = model.binary_var()
            model.add_equivalence(
                timeCheckingVar,
                checkedStartTime == model.shift_start_vars[thisShiftKey],
            )

            model.add_constraint(
                shiftCheckingVar
                == model.logical_and(
                    timeCheckingVar, model.shift_assignment_vars[thisShiftKey]
                )
            )

        model.add_constraint(
            model.sum(
                shiftStartCheckingVarList) >= minPeopleWorking, "1.1.MinPeopleWorking"
        )

        model.add_constraint(
            model.sum(
                shiftStartCheckingVarList) <= vacancyQuantiyRequirement, "1.1.MaxObjectTimes"
        )

        # Check Object-time End
        checkedEndTime = objt.DateTo
        shiftEndCheckingVarList = []
        objtimeEndCheckingVarList = []
        for thisShiftKey in getShiftKeys:
            shiftCheckingVar = model.binary_var(
                "ShiftCheckingVar_{0}_{1}".format(thisShiftKey, "END")
            )
            objecttimeCheckingVar = model.binary_var(
                "ObjtimeCheckingVar_{0}_{1}".format(thisShiftKey, "END")
            )
            shiftEndCheckingVarList.append(shiftCheckingVar)
            objtimeEndCheckingVarList.append(objecttimeCheckingVar)

            timeCheckingVar = model.binary_var()
            model.add_equivalence(
                timeCheckingVar, checkedEndTime == model.shift_end_vars[thisShiftKey],
            )
            workingCheckList = [timeCheckingVar]
            for brk in range(0, MAX_BREAK_PER_SHIFT):
                _key = (
                    thisShiftKey[0],
                    thisShiftKey[1],
                    "{0}_br{1}".format(thisShiftKey[2], brk),
                )
                _brk_start = model.break_start_vars[_key]
                _duration = model.break_duration_vars[_key]

                _checkEndBreak_var = model.binary_var()

                model.add_equivalence(
                    _checkEndBreak_var, checkedEndTime >= (
                        _brk_start + _duration + 1)
                )

                workingCheckList.append(_checkEndBreak_var)

            workingCheckList.append(model.shift_assignment_vars[thisShiftKey])

            model.add_constraint(shiftCheckingVar ==
                                 model.logical_and(*workingCheckList))
            model.add_constraint(objecttimeCheckingVar == model.logical_and(
                timeCheckingVar, model.shift_assignment_vars[thisShiftKey]))

        model.add_constraint(
            model.sum(
                shiftEndCheckingVarList) >= minPeopleWorking, "1.2.MinPeopleWorking"
        )
        model.add_constraint(
            model.sum(
                objtimeEndCheckingVarList) <= vacancyQuantiyRequirement, "1.2.MaxObjectTimes"
        )

        for thisShiftKey in getShiftKeys:
            thisShiftStart = model.shift_start_vars[thisShiftKey]
            shiftStartCheckingVarList = []
            objecttimeCheckingVarList = []
            for otherShiftKey in getShiftKeys:
                if (
                    otherShiftKey[:1] == thisShiftKey[:1]
                ):  # not check 2 shifts of the same day of a member
                    continue

                checkStart_var = model.binary_var()
                checkEnd_var = model.binary_var()
                check_shift = model.binary_var()

                model.add_equivalence(
                    checkStart_var,
                    model.shift_start_vars[otherShiftKey] <= thisShiftStart,
                )
                model.add_equivalence(
                    checkEnd_var,
                    thisShiftStart <= model.shift_end_vars[otherShiftKey] - 1,
                )

                model.add_constraint(
                    check_shift
                    == model.logical_and(
                        checkEnd_var, checkStart_var
                    )  # logicAND to check inside shiftRange
                )
                workingCheckList = [check_shift]

                # check with breaks of this shift
                breakCheck = []
                for brk in range(0, MAX_BREAK_PER_SHIFT):
                    _key = (
                        otherShiftKey[0],
                        otherShiftKey[1],
                        "{0}_br{1}".format(otherShiftKey[2], brk),
                    )
                    _brk_start = model.break_start_vars[_key]
                    _duration = model.break_duration_vars[_key]
                    _check_break = model.binary_var()
                    _checkStartBreak_var = model.binary_var()
                    _checkEndBreak_var = model.binary_var()

                    # moment must be outsite break-range
                    model.add_equivalence(
                        _checkStartBreak_var, thisShiftStart <= _brk_start - 1
                    )

                    model.add_equivalence(
                        _checkEndBreak_var, thisShiftStart >= (
                            _brk_start + _duration)
                    )
                    model.add_constraint(
                        _check_break
                        == model.logical_or(
                            _checkStartBreak_var, _checkEndBreak_var
                        )  # # logicOR to check OUTide breakRange
                    )
                    breakCheck.append(_check_break)

                workingCheckList.append(
                    model.shift_assignment_vars[thisShiftKey])
                workingCheckList.append(
                    model.shift_assignment_vars[otherShiftKey])

                shiftCheckingVar = model.binary_var(
                    "ShiftCheckingVar_Start_{0}_{1}".format(
                        thisShiftKey, otherShiftKey)
                )
                objecttimeCheckingVar = model.binary_var(
                    "ObjectTimeCheckingVar_Start_{0}_{1}".format(
                        thisShiftKey, otherShiftKey)
                )
                model.add_equivalence(shiftCheckingVar, model.logical_and(
                    *(workingCheckList+breakCheck)) == 1)
                model.add_equivalence(
                    objecttimeCheckingVar, model.logical_and(*workingCheckList) == 1)

                shiftStartCheckingVarList.append(shiftCheckingVar)
                objecttimeCheckingVarList.append(objecttimeCheckingVar)

            model.add_indicator(
                model.shift_assignment_vars[thisShiftKey],
                model.sum(shiftStartCheckingVarList)
                >= minPeopleWorking
                - 1,  # cause this checker is already a working moment
                name="2.1.MinPeopleWorking",
            )

            model.add_indicator(
                model.shift_assignment_vars[thisShiftKey],
                model.sum(objecttimeCheckingVarList)
                # cause this checker is already a working moment
                <= vacancyQuantiyRequirement - 1,
                name="2.1.MaxObjectTimes",
            )

        # for thisShiftKey in getShiftKeys:
        #     thisShiftEnd = model.shift_end_vars[thisShiftKey]
        #     checkingVarList = []
        #     for otherShiftKey in getShiftKeys:
        #         if (
        #             otherShiftKey[:1] == thisShiftKey[:1]
        #         ):  # not check 2 shifts of the same day of a member
        #             continue
        #         shiftCheckingVar = model.binary_var(
        #             "ShiftCheckingVar_End_{0}_{1}".format(thisShiftKey, otherShiftKey)
        #         )

        #         checkStart_var = model.binary_var()
        #         checkEnd_var = model.binary_var()
        #         check_shift = model.binary_var()

        #         model.add_equivalence(
        #             checkStart_var,
        #             model.shift_start_vars[otherShiftKey] <= thisShiftEnd,
        #         )
        #         model.add_equivalence(
        #             checkEnd_var, thisShiftEnd <= model.shift_end_vars[otherShiftKey] - 1
        #         )

        #         model.add_constraint(
        #             check_shift
        #             == model.logical_and(
        #                 checkEnd_var, checkStart_var
        #             )  # logicAND to check inside shiftRange
        #         )
        #         arr = [check_shift]

        #         # check with breaks of this shift
        #         for brk in range(0, MAX_BREAK_PER_SHIFT):
        #             _key = (
        #                 otherShiftKey[0],
        #                 otherShiftKey[1],
        #                 "{0}_br{1}".format(otherShiftKey[2], brk),
        #             )
        #             _brk_start = model.break_start_vars[_key]
        #             _duration = model.break_duration_vars[_key]
        #             _check_break = model.binary_var()
        #             _checkStartBreak_var = model.binary_var()
        #             _checkEndBreak_var = model.binary_var()

        #             # moment must be outsite break-range
        #             model.add_equivalence(_checkStartBreak_var, thisShiftEnd <= _brk_start - 1)

        #             model.add_equivalence(
        #                 _checkEndBreak_var, thisShiftEnd >= (_brk_start + _duration)
        #             )
        #             model.add_constraint(
        #                 _check_break
        #                 == model.logical_or(
        #                     _checkStartBreak_var, _checkEndBreak_var
        #                 )  # # logicOR to check inside breakRange
        #             )

        #             arr.append(_check_break)

        #         arr.append(model.shift_assignment_vars[thisShiftKey])
        #         arr.append(model.shift_assignment_vars[otherShiftKey])

        #         model.add_equivalence(shiftCheckingVar, model.logical_and(*arr) == 1)

        #         checkingVarList.append(shiftCheckingVar)

        #     model.add_indicator(
        #         model.shift_assignment_vars[thisShiftKey],
        #         model.sum(checkingVarList) >= minPeopleWorking,
        #         name="2.2.MinPeopleWorking",
        #    )

        for thisShiftKey in getShiftKeys:
            ctactId, __, shft = thisShiftKey
            for brk in range(0, MAX_BREAK_PER_SHIFT):
                thisBreakKey = (ctactId, objtId, "{0}_br{1}".format(shft, brk))
                thisBreakStart = model.break_start_vars[thisBreakKey]

                shiftStartCheckingVarList = []
                for otherShiftKey in getShiftKeys:
                    if (
                        otherShiftKey[:1] == thisShiftKey[:1]
                    ):  # not check 2 shifts of the same day of a member
                        continue
                    shiftCheckingVar = model.binary_var(
                        "ShiftCheckingVar_{0}_{1}".format(
                            thisBreakKey, otherShiftKey)
                    )

                    checkStart_var = model.binary_var()
                    checkEnd_var = model.binary_var()
                    check_shift = model.binary_var()

                    model.add_equivalence(
                        checkStart_var,
                        model.shift_start_vars[otherShiftKey] <= thisBreakStart,
                    )
                    model.add_equivalence(
                        checkEnd_var,
                        thisBreakStart <= model.shift_end_vars[otherShiftKey] - 1,
                    )

                    model.add_constraint(
                        check_shift
                        == model.logical_and(
                            checkEnd_var, checkStart_var
                        )  # logicAND to check inside shiftRange
                    )
                    workingCheckList = [check_shift]
                    # check with breaks of this shift
                    for brk in range(0, MAX_BREAK_PER_SHIFT):
                        _key = (
                            otherShiftKey[0],
                            otherShiftKey[1],
                            "{0}_br{1}".format(otherShiftKey[2], brk),
                        )
                        _brk_start = model.break_start_vars[_key]
                        _duration = model.break_duration_vars[_key]

                        _check_break = model.binary_var()
                        _checkStartBreak_var = model.binary_var()
                        _checkEndBreak_var = model.binary_var()

                        # moment must be outsite break-range
                        model.add_equivalence(
                            _checkStartBreak_var, thisBreakStart <= _brk_start - 1
                        )

                        model.add_equivalence(
                            _checkEndBreak_var,
                            thisBreakStart >= (_brk_start + _duration),
                        )
                        model.add_constraint(
                            _check_break
                            == model.logical_or(
                                _checkStartBreak_var, _checkEndBreak_var
                            )  # # logicOR to check OUTside breakRange
                        )
                        workingCheckList.append(_check_break)

                    workingCheckList.append(
                        model.shift_assignment_vars[thisShiftKey])
                    workingCheckList.append(
                        model.shift_assignment_vars[otherShiftKey])

                    model.add_equivalence(
                        shiftCheckingVar, model.logical_and(
                            *workingCheckList) == 1
                    )

                    shiftStartCheckingVarList.append(shiftCheckingVar)

                model.add_constraint(
                    model.if_then(
                        model.break_duration_vars[thisBreakKey] >= 1,
                        model.sum(
                            shiftStartCheckingVarList) >= minPeopleWorking,
                    ),
                    "3.MinPeopleWorking",
                )

        # for thisShiftKey in getShiftKeys:
        #     ctactId, __, shft = thisShiftKey
        #     for brk in range(0, MAX_BREAK_PER_SHIFT):
        #         thisBreakKey = (ctactId, objtId, "{0}_br{1}".format(shft, brk))
        #         thisBreakStart = model.break_start_vars[thisBreakKey]

        #         shiftCheckingVarList = []
        #         for otherShiftKey in getShiftKeys:
        #             if (
        #                 otherShiftKey[:1] == thisShiftKey[:1]
        #             ):  # not check 2 shifts of the same day of a member
        #                 continue
        #             shiftCheckingVar = model.binary_var(
        #                 "ObjectTimeCheckingVar_{0}_{1}".format(thisBreakKey, otherShiftKey)
        #             )

        #             checkStart_var = model.binary_var()
        #             checkEnd_var = model.binary_var()
        #             check_shift = model.binary_var()

        #             model.add_equivalence(
        #                 checkStart_var,
        #                 model.shift_start_vars[otherShiftKey] <= thisBreakStart,
        #             )
        #             model.add_equivalence(
        #                 checkEnd_var,
        #                 thisBreakStart <= model.shift_end_vars[otherShiftKey],
        #             )

        #             model.add_constraint(
        #                 check_shift
        #                 == model.logical_and(
        #                     checkEnd_var, checkStart_var
        #                 )  # logicAND to check inside shiftRange
        #             )
        #             workingCheckList = [check_shift]
        #             workingCheckList.append(model.shift_assignment_vars[thisShiftKey])
        #             workingCheckList.append(model.shift_assignment_vars[otherShiftKey])

        #             model.add_equivalence(
        #                 shiftCheckingVar, model.logical_and(*workingCheckList) == 1
        #             )

        #             shiftCheckingVarList.append(shiftCheckingVar)

        #         model.add_constraint(
        #             model.if_then(
        #                 model.break_duration_vars[thisBreakKey] >= 1,
        #                 model.sum(shiftCheckingVarList) <= vacancyQuantiyRequirement - 1,
        #             ),
        #             "4.MinPeopleWorking",
        #         )

    # model.add_constraint(
    #     len(model.members) * model.average_member_work_time
    #     == model.sum(model.work_time_var[n] for n in model.members),
    #     "AverageWorkTime",
    # )

    # list(
    #     model.add_constraint(
    #         model.work_time_var[ctactId] == model.average_member_work_time
    #         + model.member_over_average_time_vars[ctactId]
    #         - model.member_under_average_time_vars[ctactId],
    #         "AverageWorkTime"
    #     )
    #     for ctactId in model.members.keys()
    # )

    # model.total_salary_cost = model.sum(
    #     (
    #         (model.shift_end_vars[key] - model.shift_start_vars[key])
    #         - model.sum(
    #             model.break_duration_vars[key[0], key[1], "{0}_br{1}".format(key[2], brk)]
    #             for brk in range(0, MAX_BREAK_PER_SHIFT)
    #         )
    #     )
    #     for key in model.shift_assignment_vars.keys()
    # )

    return


def setup_objective(model: Model):
    total_members_assigment = model.sum(model.member_assignment_vars)
    model.add_kpi(total_members_assigment, "Total selected members")
    # model.add_kpi(model.total_salary_cost, "Total salary cost")
    total_shift_assignment = model.sum(model.shift_assignment_vars)
    model.add_kpi(total_shift_assignment, "Total number of assignments")
    # model.add_kpi(model.average_member_work_time, "average work time")

    # total_over_average_worktime = model.sum(
    #     model.member_over_average_time_vars[n] for n in model.members
    # )
    # total_under_average_worktime = model.sum(
    #     model.member_under_average_time_vars[n] for n in model.members
    # )
    # model.add_kpi(total_over_average_worktime, "Total over-average worktime")
    # model.add_kpi(total_under_average_worktime, "Total under-average worktime")
    # total_fairness = total_over_average_worktime + total_under_average_worktime
    # model.add_kpi(total_fairness, "Total fairness")

    model.minimize(
        total_members_assigment
        #     model.total_salary_cost
        #     + total_fairness
        # + total_shift_assignment
    )
    return


def print_information(model: Model):
    print("#member=%d" % len(model.availabilities))
    model.print_information()
    model.report_kpis()


def print_solution(model: Model):
    print("*************************** Solution ***************************")


def solve(model: Model, **kwargs):
    # Here, we set the number of threads for CPLEX to 2 and set the time limit to 2mins.
    model.parameters.threads = 16
    model.parameters.timelimit = 36000  # solver should not take more than that !
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
    mdl = Model("Members", context=context, **kwargs)
    print("Loading data")
    load_data(mdl, excel_data_file, verbose=verbose)
    print("Setting up data")
    setup_data(mdl)
    print("Setting up variable")
    setup_variables(mdl)
    print("Setting up constraint")
    setup_constraints(mdl)
    print("Setting up objectives")
    setup_objective(mdl)
    return mdl


def displayModel(model: Model):
    df_shift = pd.DataFrame()
    df_break = pd.DataFrame()
    # df["ContactId"] = [
    #     key[0] for key,var in model.shift_assignment_vars.items()
    # ]

    print("Self validate: ")
    print(
        "Total members assignments: ",
        len(
            set(
                [
                    key[0]
                    for key, var in model.shift_assignment_vars.items()
                    if model.solution.get_value(var.name) == 1
                ]
            )
        ),
    )
    df_shift["ShiftID"] = [
        "{0}_{1}_{2}".format(key[0], key[1], key[2])
        for key, var in model.shift_assignment_vars.items()
        if model.solution.get_value(var.name) == 1
    ]
    df_break["ShiftID"] = [
        "{0}_{1}_{2}".format(key[0], key[1], key[2].split("_br")[0])
        for key, var in model.break_duration_vars.items()
        if model.solution.get_value(var.name) > 0
    ]

    df_shift["Assigned"] = [
        model.solution.get_value(var.name)
        for key, var in model.shift_assignment_vars.items()
        if model.solution.get_value(var.name) == 1
    ]
    df_break["Start"] = [
        model.num2date(model.solution.get_value(
            model.break_start_vars[key].name))
        for key, var in model.break_duration_vars.items()
        if model.solution.get_value(var.name) > 0
    ]
    df_break["Duration"] = [
        model.solution.get_value(var.name)
        for key, var in model.break_duration_vars.items()
        if model.solution.get_value(var.name) > 0
    ]
    df_shift["ObjectTimeStart"] = [
        model.num2date(model.objecttime_ids[key[1]].DateFrom)
        for key, var in model.shift_assignment_vars.items()
        if model.solution.get_value(var.name) == 1
    ]
    df_shift["ObjectTimeEnd"] = [
        model.num2date(model.objecttime_ids[key[1]].DateTo)
        for key, var in model.shift_assignment_vars.items()
        if model.solution.get_value(var.name) == 1
    ]
    df_shift["StartShift"] = [
        model.num2date(model.solution.get_value(
            model.shift_start_vars[key].name))
        for key, var in model.shift_assignment_vars.items()
        if model.solution.get_value(var.name) == 1
    ]
    df_shift["EndShift"] = [
        model.num2date(model.solution.get_value(
            model.shift_end_vars[key].name))
        for key, var in model.shift_assignment_vars.items()
        if model.solution.get_value(var.name) == 1
    ]
    print(df_shift)
    print(df_break)
    df_shift.to_csv("result.csv")
    # np.savetxt('result.txt', df.values)
    # print(model.solution.get_value("ShiftEnd_92764A21-6456-432B-B126-FA53152BBC3D_76_0"))

    for objtId, objt in model.objecttime_ids.items():
        # check for every moment with offset = 30min
        for moment in range(int(objt.DateFrom), int(objt.DateTo) + 1, 15):
            print(
                model.num2date(moment),
                sum(
                    1
                    for contactId in model.members.keys()
                    for shft in range(0, MAX_SHIFT_PER_OBJECTTIME)
                    if model.solution.get_value(
                        model.shift_assignment_vars[(
                            contactId, objtId, shft)].name
                    )
                    and moment
                    >= model.solution.get_value(
                        model.shift_start_vars[(contactId, objtId, shft)].name
                    )
                    and (
                        moment
                        < model.solution.get_value(
                            model.shift_end_vars[(
                                contactId, objtId, shft)].name
                        )
                        or (
                            moment
                            == model.solution.get_value(
                                model.shift_end_vars[(
                                    contactId, objtId, shft)].name
                            )
                            and moment == objt.DateTo
                        )
                    )
                    and sum(
                        1
                        for brk in range(0, MAX_BREAK_PER_SHIFT)
                        if (
                            moment
                            >= model.solution.get_value(
                                model.break_start_vars[
                                    (contactId, objtId,
                                     "{0}_br{1}".format(shft, brk))
                                ]
                            )
                            and moment
                            < DEFAULT_BREAK_LENGTH
                            + model.solution.get_value(
                                model.break_start_vars[
                                    (contactId, objtId,
                                     "{0}_br{1}".format(shft, brk),)
                                ]
                            )
                        )
                    )
                    == 0
                ),
            )


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
