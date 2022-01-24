# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 13:01:45 2022

@author: Bronwyn
"""

from collections import namedtuple
import datetime
from tqdm import tqdm
import pandas as pd
from docplex.mp.model import Model
from docplex.util.environment import get_environment
from functools import reduce
import numpy as np

TShift = namedtuple("TShift", ["name", "start_hour", "end_hour",
                                "num_breaks","hours","var","work_indicator"])
TBreak = namedtuple(
    "TBreak", ["start_hour","end_hour"])
TPeriod = namedtuple(
    "TPeriod", ["ContactID","shift","period_start","period_end","work_indicator"])
TTeamMemberInfo = namedtuple("TTeamMemberInfo", ["contactid", "gradeid", "ebaid",
                                                 "employment_typeid", "availability", "roster"])
TVar = namedtuple("TVariable", ["ContactID", "time_period",
                                "shift"])
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

file_name = "./Data/Squirrel_Optimization.xlsx"
excel_data_file = pd.ExcelFile(file_name)

MAX_BREAK_PER_SHIFT = 4
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
    def date2num(dt): return int((dt - anchor_date).total_seconds() / 3600)
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
                i if i != "04:00 to 06:00" else TRange(4 * 60, 6 * 60)
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
    
    return

h5 = []
h6 = []
h7 = []
h8 = []
h9 = []
h13 = []
h14 = []
h15 = []
h16 = []

for i in range(7):
    h5.append(5+24*i)
    h6.append(6+24*i)
    h7.append(7+24*i)
    h8.append(8+24*i)
    h9.append(9+24*i)
    h13.append(13+24*i)
    h14.append(14+24*i)
    h15.append(15+24*i)
    h16.append(16+24*i)
df_shift_start = pd.DataFrame([h5,h6,h7,h8,h9,h13,h14,h15,h16]).T

def getshifthours(timefrom,timeto):
    "Create modified availability start and end time based on open and close hours"
    shiftopen = [(5+24*i,22+24*i) for i in range(7)]
    for i in shiftopen:
        if timefrom in range(i[0],i[1]) and timeto in range(i[0],i[1]):
            avail_start = timefrom
            avail_end = timeto
            return (avail_start,avail_end)
        if timefrom in range(i[0],i[1]):
            return (timefrom,i[1])
        if timeto in range(i[0],i[1]):
            return (i[0],timeto)
        if timefrom <= i[0] and timeto >= i[1]:
            return (i[0],i[1])
        
VAR = []    
def setup_data(model):
    temp = []
    for avail in model.availabilities[:]:
        avail_start,avail_end = getshifthours(avail.TimeFrom,avail.TimeTo)
        #print(avail_start,avail_end)
        shifts = create_shift(model,avail_start,avail_end)
        temp.append((avail.ContactID,(avail_start,avail_end),shifts))
    VAR.extend([TVar(*v) for v in temp])
    return

def get_shift_length(start_time,end_time):
    "Create shift length possiblities based on different start times and end time"
    shift_len = [4,8,10]
    shift_len_list = []
    for sl in shift_len:
        if start_time + sl <= end_time:
            shift_len_list.append(sl)
    return shift_len_list
     
def get_num_breaks(shift_len):
    "get number of breaks by shift length"
    if shift_len == 4:
        return 0
    if shift_len == 8:
        return 1
    if shift_len == 10:
        return 2

def create_shift(model:Model,start_time,end_time):
    "creates shifts based on different starttimes and lengths based on availability"
    SHIFTS = []
    for shift_start in range(start_time,end_time):
        for i in range(9):
            for j in range(7):
                if (shift_start/df_shift_start[i][j]) == 1:
                    # print("shift start: ",shift_start,"shift lengths",shift_len_list)
                    shift_len_list = get_shift_length(shift_start,end_time)
                    for shift_len in shift_len_list:
                        var = model.binary_var()
                        work_indicator = model.binary_var()
                        SHIFTS.append(("{0} hour shift".format(4),shift_start,
                                       shift_start+shift_len,get_num_breaks(shift_len),
                                       shift_len,var,work_indicator))
    shifts = [TShift(*rs) for rs in SHIFTS]
    return shifts
           
def get_shift_periods(shift_len):
    "get number of periods for each shift based on shift length"
    if shift_len == 4:
        return 1
    if shift_len == 8:
        return 3
    if shift_len == 10:
        return 5

             
def setup_variables(model:Model):
    k = []
    for v in VAR:
        for sh in v.shift:
            num_periods = get_shift_periods(sh.hours)
            for period in range(num_periods):
                p_start = model.integer_var()
                p_end = model.integer_var()
                k.append((v.ContactID,sh,p_start,p_end,sh.work_indicator))
    model.periods = [TPeriod(*p) for p in k]
    return 

def setup_constraints(model:Model):                    
    
    for v in VAR:
        shifts = v.shift
        model.add_constraint(model.sum(shift.var for shift in shifts) <= 1)
        for sh in v.shift:
            total_break_time = sh.num_breaks*DEFAULT_BREAK_LENGTH
            total_work_time = sh.hours - total_break_time
            work_indicator = sh.work_indicator
            model.add_constraint(work_indicator<=sh.var)
            for t in range(sh.hours):
                if t <= 4:
                    model.add_constraint(work_indicator == sh.var)
                if t in range(6,9):
                    model.add_constraint(work_indicator == sh.var)
            period = [p for p in model.periods if p.shift == sh]
            if sh.hours != 4:
                for p in period:
                    model.add_indicator(work_indicator, p.period_end - p.period_start == 0.5, 0)
            model.add_indicator(work_indicator,model.sum(p.period_end - p.period_start for p in period)
                                == total_work_time, 1)
            model.add_indicator(work_indicator,model.sum(p.period_end - p.period_start for p in period)
                                == total_break_time, 0)
        
            
    print("Finished allocating breaks constraint")
    minPeopleWorking = model.shift_constraints.MinPeopleWorking
    vacancyQuantiyRequirement = model.vacancy_detail.Quantity
    vacancyQuantiyRequirement = 12
    minPeopleWorking = 5
    for objtId, objt in tqdm(model.objecttime_ids.items()):
        # check for every moment with offset = 30min
        for moment in range(
            int(objt.DateFrom), int(objt.DateTo) + 1, 15
        ): 
            var_list = [p for p in model.periods if moment in range(p.shift.start_hour,p.shift.end_hour)]
            on_floor_var = model.sum()

    print("Added vacancy filling constraints")
    
def setup_objective(model: Model):

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
    model.parameters.timelimit = 3600  # solver should not take more than that !
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
    ""


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
