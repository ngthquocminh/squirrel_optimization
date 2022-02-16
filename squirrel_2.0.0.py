# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 13:18:22 2022

@author: Bronwyn
"""

#

from collections import namedtuple
import datetime
from tqdm import tqdm
import pandas as pd
from docplex.mp.model import Model
from docplex.util.environment import get_environment
from functools import reduce
import numpy as np
import uuid
import math
from datetime import timedelta
import sys
import time
import json
from pandas import json_normalize


class ModelObjects:
    def __init__(self, data, model):
        self.json_input = data
        self.model = model

    def create_namedtuples(self):
        self.model.TShift = namedtuple(
            "TShift",
            ["name", "objecttimeid", "start_hour", "end_hour", "num_breaks", "hours", "var"])
        self.model.TBreak = namedtuple(
            "TBreak", ["start_hour", "end_hour"])
        self.model.TPeriod = namedtuple(
            "TPeriod",
            ["contactid", "shift", "period_start", "period_end", "work_indicator", "break1_indicator", "break2_indicator"])
        self.model.TTeamMemberInfo = namedtuple(
            "TTeamMemberInfo",
            ["contactid", "gradeid", "ebaid", "employment_typeid", "availability", "roster"])

        self.model.TVar = namedtuple(
            "TVariable",
            ["contactid", "time_period", "shift"])

        self.model.TMemberAvailability = namedtuple(
            "TMemberAvailability",
            ["contactid", "team_member", "start_hour", "end_hour"]
        )
        self.model.TMemberWorksitePreference = namedtuple(
            "TMemberWorksiteReference",
            ["contactid", "team_member", "Worksite"]
        )
        self.model.TVacancyObjectTime = namedtuple(
            "TVacancyObjectTime",
            ["vacancyid", "objecttimeid", "start_hour",
                "end_hour", "positionid", "worksiteid"]
        )

        self.model.TShiftConstraints = namedtuple(
            "TShiftContraints",
            [
                "MinPeopleWorking",
                "MaxHoursPerDay",
                "MaxHoursPerWeek",
                "MaxConsecutiveShift",
                "MinBreakBetweenShift",
                "MinHoursPerShift",
                "MaxPeopleWorking"
            ],
        )
        self.model.TTeamMemberQual = namedtuple(
            "TTeamMemberQualifications",
            ['contactid', 'team_member', 'measurementid'])

        self.model.TVacancyDetails = namedtuple(
            "TVacancyDetails",
            ['vacancyid', 'StartDateTime', 'EndDateTime', 'quantity', 'minquantity', 'worksiteid', 'posisitonid', 'measurementid'])

    def read_input_data(self):
        file = open(self.json_input, "r")
        contents = file. read()
        file. close()
        self.input_json = json.loads(contents)

    def load_input_data(self):
        self.model.df_vacancy_details = json_normalize(
            self.input_json['Vacancy_details'])
        self.model.vacancy_objecttime = json_normalize(
            self.input_json['Vacancy_ObjectTime'])
        self.model.df_teamMember_availability = json_normalize(
            self.input_json['Team_Member_Availability'])
        self.model.teamMember_qual = json_normalize(
            self.input_json['Team_Member_Qualifications'])
        self.model.df_shift_constraints = json_normalize(
            self.input_json['Shift_Constraints'])
        self.model.df_worksite_reference = json_normalize(
            self.input_json['Worksite_Preferences'])
        self.model.tm_shift_pref = json_normalize(
            self.input_json['Team_Member_Shift_Preference'])
        self.model.anchor_date = pd.to_datetime(
            self.model.df_vacancy_details.StartDateTime).min()
        # convert to datetime
        self.model.df_vacancy_details['StartDateTime'] = pd.to_datetime(
            self.model.df_vacancy_details['StartDateTime'])
        self.model.df_vacancy_details['EndDateTime'] = pd.to_datetime(
            self.model.df_vacancy_details['EndDateTime'])
        self.model.vacancy_objecttime['StartDateTime'] = pd.to_datetime(
            self.model.vacancy_objecttime['StartDateTime'])
        self.model.vacancy_objecttime['EndDateTime'] = pd.to_datetime(
            self.model.vacancy_objecttime['EndDateTime'])
        self.model.df_teamMember_availability['StartDateTime'] = pd.to_datetime(
            self.model.df_teamMember_availability['StartDateTime'])
        self.model.df_teamMember_availability['EndDateTime'] = pd.to_datetime(
            self.model.df_teamMember_availability['EndDateTime'])
        # convert to num
        self.convert_time_to_num(
            self.model.df_vacancy_details, 'StartDateTime')
        self.convert_time_to_num(self.model.df_vacancy_details, 'EndDateTime')
        self.convert_time_to_num(
            self.model.df_teamMember_availability, 'StartDateTime')
        self.convert_time_to_num(
            self.model.df_teamMember_availability, 'EndDateTime')
        self.convert_time_to_num(
            self.model.vacancy_objecttime, 'StartDateTime')
        self.convert_time_to_num(self.model.vacancy_objecttime, 'EndDateTime')

    def load_model_objects(self):
        for i in range(len(self.model.df_teamMember_availability)):
            if self.model.df_teamMember_availability.loc[i, 'StartDateTime'] == self.model.df_teamMember_availability.loc[i, 'EndDateTime']:
                self.model.df_teamMember_availability.loc[i,
                                                          'EndDateTime'] = self.model.df_teamMember_availability.loc[i, 'StartDateTime'] + 24

        MEM_AVAILAVILITY = [
            self.model.TMemberAvailability(*row) for _, row in self.model.df_teamMember_availability.iterrows()
        ]
        self.model.availabilities = MEM_AVAILAVILITY[:]

        VACANCY_OBJECTTIME = [
            self.model.TVacancyObjectTime(*row) for _, row in self.model.vacancy_objecttime.iterrows()]
        self.model.vacancy_objecttimes = VACANCY_OBJECTTIME[:1]

        VACANCY_DETAILS = [
            self.model.TVacancyDetails(*row) for _, row in self.model.df_vacancy_details.iterrows()
        ]
        self.model.vacancy_details = VACANCY_DETAILS
        self.model.get_vacancyid_details = {v.vacancyid: (
            v.quantity, v.minquantity) for v in self.model.vacancy_details}

        SHIFT_CONSTRAINTS = [
            self.model.TShiftConstraints(*row) for _, row in self.model.df_shift_constraints.iterrows()]
        self.model.shift_constraints = SHIFT_CONSTRAINTS

        MEM_WORKSITE_REFERENCE = [
            self.model.TMemberWorksitePreference(*row)
            for _, row in self.model.df_worksite_reference.iterrows()
        ]
        self.model.worksite_refs = MEM_WORKSITE_REFERENCE

        MEM_QUAL = [
            self.model.TTeamMemberQual(*row) for _, row in self.model.teamMember_qual.iterrows()]
        self.model.tm_qual = MEM_QUAL

    def date2num(self, dt):
        return int((dt - self.model.anchor_date).total_seconds() / 3600)

    def convert_time_to_num(self, df, col):
        df.loc[:, col] = list(map(self.date2num, df[col]))

    def init_starttimes(self):
        "Initailizing starttimes for shifts"
        start_times = [5,6,7,8,9,10,13,14,15,16]
        self.model.df_shift_start = pd.DataFrame()
        for i,st in enumerate(start_times):
            arr = []
            for mins in range(60):
                arr.append(st + st*mins)
            self.model.df_shift_start[i] = arr


    def convert_to_datetime(self, hour):
        "Convert hour(int) to datetime based on anchor date"
        beginning_date = self.model.anchor_date
        i = math.floor(hour / 24)
        j = hour - 24 * i
        date = beginning_date + timedelta(days=i)
        date_time = date + timedelta(hours=j)
        return date_time


class SetupData(ModelObjects):

    def __init__(self, data, model):
        self.json_input = data
        self.model = model
        self.create_namedtuples()
        self.read_input_data()
        self.load_input_data()
        self.load_model_objects()
        self.init_starttimes()
        self.model.VAR = []
        self.model.periods = []
        self.shift_len_data = {
            4: {"NBreaks": 0, "NPeriods": 1},
            6: {"NBreaks": 1, "NPeriods": 3},
            8: {"NBreaks": 1, "NPeriods": 3},
            10: {"NBreaks": 2, "NPeriods": 5}
        }

    def getshifthours(self, contactid, timefrom, timeto):
        "Create modified availability start and end time based on open and close hours"
        shiftopen = [(v.start_hour, v.end_hour, v.objecttimeid)
                     for v in self.model.vacancy_objecttimes]
        shifthours = []
        for objecttime in shiftopen:
            start, end, id = objecttime

            if timefrom in range(start, end) and timeto in range(start, end):
                shifthours.append((contactid, timefrom, timeto, id))
            elif timefrom in range(start, end):
                shifthours.append((contactid, timefrom, end, id))
            elif timeto in range(start, end):
                shifthours.append((contactid, start, timeto, id))
            elif start in range(timefrom, timeto) and end in range(timefrom, timeto):
                shifthours.append((contactid, start, end, id))
            else:
                pass
        return shifthours

    def check_shift_in_range(self, sh_start, object_time_id):
        "Checks if start and end time exists between range of open and close times"
        for vacancy in self.model.vacancy_objecttimes:
            if vacancy.objecttimeid == object_time_id:
                starthour = vacancy.start_hour
                endhour = vacancy.end_hour
                time_diff = vacancy.end_hour - sh_start
                for sl in self.shift_len_data:
                    if sl <= time_diff:
                        var = self.model.binary_var()
                        return (
                            "{0} hour shift".format(sl), 
                            object_time_id, 
                            sh_start,
                            sh_start + sl, 
                            self.get_num_breaks(sl),
                            sl, 
                            var
                        )

    def get_shift_length(self, start_time, end_time):
        "Create shift length possiblities based on different start times and end time"
        shift_len_list = []
        for sl in self.shift_len_data:
            if start_time + sl <= end_time:
                shift_len_list.append(sl)
        return shift_len_list

    def get_num_breaks(self, shift_len):
        "get number of breaks by shift length"
        return self.shift_len_data[shift_len]["NBreaks"] if shift_len in self.shift_len_data else 0

    def create_shift(self, shifts):
        "creates shifts based on different starttimes and lengths based on availability"
        self.SHIFTS = []
        if len(shifts) > 0:
            for shift in shifts:
                start_time = shift[1]
                end_time = shift[2]
                objecttimeid = shift[3]
                for shift_start in range(start_time, end_time):
                    if self.model.df_shift_start.isin([shift_start]).sum().sum() > 0:
                        _shift = self.check_shift_in_range(shift_start, objecttimeid)
                        if _shift != None:
                            self.SHIFTS.append(_shift)
        shifts_obj = [self.model.TShift(*rs) for rs in self.SHIFTS]
        return shifts_obj

    def get_shift_periods(self, shift_len):
        "get number of periods for each shift based on shift length"
        return self.shift_len_data[shift_len]["NPeriods"] if shift_len in self.shift_len_data else 0

    def setup_data(self):
        "Setting up shifts to be allocated based on availabilities"
        temp = []
        for avail in self.model.availabilities:
            # print(avail.start_hour,avail.end_hour)
            shift_list = self.getshifthours(
                avail.contactid, avail.start_hour, avail.end_hour)
            # print(avail_start,avail_end)
            shifts = self.create_shift(shift_list)
            temp = (avail.contactid, avail, shifts)
            self.model.VAR.append(self.model.TVar(*temp))

        # Creating objects for work periods and break periods
        k = []
        for v in self.model.VAR:
            for sh in v.shift:
                num_periods = self.get_shift_periods(sh.hours)
                for period in range(num_periods):
                    work_indicator = self.model.binary_var()
                    break1_indicator = self.model.binary_var()
                    break2_indicator = self.model.binary_var()
                    p_start = self.model.continuous_var()
                    p_end = self.model.continuous_var()
                    k = (v.contactid, sh, p_start, p_end, work_indicator,
                         break1_indicator, break2_indicator)
                    self.model.periods.append(self.model.TPeriod(*k))

        return


class SetupConstraints(ModelObjects):

    def __init__(self, data, model):
        self.input_json = data
        self.model = model
        self.model.day_limit = 10

    def shift_assign_constraint(self):
        for v in tqdm(self.model.VAR):
            shifts = v.shift
            self.model.add_constraint(self.model.sum(
                shift.var*(shift.end_hour - shift.start_hour) for shift in shifts) <= self.model.day_limit)
            for sh in v.shift:
                periods_shift = [p for p in self.model.periods if p.contactid == v.contactid and
                                 p.shift is sh]
                # Break indicator constraints based on shift hours
                if sh.hours == 4:
                    self.model.add_constraint(self.model.sum(
                        p.break1_indicator for p in periods_shift) == 0*sh.var)
                    self.model.add_constraint(self.model.sum(
                        p.break2_indicator for p in periods_shift) == 0*sh.var)
                if sh.hours == 8:
                    self.model.add_constraint(self.model.sum(
                        p.break1_indicator for p in periods_shift) == 1*sh.var)
                    self.model.add_constraint(self.model.sum(
                        p.break2_indicator for p in periods_shift) == 0*sh.var)
                if sh.hours == 10:
                    self.model.add_constraint(self.model.sum(
                        p.break1_indicator for p in periods_shift) == 1*sh.var)
                    self.model.add_constraint(self.model.sum(
                        p.break2_indicator for p in periods_shift) == 1*sh.var)

                for period in periods_shift:
                    # period end > period start constraint
                    self.model.add_constraint(
                        period.period_end >= period.period_start)
                    # period bound constraints
                    self.model.add_constraint(
                        period.period_start >= sh.start_hour*sh.var)
                    self.model.add_constraint(
                        period.period_start <= sh.end_hour*sh.var)
                    self.model.add_constraint(
                        period.period_end >= sh.start_hour*sh.var)
                    self.model.add_constraint(
                        period.period_end <= sh.end_hour*sh.var)
                    # Indicator bound constraints
                    self.model.add_constraint(period.work_indicator <= sh.var)
                    self.model.add_constraint(
                        period.break1_indicator <= sh.var)
                    self.model.add_constraint(
                        period.break2_indicator <= sh.var)
                    self.model.add_constraint(period.work_indicator + period.break1_indicator + period.break2_indicator
                                              == sh.var)

                    # Workhour and breakhour constraints
                    self.model.add_indicator(
                        period.break1_indicator, period.period_start - sh.var*sh.start_hour >= 4*sh.var, 1)
                    self.model.add_indicator(
                        period.break1_indicator, period.period_start - sh.var*sh.start_hour <= 6*sh.var, 1)
                    self.model.add_indicator(
                        period.break2_indicator, period.period_start - sh.var*sh.start_hour >= 8*sh.var, 1)
                    self.model.add_indicator(
                        period.break1_indicator, period.period_end - period.period_start == 0.5, 1)
                    self.model.add_indicator(
                        period.break2_indicator, period.period_end - period.period_start == 0.5, 1)

                # Applying totalhours constraint
                self.model.add_constraint(self.model.sum(
                    p.period_end - p.period_start for p in periods_shift) == sh.hours*sh.var)

                # Applying non overlapping periods constraint
                for i in range(len(periods_shift)-1):
                    self.model.add_constraint(
                        periods_shift[i].period_end == periods_shift[i+1].period_start)

                # Constraint setting start and end time for periods
                self.model.add_constraint(
                    periods_shift[0].period_start == sh.start_hour*sh.var)
                self.model.add_constraint(
                    periods_shift[len(periods_shift)-1].period_end == sh.end_hour*sh.var)
        return

    def vacancy_filling_constraint(self):
        self.model.total_slack_members = list()
        self.model.on_floor_members_time = list()
        for vacancy in self.model.vacancy_objecttimes:
            minQuantity = self.model.get_vacancyid_details[vacancy.vacancyid][0]
            maxQuantity = self.model.get_vacancyid_details[vacancy.vacancyid][1]
            # print(minQuantity,maxQuantity)
            for h in np.arange(vacancy.start_hour, vacancy.end_hour, 0.25):
                on_floor_sum = list()
                slack_members = self.model.integer_var()
                var_list = [
                    p for p in self.model.periods if p.shift.start_hour <= h <= p.shift.end_hour]
                for p in var_list:
                    ind_var = self.model.binary_var()
                    self.model.add_constraint(ind_var <= p.work_indicator)
                    self.model.add_indicator(ind_var, p.period_start <= h, 1)
                    self.model.add_indicator(ind_var, p.period_end >= h, 1)
                    on_floor_sum.append(ind_var)

                # Constraint to set minimum and maximum limit on on floor members
                on_floor_var = self.model.sum(
                    indicator for indicator in on_floor_sum)
                self.model.add_constraint(on_floor_var + slack_members >= minQuantity,
                                          "Minimum {0} members on floor at any time".format(minQuantity))
                self.model.add_constraint(
                    on_floor_var <= maxQuantity, "Max {0} members on floor at any time".format(maxQuantity))
                self.model.total_slack_members.append(slack_members)
                self.model.on_floor_members_time.append(on_floor_var)

        return

    def add_constraints(self):
        "Adding model constraints"
        self.shift_assign_constraint()
        print("Finished allocating breaks constraint")
        self.vacancy_filling_constraint()
        print("Added vacancy filling constraints")


class SetupObjectives(ModelObjects):

    def __init__(self, data, model):
        self.input_json = data
        self.model = model

    def setup_objectives(self):
        "Setting up objectives and KPIs"
        self.model.total_shifts = self.model.sum(
            sh.var for v in self.model.VAR for sh in v.shift)
        self.model.unfilled_members = self.model.sum(
            self.model.total_slack_members)
        # KPIs
        self.model.add_kpi(self.model.total_shifts, "Total shifts assigned")
        self.model.add_kpi(self.model.unfilled_members,
                           "Total unfilled members across the week")
        # Minimization function
        self.model.minimize(self.model.unfilled_members)
        print("Done setting up objectives")
        return


class ModelSolve(ModelObjects):

    def __init__(self, data, model):
        self.input_json = data
        self.model = model

    def solve_model(self):
        print("Solving the model...\n")
        self.model.parameters.mip.tolerances.mipgap.set(1e-01)
        self.model.parameters.multiobjective.display.set(1)
        solve = self.model.solve()
        print("Model solve complete")
        return solve


class CreateResults(ModelObjects):

    def __init__(self, data, model, solution):
        self.input_json = data
        self.model = model
        self.solution = solution

    def create_results(self):
        if self.solution:
            self.model.report_kpis(solution=self.solution)
            self.create_hourly_profile()
            self.create_shifts()
            self.write_json()

    def create_hourly_profile(self):
        # Creating results for hourly profile
        i = 0
        hourly_profile = list()
        for vacancy in self.model.vacancy_objecttimes:
            for h in np.arange(vacancy.start_hour, vacancy.end_hour, 0.25):
                hourly_profile.append((self.convert_to_datetime(h), vacancy.vacancyid, vacancy.objecttimeid, self.model.total_slack_members[i].solution_value,
                                       self.model.on_floor_members_time[i].solution_value))
                i += 1

        self.hourly_profile_df = pd.DataFrame(hourly_profile, columns=[
                                              'Time', 'VacancyID', 'ObjectTimeID', 'Unfilled_Members', 'Members_on_floor'])

    def create_shifts(self):
        # Creating Shifts
        k = []
        for v in self.model.VAR:
            for sh in v.shift:
                shift_assigned = sh.var.solution_value
                if shift_assigned > 1e-8:
                    shift_uid = str(uuid.uuid4())
                    periods_shift1 = [p for p in self.model.periods if p.contactid == v.contactid and
                                      p.shift is sh]
                    for p in periods_shift1:
                        period_uid = str(uuid.uuid4())
                        k.append((v.contactid, shift_uid, sh.objecttimeid, self.convert_to_datetime(sh.start_hour), self.convert_to_datetime(sh.end_hour),
                                  self.convert_to_datetime(p.period_start.solution_value), self.convert_to_datetime(
                                      p.period_end.solution_value),
                                  p.work_indicator.solution_value, p.break1_indicator.solution_value,
                                  p.break2_indicator.solution_value, period_uid))

        self.shifts = pd.DataFrame(k, columns=['ContactID', 'ShiftID', 'ObjectTimeID', 'Shift_Start', 'Shift_End', 'Period_Start',
                                               'Period_End', 'Work_Indicator', 'Break1', 'Break2', 'Shift_Detail_ID'])
        self.shifts['Shift_Start'] = self.shifts['Shift_Start'].astype(str)
        self.shifts['Shift_End'] = self.shifts['Shift_End'].astype(str)
        self.shifts['Period_Start'] = self.shifts['Period_Start'].astype(str)
        self.shifts['Period_End'] = self.shifts['Period_End'].astype(str)

        for i in range(len(self.shifts)):
            self.shifts.loc[i, 'Period_Start'] = self.shifts.loc[i, 'Period_Start'].split(".", 1)[
                0]
            self.shifts.loc[i, 'Period_End'] = self.shifts.loc[i, 'Period_End'].split(".", 1)[
                0]

    def write_json(self):
        # Creating output sample
        self.hourly_profile_df['Time'] = self.hourly_profile_df['Time'].astype(
            str)
        # Build sample outputs
        self.hourly_profile_df.name = 'Hourly_Profile'
        self.shifts.name = 'Shift_Details'
        self.shifts.to_csv('Squirrel_Shifts.csv')
        self.hourly_profile_df.to_csv('Hourly_Profile.csv')
        k = []
        output_list = [self.hourly_profile_df, self.shifts]
        for df in output_list:
            temp_dict = {}
            #temp_df = df.iloc[:1]
            temp_dict[df.name] = df.to_dict(orient='records')
            k.append(temp_dict)

        import json
        with open("output_squirrel.json", 'w') as outfile:
            outfile.write(json.dumps(k))


class ModelBuild:

    def __init__(self, data, scenario_id):
        self.model = Model()
        self.model.start_time = time.time()
        self.data = data
        self.scenario_id = scenario_id

    def create_model_run(self):
        self.Setup_Data = SetupData(self.data, self.model)
        self.Setup_Data.setup_data()
        print("1. Setting up Data: Done!")
        self.Setup_Constraints = SetupConstraints(self.data, self.model)
        self.Setup_Constraints.add_constraints()

        print("2. Setting up Constraints: Done!")
        self.Setup_Objectives = SetupObjectives(self.data, self.model)
        self.Setup_Objectives.setup_objectives()

        print("3. Setting up Objectives: Done!")
        self.Model_Solve = ModelSolve(self.data, self.model)
        self.solution = self.Model_Solve.solve_model()

        print("4. Model solving: Done!")
        self.Create_Results = CreateResults(
            self.data, self.model, self.solution)
        self.Create_Results.create_results()


if __name__ == "__main__":
    "MAIN"
    mb = ModelBuild("get_input_data_squirrel.json", Model())
    mb.create_model_run()
