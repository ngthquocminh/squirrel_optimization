from datetime import datetime
import json
import random
import pandas as pd


def foo1():
    "foo1"
    df = pd.read_csv("Availabilities.csv",index_col=0)
    _df = df.groupby("ContactID")
    _df = _df.first()
    del _df["TimeFrom"]
    del _df["TimeTo"]
    _df["Measurement"] = ["Certificate I In Health Support"] * _df.shape[0]
    print(_df)
    _df.to_csv("MembersMeasurement.csv")
    
def foo2():
    "foo2"
    df = pd.read_csv("MembersMeasurement.csv")
    print(len(set(df["ContactID"])))
    
def foo3():
    "foo3"
    data = json.loads(open("get_input_data_squirrel.json","r").read())
    for sheet in data:
        file = pd.json_normalize(data[sheet])
        print(file)
        file.to_csv(sheet + ".csv")
        
    
def foo4():
    "create new Team_Member_Availability data"
    df_objtime = pd.read_csv('Vacancy_ObjectTime.csv', index_col=0)
    df_qual = pd.read_csv('Team_Member_Qualifications.csv', index_col=0)
    df_qual = df_qual.groupby("ContactID").first()
    del df_qual["MeasurementID"]
    df_qual = df_qual.reset_index()
    print(df_qual)
    df = pd.merge(df_qual,df_objtime, how='cross')
    print(df)
    
    df = df[:][[random.choice([1,2,3,4,5,6,7])==1 for i in range(df.shape[0])]]
    del df["VacancyID"]
    del df["ObjectTimeID"]
    del df["PositionID"]
    del df["WorksiteID"]
    
    print(df)
    t_formart = '%Y-%m-%dT%H:%M:%S'
    df = df.reset_index(drop=True)

    for i,row in df.iterrows():
        t1 = datetime.strptime(row['StartDateTime'], t_formart)
        t1 = t1.replace(hour=1, minute=0, second=0)
        df.loc[i,'StartDateTime'] = t1
    df = df.groupby(["StartDateTime","ContactID"]).first().reset_index()
    print(df)
    df['EndDateTime'] = [None]*df.shape[0]
    df = df[["ContactID","TeamMember","StartDateTime","EndDateTime"]]
    for i,row in df.iterrows():
        t = row["StartDateTime"]
        t1 = t.replace(hour=random.choice([1,1,1,3,3,3,4,4,5,6]), minute=0, second=0)
        df.loc[i,'StartDateTime'] = t1
        # print(t1)
        t2 = t.replace(hour=random.choice([15,16,17,18,19,20,20,20,20,21,22,22,22,23,23,23,23]), minute=0, second=0)
        df.loc[i,'EndDateTime'] = t2
        
 
    print(df)
    df.to_csv("Team_Member_Availability_xx.csv")
    
def foo5():
    "fo5"
    df1 = pd.DataFrame({"x1":[1,2],"x2":["x1","x2"]})
    df2 = pd.DataFrame({"y1":["a","b"],"y2":["_","_"]})
    print(pd.merge(df1,df2, how='cross'))
    
def foo6():
    "foo6"
    json_file = open("get_input_data_squirrel_bk.json","r")
    dict_data = json.loads(json_file.read())
    json_file.close()
    
    # print(dict_data["Team_Member_Availability"])
    avai_data = pd.read_csv("Team_Member_Availability_xx.csv",index_col=0)
    print(avai_data)
    avai_data = avai_data.to_dict('records')
    t_formart = '%Y-%m-%dT%H:%M:%S'
    for row in avai_data:
        row["StartDateTime"] = datetime.strptime(row['StartDateTime'],'%Y-%m-%d %H:%M:%S').strftime(t_formart)
        row["EndDateTime"] = datetime.strptime(row['EndDateTime'],'%Y-%m-%d %H:%M:%S').strftime(t_formart)
    # print(avai_data)
    dict_data["Team_Member_Availability"] = avai_data
    
    new_json_file = open("get_input_data_squirrel.json","w")
    new_json_file.write(json.dumps(dict_data,indent=5))
    new_json_file.close()    
    # dict_data["Team_Member_Availability"] = 
    
    
if __name__ == "__main__":
    "main"
    foo6()