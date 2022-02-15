import pandas as pd
def foo1():
    "foo1"
    df = pd.read_csv("Availabilities.csv")
    _df = df.groupby("ContactID")
    _df = _df.first()
    del _df["Unnamed: 0"]
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
    import json
    data = json.loads(open("get_input_data_squirrel.json","r").read())
    for sheet in data:
        file = pd.json_normalize(data[sheet])
        print(file)
        file.to_csv(sheet + ".csv")
        
    
def foo4():
    "foo4"
    
def foo5():
    "fo5"
    
def foo6():
    "foo6"
    
    
if __name__ == "__main__":
    "main"
    foo3()