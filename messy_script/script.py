import pandas as pd
import time
import os

print("STARTING PIPELINE...")

# Extract data
data = pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv")
print("file loaded!")

# drop nans
data2 = data.dropna()
print("nulls removed")

# filter only groups larger than 2 people
data3 = data2[data2["size"] > 2]
print("filtered groups > 2")

# calc per person cost
data3["ppcost"] = data3["total_bill"] / data3["size"]
print("calculated per person cost")

# stats
avg = data3["ppcost"].mean()
mx = data3["ppcost"].max()
mn = data3["ppcost"].min()
print("avg:", avg)
print("max:", mx)
print("min:", mn)

# save file
if not os.path.exists("clean"):
    os.mkdir("clean")
data3.to_csv("clean/out.csv")
print("file saved in clean/out.csv")

# fake timing
time.sleep(2)
print("DONE.")
