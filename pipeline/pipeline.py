import sys
import pandas as pd
args=sys.argv

print(f"Argument: {args}")

df=pd.DataFrame({'col a': [1,2,3], 'col b':['a','b','c']})
print(df)

df.to_parquet('output.parquet')