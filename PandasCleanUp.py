import pandas as pd 
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', lambda x: '%.5f' % x)
df=pd.read_csv ('./csv/13052021_172649.csv')

df[6] = df[6].astype(str)

print (df.to_string())