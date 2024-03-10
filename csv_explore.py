import pandas as pd
import numpy as np

df = pd.read_csv("resource\\btcusdt_all_optimize_reorder_25022024_235900_2.csv")

# df = pd.read_csv("resource\\btcusdt_all_optimize_25022024_235900.csv")
# db_columns = ['time', 'currency', 'close_minmax_scale', 'close', 'ma25_99h', 'ma7_25h', 'ma7_25d', 'ma25_99h_scale', 'ma7_25h_scale', 'ma7_25d_scale']
# df = df[db_columns]

print(df.info())
print(df.head(3))
# print(df.tail(1))
# df.to_csv('resource\\btcusdt_all_noresult_dbcolorder_25022024_235900.csv', index=False, header=True)