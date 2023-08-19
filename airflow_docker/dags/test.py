import pandas as pd

csv_filepath = 'Netflix_dataset.csv'
df = pd.read_csv(csv_filepath, sep=";")

df = df[df['Age'] <= 110]  

print(df)