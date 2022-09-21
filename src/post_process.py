import pandas as pd


a = pd.read_csv("items.csv")
a = a[a["description"].str.contains('7')]
a = a.replace("pd.NA", pd.NA)
a['price'] = pd.to_numeric(a['price'], errors="coerce")
a = a.sort_values(by = "price", na_position="last")
print(a)
a.to_csv("filtered_items.csv")