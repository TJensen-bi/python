import requests
import pandas as pd
from IPython.display import display

try:
    response = requests.get("http://api.openweathermap.org/geo/1.0/reverse?lat=56.075984&lon=12.512430&appid=3db8fc6d42877ba151fe5f6ba8849b0b")
    response.raise_for_status()  # Vil kaste en HTTPError for dårlige statuskoder
    data = response.json()
except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except Exception as err:
    print(f"An error occurred: {err}")

# Konverter data til en DataFrame
df = pd.DataFrame(data)

# Udtræk specifikke nøgler fra 'local_names'
keys_to_extract = ['da', 'fi']  # Eksempel på nøgler du vil udtrække
for key in keys_to_extract:
    df[key] = df['local_names'].apply(lambda x: x.get(key) if isinstance(x, dict) else None)


# Udflad 'Local_names' kolonnen
#if 'local_names' in df.columns:
#    local_names_df = df['local_names'].apply(pd.Series)
#    df = pd.concat([df.drop('local_names', axis=1), local_names_df], axis=1)


# Fjern 'local_names' kolonnen, hvis du ikke længere har brug for den
df.drop('local_names', axis=1, inplace=True)


display(df)
