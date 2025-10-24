import requests
import pandas as pd

try:
    response = requests.get("http://api.openweathermap.org/geo/1.0/reverse?lat=56.075984&lon=12.512430&appid=3db8fc6d42877ba151fe5f6ba8849b0b")
    response.raise_for_status()  # Vil kaste en HTTPError for dårlige statuskoder
    data = response.json()
except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except Exception as err:
    print(f"An error occurred: {err}")

#df = display(data)

# Konverter data til en DataFrame
df = pd.DataFrame(data)
