import time
import json
import pandas as pd
from azure.servicebus import ServiceBusService
sbs = ServiceBusService(
    service_namespace='amldemo',
    shared_access_key_name='RootManageSharedAccessKey',
    shared_access_key_value='4f5O5hLqVLAu7jHkH+l8Lb2+gsB1IB6Pukw+Vo6DDJI=')
# -------------------------------------------------------
# This is the global variables to create some sample data
# --------------------------------------------------------
df = pd.read_csv('https://query.data.world/s/nmz3slczc3c4orpadnuxrqvw54uxd4')
new_df = df.drop(df.columns[0], axis=1)
# --------------------------------------------------------
# Define price ranges

# --------------------------------------------------------

# This will run for about 2.5 hours which should be fine for labs and demos
for y in range(0, 200000):
    row = new_df.sample()
    reading = row.to_dict('records')

    # use json.dumps to convert the dictionary to a JSON format

    s = json.dumps(reading)
    # send to Azure Event Hub
    sbs.send_event("amldemoeh", s)
    print(s)
    # wait 5 seconds
    time.sleep(5)