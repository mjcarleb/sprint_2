import asyncio
from pyzeebe import ZeebeClient, create_camunda_cloud_channel
import pandas as pd

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="~Ri8C.ORXfo-aic-ZsnnTzaQG-YWvJwW",
    client_secret="WYn8cxBNHx3knm74WahUXsotBzVweHdgJ45WEW00z0ig7CN5dZszdLxeloS.eGxC",
    cluster_id="f0e81eb1-248d-43cf-a660-ee4b4662968c",
    region="bru-2"
)
# Create single threaded worker
client = ZeebeClient(channel)


async def run_trade_match_batch(bpmn_process_id, df):

    for i, (idx, trade) in enumerate(df.iterrows()):
        if i == 10:
            break
        else:
            await client.run_process(bpmn_process_id=bpmn_process_id,
                                     variables=trade.to_dict())

bpmn_process_id = "Process_cdd8ac1b-a3e5-4467-8061-784691625fe2"

loop = asyncio.get_event_loop()

# Get trades from firm trade file
data_dir = "data/"
df = pd.read_parquet(path=f"{data_dir}ideal_trades")

try:
    results = loop.run_until_complete(run_trade_match_batch(bpmn_process_id, df))
finally:
    loop.stop()
    loop.close()







