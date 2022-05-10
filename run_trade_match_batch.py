import asyncio
from pyzeebe import ZeebeClient, create_camunda_cloud_channel
import pandas as pd

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="zR0s.fLdJBnPHX.J1S8leJMS0dv4H2LI",
    client_secret="3h7kspi_x4ic28t-y0DtP8P8Ep-jfBn3wVtM792tcYXTgz~fgL-G-m0u4ZkETSKO",
    cluster_id="b58c17d6-f7f4-44fe-8f30-98f0bc0c4ef8",
    region="bru-2"
)
# Create single threaded worker
client = ZeebeClient(channel)


async def run_trade_match_batch(bpmn_process_id, df):

    for i, (idx, trade) in enumerate(df.iterrows()):
        if i == 3:
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







