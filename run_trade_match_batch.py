import asyncio
from pyzeebe import ZeebeClient, create_camunda_cloud_channel
import pandas as pd

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="Dk0MPLoP_F0CmECfoidErdBdcLzZxLr.",
    client_secret="_2VaLaDvXpFqtTlUbbinhs_oT_9e.8epWXIAMe5STottheBev9293zxQHG6jGaM~",
    cluster_id="241fa57d-bec2-4fec-9968-8f651682b023",
)
# Create single threaded worker
client = ZeebeClient(channel)


async def run_trade_match_batch(bpmn_process_id, df):

    for i, (idx, trade) in enumerate(df.iterrows()):
        if i == 50:
            break
        else:
            await client.run_process(bpmn_process_id=bpmn_process_id,
                                     variables=trade.to_dict())

bpmn_process_id = "Process_cdd8ac1b-a3e5-4467-8061-784691625fe2"

loop = asyncio.get_event_loop()

# Get trades from firm trade file
data_dir = "../DataGeneration/data/"
file_name = "firm_trades"
df = pd.read_parquet(path=f"{data_dir}{file_name}")

try:
    results = loop.run_until_complete(run_trade_match_batch(bpmn_process_id, df))
finally:
    loop.stop()
    loop.close()







