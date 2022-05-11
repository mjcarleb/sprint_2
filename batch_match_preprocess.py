import pandas as pd
import dask.dataframe as dd
import asyncio
from pyzeebe import ZeebeClient, create_camunda_cloud_channel


async def run_trade_match_batch(bpmn_process_id, merged_df):
    """Create C8 process to process all trades"""

    for i, (idx, trade) in enumerate(merged_df.iterrows()):
        if i == 101:
            break
        else:
            await client.run_process(bpmn_process_id=bpmn_process_id,
                                     variables=trade.to_dict())

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="Dk0MPLoP_F0CmECfoidErdBdcLzZxLr.",
    client_secret="_2VaLaDvXpFqtTlUbbinhs_oT_9e.8epWXIAMe5STottheBev9293zxQHG6jGaM~",
    cluster_id="241fa57d-bec2-4fec-9968-8f651682b023",
)
# Create single threaded worker
client = ZeebeClient(channel)

# Perform batch pre-processing (match vs. unmatched trades)
data_dir = "../DataGeneration/data/"
file_name = "firm_trades"
df = pd.read_parquet(path=f"{data_dir}{file_name}")
firm_ddf = dd.from_pandas(df, npartitions=2)

file_name = "street_trades"
df = pd.read_parquet(path=f"{data_dir}{file_name}")
street_ddf = dd.from_pandas(df, npartitions=2)

firm_idx = firm_ddf.index
street_idx = street_ddf.index

merged_df = firm_ddf.merge(street_ddf, how="outer", indicator=True)

# Now, send matched and unmatched trades through the process
bpmn_process_id = "Process_cdd8ac1b-a3e5-4467-8061-784691625fe2"
loop = asyncio.get_event_loop()
try:
    results = loop.run_until_complete(run_trade_match_batch(bpmn_process_id, merged_df))
finally:
    loop.stop()
    loop.close()

