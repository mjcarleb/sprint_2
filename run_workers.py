import asyncio
from pyzeebe import ZeebeWorker, create_camunda_cloud_channel
import pandas as pd

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="Dk0MPLoP_F0CmECfoidErdBdcLzZxLr.",
    client_secret="_2VaLaDvXpFqtTlUbbinhs_oT_9e.8epWXIAMe5STottheBev9293zxQHG6jGaM~",
    cluster_id="241fa57d-bec2-4fec-9968-8f651682b023",
)
# Create single threaded worker
worker = ZeebeWorker(channel)

data_dir = "../DataGeneration/data/"
file_name = "street_trades"
street_df = pd.read_parquet(path=f"{data_dir}{file_name}")
street_idx = street_df.index
a=3

# Define work this client should do when trade_match_worker job exists in Zeebe
@worker.task(task_type="trade_match_worker")
async def trade_match_work(trans_ref,
                           account,
                           security_id,
                           price,
                           price_currency,
                           sanctioned_security,
                           quantity,
                           trans_type,
                           amount,
                           amount_currency,
                           market,
                           counter_party,
                           participant,
                           settle_date,
                           actual_settle_date,
                           source_system,
                           trade_status,
                           user_id,
                           matched
                           ):

    firm_trade = f"{account}|" + \
                 f"{security_id}|" + \
                 f"{quantity}|" + \
                 f"{trans_type}|" + \
                 f"{amount}|" + \
                 f"{amount_currency}|" + \
                 f"{market}|" + \
                 f"{counter_party}|" + \
                 f"{settle_date}|" + \
                 f"{participant}"

    print(f"matching:  qty={quantity}")

    matches = street_idx.isin([firm_trade])
    if matches.any():
        for i, match in enumerate(matches):
            if match:
                previously_matched = street_df["matched"].loc[street_idx[i]]
                if previously_matched == "":
                    # Dupes in index?
                    street_df.at[street_idx[i], "matched"] = "matched"
                    return {"match_result": "matched"}

        # All matches already used
        return {"match_result": "unmatched"}
    else:
        return {"match_result": "unmatched"}

# Main loop
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(worker.work())
finally:
    loop.stop()
    loop.close()