import asyncio

import pandas as pd
from pyzeebe import ZeebeWorker, create_camunda_cloud_channel
import pickle

####################################################
#      SETUP TO USE DT MODEL
####################################################
model_dir = "../ML_resolver/models/"

file_name = "feature_enc_model.serial"
with open(f"{model_dir}{file_name}", "rb") as f:
    feature_enc = pickle.load(f)

file_name = "label_enc_model.serial"
with open(f"{model_dir}{file_name}", "rb") as f:
    label_enc = pickle.load(f)

file_name = "dt_model.serial"
with open(f"{model_dir}{file_name}", "rb") as f:
    dt_model = pickle.load(f)


####################################################
#      SETUP TO USE C8 ZEEBE
####################################################
# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="Dk0MPLoP_F0CmECfoidErdBdcLzZxLr.",
    client_secret="_2VaLaDvXpFqtTlUbbinhs_oT_9e.8epWXIAMe5STottheBev9293zxQHG6jGaM~",
    cluster_id="241fa57d-bec2-4fec-9968-8f651682b023",
)
# Create single threaded worker
worker = ZeebeWorker(channel)

####################################################
#          DEFINE WORKERS
####################################################
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
                           ledger,
                           _merge
                           ):

    print(f"merge_results for {trans_ref} from {ledger} = {_merge}")

    if _merge == "both":
        return {"match_result": "matched"}
    else:
        return {"match_result":  "unmatched"}


@worker.task(task_type="assign_resolver_worker")
async def assign_resolver_work(trans_ref,
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
                           ledger,
                           _merge
                           ):

    #########################################
    # Point of reference about Ops Org model:
    #
    #  (1) Mary manages settlements in US
    #  (2) Hasan manages corp actions in US
    #  (3) Monique manages stock loan in US
    #  (4) Greta manages settlements in EC
    #  (5) Kareem manages corp actions in EC
    #  (6) Suresh manages stock loan in EC
    #  (7) Justin manages settlements in UK
    #  (8) Bianca manages corp actions in UK
    #  (9) Mona manages stock loan in UK
    #  (10) Yui manages settlements in JPN
    #  (11) Akari manages corp actions in JPN
    #  (12) Ema manages stock loan in JPN
    #
    #########################################

    # encode trade values to create X to feed DT predict model
    # result will come back as OHE
    X = feature_enc.transform(pd.DataFrame(data={"market":[market],
                                                 "source_system": [source_system],
                                                 "account":  [account]}))
    resolver_ohe = dt_model.predict(X)

    # reverse OHE of prediction to get resolver in english
    resolver = label_enc.inverse_transform(resolver_ohe)[0][0]

    print(f"creating assignment for {trans_ref}:  {resolver}")

    return {"assignment_value": f"{resolver}"}

# Main loop
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(worker.work())
finally:
    loop.stop()
    loop.close()