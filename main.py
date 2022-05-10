import asyncio
from pyzeebe import ZeebeWorker, create_camunda_cloud_channel

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="F6twwTJrs9Vo77b8CEf~DT6ga1wqY~0I",
    client_secret="vsQVRdmL7vPSfjDbouwKDatNe_N~oOVEpHzpQw-SF7Oo3kIo-vucAy-e615rHtKy",
    cluster_id="df4c514a-5a6b-45a8-8bf3-1c147540a2f4",
    region="bru-2"
)
# Create single threaded worker
worker = ZeebeWorker(channel)

# Define work this client should do when trade_match_worker job exists in Zeebe
@worker.task(task_type="trade_match_worker")
async def trade_match_work(qty, account):
    print(f"working:  qty={qty}")
    return {"match_result": "unmatched"}

# Main loop
loop = asyncio.get_event_loop()
loop.run_until_complete(worker.work())


# Trigger this on some event (SIGTERM for example)
async def shutdown():
    await worker.stop()