import asyncio
from pyzeebe import ZeebeWorker, create_camunda_cloud_channel

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="zR0s.fLdJBnPHX.J1S8leJMS0dv4H2LI",
    client_secret="3h7kspi_x4ic28t-y0DtP8P8Ep-jfBn3wVtM792tcYXTgz~fgL-G-m0u4ZkETSKO",
    cluster_id="b58c17d6-f7f4-44fe-8f30-98f0bc0c4ef8",
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