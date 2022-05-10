import asyncio
from pyzeebe import ZeebeClient, create_camunda_cloud_channel

# Create channel to Zeebe
channel = create_camunda_cloud_channel(
    client_id="zR0s.fLdJBnPHX.J1S8leJMS0dv4H2LI",
    client_secret="3h7kspi_x4ic28t-y0DtP8P8Ep-jfBn3wVtM792tcYXTgz~fgL-G-m0u4ZkETSKO",
    cluster_id="b58c17d6-f7f4-44fe-8f30-98f0bc0c4ef8",
    region="bru-2"
)
# Create single threaded worker
client = ZeebeClient(channel)


async def deploy_proc():
    await client.deploy_process("process_models/trade-reconcile.bpmn")
    await client.run_process("trade-reconcile")
    a=3


# Main loop
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(deploy_proc())
finally:
    loop.stop()
    loop.close()







