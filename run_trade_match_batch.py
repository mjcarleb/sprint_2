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


async def run_proc(proc_key):

    results = await client.run_process(bpmn_process_id=bpmn_process_id,
                                       variables={'security_id':'AA_1234'})
    return results

bpmn_process_id = "Process_cdd8ac1b-a3e5-4467-8061-784691625fe2"

# Main loop
loop = asyncio.get_event_loop()
try:
    results = loop.run_until_complete(run_proc(bpmn_process_id))
    a=3
finally:
    loop.stop()
    loop.close()
    a=3








