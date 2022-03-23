from multiprocessing import Event
import sys, time, logging, urllib, hmac, hashlib, base64
import asyncio, os
import asyncio
from datetime import datetime
from azure.core.credentials import AzureSasCredential
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.storage.blob.aio import BlobServiceClient
from numpy import int64

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  

logger.addHandler(logging.StreamHandler(sys.stdout))

logger.info("Read environment variables...")

blob_service_uri = os.environ.get("BLOB_URI", "<YOUR CHECKPOINT AZURE STORAGE URI>")
blob_service_sas = os.environ.get("BLOB_SAS", "<SAS TOKEN for the azure storage>")
checkpoint_container = os.environ.get("BLOB_CHECKPOINT_CONTAINER", "<CONTAINER NAME of the checkpoint>")
event_hub_namespace = os.environ.get("EVENTHUB_NAMESPACE", "<EVENT HUB NAMESPACE>")
event_hub_name = os.environ.get("EVENTHUB_NAME", "<EVENT HUB NAME")
consumer_group_name = os.environ.get("EVENTHUB_CONSUMERGROUP", "<CONSUMER GROUP NAME>")
interval_process = os.environ.get("INTERVAL_CRAWLER", 120)
event_hub_fq = "{}.servicebus.windows.net".format(event_hub_namespace)
eh_sas_name = "RootManageSharedAccessKey"
eh_sas_key = "<YOUR EVENT HUB ACCESS KEY>"
prefix_path = event_hub_fq + "/" + event_hub_name + "/" 

logger.info("Create EH and Blob Service clients")
blob_service_client = BlobServiceClient(account_url=blob_service_uri, credential=blob_service_sas)

#helper method for generating EH SAS Token
async def get_auth_token(eh_ns, eh_name, sas_name, sas_value):   
  uri = urllib.parse.quote_plus("https://{}.servicebus.windows.net/{}" \
                                .format(eh_ns,eh_name))
  sas = sas_value.encode('utf-8')
  expiry = str(int(time.time() + 10000))
  string_to_sign = (uri + '\n' + expiry).encode('utf-8')
  signed_hmac_sha256 = hmac.HMAC(sas, string_to_sign, hashlib.sha256)
  signature = urllib.parse.quote(base64.b64encode(signed_hmac_sha256.digest()))
  return  "SharedAccessSignature sr={}&sig={}&se={}&skn={}" \
                    .format(uri, signature, expiry, sas_name)

async def getunprocessedevent(i_consumer_group):
  generated_sas = await get_auth_token(eh_ns=event_hub_namespace, eh_name=event_hub_name, sas_name=eh_sas_name, sas_value=eh_sas_key)
  cred = AzureSasCredential(generated_sas)
  eh_client = EventHubConsumerClient(fully_qualified_namespace=event_hub_fq,eventhub_name=event_hub_name,
        consumer_group=i_consumer_group, credential=cred)
  
  partition_ids = await eh_client.get_partition_ids()
  retval = 0
  for partitionId in partition_ids:
    logger.info("Retrieve info partition - " + partitionId)
    partitioninfo = await eh_client.get_partition_properties(partitionId)
    blob_path = prefix_path+consumer_group_name+"/checkpoint/"+partitionId
    blob_client = blob_service_client.get_blob_client(container = checkpoint_container, blob = blob_path)
    properties = await blob_client.get_blob_properties()
    seq_num = int64(properties.metadata["sequencenumber"])
    offset = properties.metadata["offset"]
    last_enqueued = partitioninfo["last_enqueued_sequence_number"]
    if not offset:
      retval += seq_num + 1
    elif last_enqueued >= seq_num:
      retval += last_enqueued - seq_num
    else:
      tmp = (sys.maxsize - last_enqueued) + seq_num
      if tmp > 0:
        retval += tmp
  return retval
    

async def main():
  while True:
    if not consumer_group_name:
      logger.info("TODO: go over all consumer groups")
      #https://docs.microsoft.com/en-us/rest/api/eventhub/preview/consumer-groups/list-by-event-hub
    else:
      logger.info("Get consumer group info - " + consumer_group_name)
      unprocess_msg = await getunprocessedevent(consumer_group_name)
      logger.warn(unprocess_msg)
    await asyncio.sleep(interval_process)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())