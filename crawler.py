import sys, time, logging, urllib, hmac, hashlib, base64
import os
import asyncio
import datetime
from azure.core.credentials import AzureSasCredential
from azure.eventhub.aio import EventHubConsumerClient
from azure.storage.blob.aio import BlobServiceClient
from numpy import int64
import xml.etree.ElementTree as ET
import requests_async as requests

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
workspace_id = os.environ.get("WORKSPACE_ID","")
workspace_sas = os.environ.get("WORKSPACE_KEY", "")
workspace_log_type = os.environ.get("WORKSPACE_LOG_TYPE","")
prefix_path = event_hub_fq + "/" + event_hub_name + "/" 

logger.info("Create EH and Blob Service clients")
blob_service_client = BlobServiceClient(account_url=blob_service_uri, credential=blob_service_sas)

#sign header gor log
async def build_signature(ws_id, ws_sas, date, content_length, method, content_type, resource):
    x_headers = "x-ms-date:" + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = bytes(string_to_hash, encoding="utf-8") 
    decoded_key = base64.b64decode(ws_sas)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode()
    authorization = f"SharedKey {ws_id}:{encoded_hash}"
    return authorization

#POST data to log analytic
async def post_data(ws_id, ws_sas, body, log_type):
    method = "POST"
    content_type = "application/json"
    resource = "/api/logs"
    rfc1123date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    content_length = len(body)
    signature = await build_signature(ws_id, ws_sas, rfc1123date, content_length, method, content_type, resource)
    uri = "https://" + ws_id + ".ods.opinsights.azure.com" + resource + "?api-version=2016-04-01"
    headers = {
        "content-type": content_type,
        "Authorization": signature,
        "Log-Type": log_type,
        "x-ms-date": rfc1123date
    }

    logger.info(f"Sending to log analytic {content_length} bytes")
    response = await requests.post(uri,data=body, headers=headers)
    if (response.status_code >= 200 and response.status_code <= 299):
        logger.info(f"OK sending to log analytic {response.status_code}")
        return True
    else:
        logger.error(f"Error in response code: {response.status_code}")
        return False

#helper method for generating EH SAS Token
async def get_auth_token(eh_ns, eh_name, sas_name, sas_value):   
  uri = urllib.parse.quote_plus("https://{}.servicebus.windows.net/{}" \
                                .format(eh_ns,eh_name))
  sas = sas_value.encode("utf-8")
  expiry = str(int(time.time() + 10000))
  string_to_sign = (uri + "\n" + expiry).encode("utf-8")
  signed_hmac_sha256 = hmac.HMAC(sas, string_to_sign, hashlib.sha256)
  signature = urllib.parse.quote(base64.b64encode(signed_hmac_sha256.digest()))
  return  "SharedAccessSignature sr={}&sig={}&se={}&skn={}" \
                    .format(uri, signature, expiry, sas_name)

async def getunprocessedevent(i_consumer_group, sas):
  cred = AzureSasCredential(sas)
  eh_client = EventHubConsumerClient(fully_qualified_namespace=event_hub_fq,eventhub_name=event_hub_name,
        consumer_group=i_consumer_group, credential=cred)
  
  partition_ids = await eh_client.get_partition_ids()
  retval = 0
  for partitionId in partition_ids:    
    partitioninfo = await eh_client.get_partition_properties(partitionId)
    blob_path = prefix_path+i_consumer_group+"/checkpoint/"+partitionId
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
    logger.info("Retrieve info partition - " + partitionId + " consumer group - " + i_consumer_group + " aggragate lag - " + str(retval))
  return retval

async def getallconsumergroup(sas):
  logger.info("Retrieve all consumer groups for " + event_hub_namespace + "/" + event_hub_name)
  consumers_groups = []
  request_url = "https://{}.servicebus.windows.net/{}/consumergroups?timeout=60&api-version=2014-01".format(event_hub_namespace,event_hub_name)
  response_consumers = await requests.get(request_url,headers={'Authorization':sas, 'Content-Type':'application/atom+xml;type=entry;charset=utf-8'})
  if response_consumers.status_code == 200:    
    xmlresponse = ET.fromstring(response_consumers.text)
    for child in xmlresponse:
      if child.tag == '{http://www.w3.org/2005/Atom}entry':
        for subchild in child:
          if subchild.tag == '{http://www.w3.org/2005/Atom}title':
            consumers_groups.append(subchild.text)
            break              
  else:
    logger.error(f"Error get consumer groups list, err code={response_consumers.status_code}")
  return consumers_groups

async def main():
  logger.info("Generate EH SAS")
  generated_sas = await get_auth_token(eh_ns=event_hub_namespace, eh_name=event_hub_name, sas_name=eh_sas_name, sas_value=eh_sas_key)
  if not consumer_group_name:
    consumers = await getallconsumergroup(sas=generated_sas)
  else:
    consumers = [consumer_group_name]
  while True:
    retval = ""      
    i = 0
    for consumer in consumers:
      if (i > 0):
        retval+= ","
      unprocess_msg = await getunprocessedevent(i_consumer_group=consumer.lower(),sas=generated_sas)
      retval += "{}:{}".format(consumer,unprocess_msg)
      i+= 1
    retval = "{" + retval + "}"
    logger.warning(retval)
    if not workspace_id:
      await post_data(workspace_id, workspace_sas, retval, workspace_log_type)
    await asyncio.sleep(interval_process)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())