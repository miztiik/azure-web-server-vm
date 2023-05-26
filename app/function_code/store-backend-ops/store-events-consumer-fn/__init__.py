from azure.identity.aio import DefaultAzureCredential
from azure.eventhub.aio import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
import azure.functions as func
import os
import logging
import json
import datetime
import random
import uuid
import asyncio
import time


class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2023-05-26"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    EVNT_WEIGHTS = {"success": 80, "fail": 20}
    TRIGGER_RANDOM_FAILURES = os.getenv("TRIGGER_RANDOM_FAILURES", True)
    WAIT_SECS_BETWEEN_MSGS = int(os.getenv("WAIT_SECS_BETWEEN_MSGS", 2))
    TOT_MSGS_TO_PRODUCE = int(os.getenv("TOT_MSGS_TO_PRODUCE", 10))

    SA_NAME = os.getenv("SA_NAME", "warehousehuscgs003")
    BLOB_SVC_ACCOUNT_URL = os.getenv("BLOB_SVC_ACCOUNT_URL","https://warehousehuscgs003.blob.core.windows.net")
    BLOB_NAME = os.getenv("BLOB_NAME", "store-events-blob-003")
    BLOB_PREFIX = "sales_events"

    COSMOS_DB_URL = os.getenv("COSMOS_DB_URL", "https://partition-processor-db-account-003.documents.azure.com:443/")
    COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME", "partition-processor-db-account-003")
    COSMOS_DB_CONTAINER_NAME = os.getenv("COSMOS_DB_CONTAINER_NAME", "store-backend-container-003")
    
    SVC_BUS_FQDN = os.getenv("SVC_BUS_FQDN", "warehouse-q-svc-bus-ns-003.servicebus.windows.net")
    SVC_BUS_Q_NAME = os.getenv("SVC_BUS_Q_NAME","warehouse-q-svc-bus-q-003")

    MSG_COUNT = 0
    MAX_MESSAGES_TO_PROCESS = 5
    EVENT_HUB_FQDN = os.getenv("EVENT_HUB_FQDN", "warehouse-event-hub-ns-partition-processor-003.servicebus.windows.net")
    EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME","store-events-stream-003")
    EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME = os.getenv("EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME","sale-events-consumers-003")

msg_count=0

async def write_to_blob(container_prefix: str, data: dict):

    from azure.identity import DefaultAzureCredential
    azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
    default_credential = DefaultAzureCredential(logging_enable=False,logging=azure_log_level)

    blob_svc_client = BlobServiceClient(GlobalArgs.BLOB_SVC_ACCOUNT_URL, credential=default_credential, logging=azure_log_level)
    try:
        blob_name = f"{GlobalArgs.BLOB_PREFIX}/event_type={container_prefix}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{str(int(time.time() * 1000))}.json"
        if container_prefix is None:
            blob_name = f"{GlobalArgs.BLOB_PREFIX}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{datetime.datetime.now().strftime('%s%f')}.json"
        resp = blob_svc_client.get_blob_client(container=f"{GlobalArgs.BLOB_NAME}", blob=blob_name).upload_blob(json.dumps(data).encode("UTF-8"))
        print(resp)
        logging.info(f"Blob {blob_name} uploaded successfully")
        logging.debug(f"{resp}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

async def write_to_cosmosdb(data: dict):
    try:
        from azure.identity import DefaultAzureCredential
        azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
        default_credential = DefaultAzureCredential(logging_enable=False,logging=azure_log_level)
        cosmos_client = CosmosClient(url=GlobalArgs.COSMOS_DB_URL, credential=default_credential)
        db_client = cosmos_client.get_database_client(GlobalArgs.COSMOS_DB_NAME)
        db_container = db_client.get_container_client(GlobalArgs.COSMOS_DB_CONTAINER_NAME)
        data["id"] = data.pop("request_id", None)
        resp = db_container.create_item(body=data)
        print(resp)
        # db_container.create_item(body={'id': str(random.randrange(100000000)), 'ts': str(datetime.datetime.now())})
        logging.info(f"Document with id {data['id']} written to CosmosDB successfully")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

async def receive_events():
    credential = DefaultAzureCredential(logging_enable=True)
    client = EventHubConsumerClient(
    fully_qualified_namespace=GlobalArgs.EVENT_HUB_FQDN,
    eventhub_name=GlobalArgs.EVENT_HUB_NAME,
    consumer_group=GlobalArgs.EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME,
    credential=credential,
    logging_enable=True)

    async def on_event_batch(partition_context, events):
        # Process the batch of events
        for event in events:
            print(event.body_as_str())
            # global msg_count, client
            global msg_count
            msg_count += 1
            recv_body = event.body_as_str()
            print("Received event from partition: {}.".format(partition_context.partition_id))
            print("Received event: {}".format(event.body_as_str()))
            print("Properties: {}".format(event.properties))
            print("System properties: {}".format(event.system_properties))
            print(f"Msg Count: {msg_count} of {GlobalArgs.MAX_MESSAGES_TO_PROCESS}")
            logging.info("Received event from partition: {}.".format(partition_context.partition_id))
            logging.info("Received event: {}".format(event.body_as_str()))
            logging.info("Properties: {}".format(event.properties))
            logging.info("System properties: {}".format(event.system_properties))
            logging.info(f"Message Count: {msg_count} of {GlobalArgs.MAX_MESSAGES_TO_PROCESS}")
          
            # write to blob
            _evnt_type=event.properties[b'event_type'].decode()
            await write_to_blob(container_prefix=_evnt_type, data=json.loads(recv_body))

            # write to cosmosdb
            await write_to_cosmosdb(json.loads(recv_body))

            # logging.info(f"Message {msg_count} processed successfully")
            # print(f"Message {msg_count} processed successfully")

            if msg_count >= GlobalArgs.MAX_MESSAGES_TO_PROCESS:
                await partition_context.update_checkpoint(event)
                print("Updated checkpoint at {}".format(event.offset))
                logging.info("Updated checkpoint at {}".format(event.offset))
                logging.info("Exiting receive handler...")
                await client.close()

        # Update the checkpoint
        await partition_context.update_checkpoint()

    async with client:
        await client.receive_batch(
            on_event_batch=on_event_batch,
            starting_position="-1",  # "-1" is from the beginning of the partition.
            max_batch_size=5,
            partition_id = str(random.choice([1, 3]))
        )

async def main(req: func.HttpRequest):
# async def main():
    _d={
        "miztiik_event_processed": False,
        "msg": ""
    }
    await receive_events()
    _d["count"] = GlobalArgs.MAX_MESSAGES_TO_PROCESS
    _d["miztiik_event_processed"] = True
    _d["last_processed_on"] = datetime.datetime.now().isoformat()
    return func.HttpResponse(
        f"{json.dumps(_d, indent=4)}",
            status_code=200
    )

# if __name__ == "__main__":
#     asyncio.run(main())
