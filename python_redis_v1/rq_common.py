from redis import Redis
from pydantic_redis import Model, RedisConfig, Store
from pydantic import BaseModel
from typing import Optional
import time
import json


CLIENT_APP_LIST        = "clients" # List of used clients, Redis list 
BACKEND_SERVER_LIST    = "servers" # List of used servers, Redis list 

REQUEST_ZSET_PENDING   = "requests:pending"   # List of Request ID with priority, Redis zset 
REQUEST_LIST_COMPLETED = "requests:completed" # list of Request ID, Redis list, optional
REQUEST_LIST_FAILED    = "requests:failed"    # list of Request ID, Redis list, Optional

TEMPORARY_KEYS         = "temporary"  # key prefix for notification
STREAMING              = "streaming"  # key prefix for streaming 


class Request(Model):
    _primary_key_field:str = "id"
    id: str                            # Client_ID + its Request_ID, globally unique
    input: str                         # Detailed task
    priority: Optional[int] = 0        # Processing priority
    status: Optional[str] = "pending"  # "pending", "completed", "failed" 
    result: Optional[str] = None       # A string for text, image or video
                                       # You could add a field indicating which node processed the request.

class RQ_Service:
    def __init__(self, server_response_time = 20):
        try:
            self.queue = Redis( host="template", 
                                port=6379, 
                                socket_connect_timeout=5, 
                                health_check_interval=10,
                                socket_keepalive=True,
                                socket_timeout=server_response_time,
                                retry_on_timeout=True )
            stats = self.Get_Statistics_for_Autoscaling_or_Flow_Control() # Initiate and test the connection
            print(stats)
        except Exception as e:
            print(f"Failed to connect to the Redis Cluster: {e}")
            exit(1)  # Exit if connection error

    def Client_Register(self, client):
        try:
            client = json.dumps(client)       
            self.queue.lpush( CLIENT_APP_LIST, client )
        except Exception as e:
            print(f"Error when registering the client {client}: {e}")
            exit(1)  # Exit if connection error

    def Server_Register(self, server):
        try:
            server = json.dumps(server)       
            self.queue.lpush( BACKEND_SERVER_LIST, server )
        except Exception as e:
            print(f"Error when registering the client {server}: {e}")
            exit(1)  # Exit if connection error

    def Client_Send_A_Request(self, request_ID, request_priority):
        try:       
            self.queue.zadd( REQUEST_ZSET_PENDING, {request_ID : request_priority} )
        except Exception as e:
            print(f"Error when sending the request {request_ID}: {e}")

    def Server_Retrieve_A_Request(self):
        try:            
            _, request_id, _ = self.queue.bzpopmax([REQUEST_ZSET_PENDING]) # blocking, may timeout
            request_id = request_id.decode('utf-8')
            return request_id
        except Exception as e:
            print(f"Error when retrieving the request - timeout, no request")

    def Client_Wait_Response(self, request_id):
        try:
            temp = self.queue.brpop(f"{TEMPORARY_KEYS}:{request_id}")   
            print(temp)
            return temp
        except Exception as e:
            print(f"Error when waiting for the response {request_id}: {e}")

    def Server_Provide_Response(self, request_id):
        try:
            self.queue.lpush(f"{TEMPORARY_KEYS}:{request_id}", "processed")
        except Exception as e:
            print(f"Error when providing the response {request_id}: {e}")

    def Client_Record_A_Finished_Request(self, request_id, success=True):
        try:
            list = REQUEST_LIST_COMPLETED if success else REQUEST_LIST_FAILED 
            self.queue.lpush(list, request_id)
        except Exception as e:
            print(f"Error when recording the processed request {request_id}: {e}")
        stats = self.Get_Statistics_for_Autoscaling_or_Flow_Control()
        print(stats)

    def Get_Statistics_for_Autoscaling_or_Flow_Control(self): 
        return {
            'pending': self.queue.zcount(REQUEST_ZSET_PENDING, '-inf', '+inf'), 
            'completed': self.queue.llen(REQUEST_LIST_COMPLETED), 
            'failed': self.queue.llen(REQUEST_LIST_FAILED)
        }


class Request_Store:
    def __init__(self, server_response_time = 20):
        try:
            self.store = Store(
                name="request_store",
                redis_config=RedisConfig( host="template", 
                                          port=6379, 
                                          socket_connect_timeout=5, 
                                          health_check_interval=10,
                                          socket_keepalive=True,
                                          socket_timeout=server_response_time,
                                          retry_on_timeout=True ) )
            self.store.register_model(Request)
        except Exception as e:
            print(f"Failed to connect to the Redis Cluster: {e}")
            exit(1)  # Exit if connection error

    def save(self, request: Request):
        try:
            Request.insert(request) 
        except Exception as e:
            print(f"Error when saving the request {request.id}: {e}")

    def load(self, request_id: str):
        try:
            requests = Request.select(ids=[request_id])
            return None if requests is None or len(requests) == 0 else requests[0]
        except Exception as e:
            print(f"Error when loading the request {request_id}: {e}")

    def delete(self, request_id: str):
        try:
            Request.delete(ids=[request_id])  # Delete the request by ID
        except Exception as e:
            print(f"Error when deleting the request {request_id}: {e}")


class Response_Chunk(BaseModel):
    last: bool = False
    chunk: Optional[str] = None
    # You could add a field indicating which node processed the chunks(request).
    

class Response_Streaming:
    def __init__(self, server_response_time = 20):
        try:
            self.queue = Redis( host="template", 
                                port=6379, 
                                socket_connect_timeout=5, 
                                health_check_interval=10,
                                socket_keepalive=True,
                                socket_timeout=server_response_time,
                                retry_on_timeout=True )
        except Exception as e:
            print(f"Failed to connect to the Redis Cluster: {e}")
            exit(1)  # Exit if connection error

    def load_chunk(self, request_id):
        try:
            result = self.queue.brpop([f"{STREAMING}:{request_id}"])  # Blocking pop to wait for a chunk event
            _, event_data = result
            decoded_event = event_data.decode('utf-8')
            return json.loads(decoded_event)
        except Exception as e:
            print(f"Error when loading the chunk of the request {request_id}: {e}")

    def save_chunk(self, request_id, chunk=None, last=False):
        try:
            temp = Response_Chunk(chunk=chunk, last=last)
            self.queue.lpush(f"{STREAMING}:{request_id}", temp.model_dump_json())
        except Exception as e:
            print(f"Error when saving the chunk of the request {request_id}: {e}")

  
