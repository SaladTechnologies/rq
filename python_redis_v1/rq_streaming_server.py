from rq_common import *
from rq_testdata import *
from datetime import datetime
import uuid

print( 60 * "*")

MACHINE_ID = str(uuid.uuid4())

queue = RQ_Service(server_response_time = 20)             # For requests
request_store = Request_Store(server_response_time = 10)  # Others: reference of requests, client/server, temporary

server = { "id": MACHINE_ID, "time": datetime.now().isoformat()}
queue.Server_Register( server )

response_streaming = Response_Streaming(server_response_time = 20) # For streaming 

while True:
    print( 40 * "*")    
    
    # Server: retrieve the request ID, that is removed from the requests:pending
    client_request_id = queue.Server_Retrieve_A_Request()
    if client_request_id == None:
        continue
    else:
        print(f"Get a request ID: {client_request_id}")

    # Server: load the request based on the request ID
    server_request = request_store.load(client_request_id)
    if server_request== None: # sth wrong !!!!!
        continue
    else:
        # print(f"Request: {server_request.input}")
        pass

    # Server: process the request
    length = len(list_20_50)
    for i in range(length):

        # The server may go down at this point and the request ID will be lost

        if i < length - 1:    # Server: save chunks
            response_streaming.save_chunk(client_request_id, list_20_50[i], False)
            # print(list_10_100[i],end="")
        else:                 #  Server: save the last chunk           
            response_streaming.save_chunk(client_request_id, list_20_50[i], True)
            # print(list_10_100[i])


