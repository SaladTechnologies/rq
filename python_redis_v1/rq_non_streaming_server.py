from rq_common import *
from rq_testdata import *
from datetime import datetime
import uuid

print( 60 * "*")

MACHINE_ID = str(uuid.uuid4())

request_store = Request_Store(server_response_time = 10)  # For requests
queue = RQ_Service(server_response_time = 20)             # Others: reference of requests, client/server, temporary

server = { "id": MACHINE_ID, "time": datetime.now().isoformat()}
queue.Server_Register( server ) # Register the server

while True:
    print( 40 * "*")    

    # Server: retrieve and remove the request ID from the requests:pending
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
        #print(f"Request: {server_request.input}")
        pass

    # Server: process the request 
    server_request.result = string_1000
    server_request.status = "completed" # "completed" or "failed"
    #print(f"Response: {server_request.result}")

    # The server may go down at this point and the request ID will be lost

    # Server: save its result
    request_store.save(server_request)

    # Server: notify the client
    print(f"Processed: {client_request_id}")
    queue.Server_Provide_Response(client_request_id)

