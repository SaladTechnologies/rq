from rq_common import *
from rq_testdata import *
from datetime import datetime
import uuid

print( 60 * "*")

CLIENT_ID = str(uuid.uuid4())

request_store = Request_Store(server_response_time = 10)  # For requests
queue = RQ_Service(server_response_time = 20)             # Others: reference of requests, client/server, temporary

client = { "id": CLIENT_ID, "time": datetime.now().isoformat()}
queue.Client_Register( client ) # Register the client

start_time = time.time()
num_of_requests = 100
for temp in range(num_of_requests):
    print( 40 * "*")    

    # Client: create a new user request
    client_request = Request(id = CLIENT_ID +"__"+str(temp), input = string_1000)  
    #print(client_request)

    # Client: save the request first 
    request_store.save(client_request)

    # Client: then send the request ID with its priority
    queue.Client_Send_A_Request( client_request.id, client_request.priority ) 
    
    # do something else 

    # Client: waiting the response
    temp = queue.Client_Wait_Response(client_request.id)
    if temp != None:

        # Client: Load the response
        client_request = request_store.load(client_request.id)
        # print(client_request)

        # Client: delete the original request
        request_store.delete(client_request.id)
        
    else: # timeout
        # Case 1: 
        # Case 2: 
        # Case 3: 
        stats = queue.Get_Statistics_for_Autoscaling_or_Flow_Control() 
        print(stats)
        pass

    # Client: record the request
    # queue.Client_Record_A_Finished_Request( client_request.id, True )
    # queue.Client_Record_A_Finished_Request( client_request.id, False )
        
end_time = time.time()
print(f"{num_of_requests} requests processed in {end_time - start_time} seconds.")