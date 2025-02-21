from rq_common import *
from rq_testdata import *
from datetime import datetime
import uuid

print( 60 * "*")

CLIENT_ID = str(uuid.uuid4())

queue = RQ_Service(server_response_time = 20)             # For requests
request_store = Request_Store(server_response_time = 10)  # Others: reference of requests, client/server, temporary

client = { "id": CLIENT_ID, "time": datetime.now().isoformat()}
queue.Client_Register( client )

response_streaming = Response_Streaming(server_response_time = 20) # For streaming 

start_time = time.time()
num_of_requests = 100
for temp in range(num_of_requests):
    print( 40 * "*")    

    # Client: have a new request
    client_request = Request(id = CLIENT_ID +"__"+str(temp), input = string_100)  
    # print(client_request)

    # Client: save the request first 
    request_store.save(client_request)

    # Client: then send the request ID with its priority
    queue.Client_Send_A_Request( client_request.id, client_request.priority ) 
   
    # do something else

    # Client: waiting the response
    while True:
        temp = response_streaming.load_chunk(client_request.id)
        if temp != None:

            if temp['last'] == False:
                # print(temp['chunk'],end="")
                continue
            else:                      # the last chunk
                #print(temp['chunk'])
                
                # Client: delete the request
                request_store.delete(client_request.id)
                break

        else: # Timeout
            # Case 1: 
            # Case 2: 
            # Case 3: 
            stats = queue.Get_Statistics_for_Autoscaling_or_Flow_Control() 
            print(stats)
            pass

    # Client: record the request info
    # queue.Client_Record_A_Finished_Request( client_request.id, True )
    # queue.Client_Record_A_Finished_Request( client_request.id, False )

end_time = time.time()
print(f"{num_of_requests} requests processed in {end_time - start_time} seconds.")

