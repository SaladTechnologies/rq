import redis

print( 80 * "*")
print( 80 * "*")

r = redis.Redis(host='template', port=6379, decode_responses=True)

#r.flushdb()
#print("Database flushed.")

print()
keys = r.keys("*")
print(40 * "*" + " All keys:")
for key in keys:
    print(key)

print()
print(40 * "*" + " Historical I/O Information:")
requests_pending = r.zrange("requests:pending", 0, -1, withscores=True)
print("requests:pending (zset, ephemeral): ", requests_pending)
requests_completed = r.lrange("requests:completed", 0, -1)
print("requests:completed (list, optional): ", requests_completed)
requests_failed = r.lrange("requests:failed", 0, -1)
print("requests:failed (list, optional): ", requests_failed)

print()
print(40 * "*" + " Clients and Servers:")

clients = r.lrange("clients", 0, -1)
print("***** clients (list): ")
for temp in clients:
    print(temp)

servers = r.lrange("servers", 0, -1)
print("***** servers (list): ")
for temp in servers:
    print(temp)
      
print()
print(40 * "*" + " Temporary: notifying clients")
for key in keys:
    if "temporary" in key:
        print()
        print("***** " + key + " (list):")
        chunks = r.lrange(key, 0, -1)
        for chunk in chunks:
            print(chunk)

print()
print(40 * "*" + " Requests:")
request_index = r.zrange("request__index", 0, -1, withscores=True)
print("request__index (zset): ", request_index)

for temp in request_index:
    print()
    request = r.hgetall(temp[0])
    print("***** " + temp[0]+ " (hash):")
    print(request)

print()
print(40 * "*" + " Streaming:")
for key in keys:
    if "streaming" in key:
        print()
        print("***** " + key + " (list):")
        chunks = r.lrange(key, 0, -1)
        for chunk in chunks:
            print(chunk)

print( 80 * "*")
print( 80 * "*")

