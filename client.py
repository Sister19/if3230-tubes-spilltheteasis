from lib.struct.address     import Address
from lib.raft               import RaftNode
from xmlrpc.server          import SimpleXMLRPCServer
import sys
from xmlrpc.client          import ServerProxy
import json
import time
# from app           import MessageQueue

def start_communication(addr: Address, contact_node_addr: Address):
    print(f"[{addr.ip}:{addr.port}] [{time.strftime('%H:%M:%S')}] [Client] Start communicating to server {contact_node_addr}")
    server = ServerProxy(f'http://{contact_node_addr.ip}:{contact_node_addr.port}')

    print(server.system.listMethods())

    while True:
        command = input('Command: ')
        
        if command == 'exit':
            exit()
        if command == 'help':
            print('Commands: queue("[content]"), dequeue(), request_log(), exit, help') 

        if 'queue' in command or 'dequeue' in command or 'request_log' in command:
            request = {
                'request_addr': addr,
                'node_id': contact_node_addr,
                'command': command,
            }
            request = json.dumps(request)
            print(f"[{addr.ip}:{addr.port}] [{time.strftime('%H:%M:%S')}] [Client] Start sending commands to server")
            try: 
                print(type(request))
                response = server.execute(request)
            except Exception as e:
                # print(f"[{addr.ip}:{addr.port}] [{time.strftime('%H:%M:%S')}] [Client] Server is not responding")
                print(e)
                continue

            # print(response)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("client.py <ip> <port> <opt: contact ip> <opt: contact port>")
        exit()

    contact_addr = None
    if len(sys.argv) == 5:
        contact_addr = Address(sys.argv[3], int(sys.argv[4]))
    server_addr = Address(sys.argv[1], int(sys.argv[2]))

    start_communication(server_addr, contact_addr)

