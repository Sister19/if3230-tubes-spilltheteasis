from lib.struct.address     import Address
from lib.raft               import RaftNode
from xmlrpc.server          import SimpleXMLRPCServer
import sys
import xmlrpc.client
# from app           import MessageQueue

def start_communication(addr: Address, contact_node_addr: Address):
    print(f'client: start communicating to server {addr} from {contact_node_addr}')
    server = xmlrpc.client.ServerProxy(f'http://{addr.ip}:{addr.port}')

    while True:
        command = input('Command:')
        message = command[7]
        
        if command[:4] == 'queue':
            server.push(message)
        elif command[:4] == 'dequeue':
            popped_elmt = server.pop()
            print(f'Popped element: {popped_elmt}')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("client.py <ip> <port> <opt: contact ip> <opt: contact port>")
        exit()

    contact_addr = None
    if len(sys.argv) == 5:
        contact_addr = Address(sys.argv[3], int(sys.argv[4]))
    server_addr = Address(sys.argv[1], int(sys.argv[2]))

    start_communication(server_addr, contact_addr)

