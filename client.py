from lib.struct.address       import Address
from lib.raft          import RaftNode
from xmlrpc.server import SimpleXMLRPCServer
import sys
import xmlrpc.client
# from app           import MessageQueue

def start_communication(addr: Address, contact_node_addr: Address):
    print(f'client: start communicating to server {addr} from {contact_node_addr}')
    server = xmlrpc.client.ServerProxy(f'http://{addr.ip}:{addr.port}')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("client.py <ip> <port> <opt: contact ip> <opt: contact port>")
        exit()

    contact_addr = None
    if len(sys.argv) == 5:
        contact_addr = Address(sys.argv[3], int(sys.argv[4]))
    server_addr = Address(sys.argv[1], int(sys.argv[2]))

    start_communication(server_addr, contact_addr)

