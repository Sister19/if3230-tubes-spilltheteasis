import asyncio
from threading          import Thread, Event
from xmlrpc.client      import ServerProxy
from typing             import Any, List
from enum               import Enum
from lib.struct.address import Address
import socket
import time
import json
import random

class RaftNode:
    HEARTBEAT_INTERVAL   = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT          = 0.5

    class NodeType(Enum):
        LEADER    = 1
        CANDIDATE = 2
        FOLLOWER  = 3

    def __init__(self, application : Any, addr: Address, contact_addr: Address = None):
        self.election_term:       int               = 0
        self.voted_for:           Address           = None
        self.log:                 List[str, str]    = []
        # [message, leader term the time msg sent]
        self.commit_length:       int               = 0

        self.type:                RaftNode.NodeType = RaftNode.NodeType.FOLLOWER
        self.cluster_leader_addr: Address           = None
        # below: tambahan dari pseudocode
        self.votes_received:      Any               = []
        self.sent_length:         int               = 0
        self.acked_length:        int               = 0
        self.last_term:           int               = None

        self.election_running:    bool              = False
        self.election_stop:       Any               = None
        self.election_triggered:  bool              = False
        self.heartbeat_thread:    Any               = None
        self.announce_leader_thread:     Any               = None

        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)

        self.address:             Address           = addr
        self.app:                 Any               = application
        self.cluster_addr_list:   List[Address]     = []

        if contact_addr is None:
            self.__print_log('Initialize as leader node...')
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__print_log('Initialize as follower node...')
            self.__try_to_apply_membership(contact_addr)

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.cluster_leader_addr = self.address
        self.type                = RaftNode.NodeType.LEADER
        request = {
            'cluster_leader_addr': self.address
        }
        # TODO : Inform to all node this is new leader
        if self.announce_leader_thread == None:
            self.announce_leader_thread = Thread(target=asyncio.run,args=[self.announce_leader()], daemon=True)
            self.announce_leader_thread.start()

    async def announce_leader(self):
        while True:
            self.election_term += 1
            self.__print_log(f'Term {self.election_term} is started')
            self.type = RaftNode.NodeType.CANDIDATE
            self.voted_for = self.address
            self.votes_received = [self.address]
            self.last_term = 0

            if len(self.log) > 0:
                self.last_term = self.log[len(self.log) - 1][1]

            request = {
                'type': 'vote_request',
                'address': self.address,
                'term': self.election_term,
                'log_length': len(self.log),
                'last_term': self.last_term
            }

            for contact_address in self.cluster_addr_list:
                if contact_address != self.address:
                    print(f'TODO: send msg to {contact_address} to inform new leader')
                    # self.__send_request(request, 'func', contact_address)

            # start election timer
            if self.heartbeat_thread == None:
                self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__leader_heartbeat()], daemon=True)

            if not(self.election_triggered):
                self.heartbeat_thread.start()
                self.election_triggered = True

            if self.election_stop == None:
                self.election_stop = Event()
                self.election_stop.set()

            time.sleep(random.randint(RaftNode.ELECTION_TIMEOUT_MIN, RaftNode.ELECTION_TIMEOUT_MAX))

    async def __leader_heartbeat(self):
        # TODO : Send periodic heartbeat
        while True and self.election_stop.is_set():
            self.__print_log(f'[Leader] Sending heartbeat...')
            pass
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

    def apply_membership(self, address):
        req = json.loads(address)
        self.cluster_addr_list.append(Address(req['ip'], req['port']))
        response = {
            'status':           'success',
            'message':          'executed apply membership',
            'log':               self.log,
            'cluster_addr_list': self.cluster_addr_list
        }
        return json.dumps(response)

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        response = {
            'status': 'redirected',
            'address': {
                'ip':   contact_addr.ip,
                'port': contact_addr.port,
            },
        }
        while response['status'] != 'success':
            self.__print_log(f'Sending membership request to {contact_addr}')
            redirected_addr = Address(response['address']['ip'], response['address']['port'])
            response        = self.__send_request(self.address, 'apply_membership', redirected_addr)

        self.log                 = response['log']
        self.cluster_addr_list   = response['cluster_addr_list']
        self.cluster_leader_addr = redirected_addr

    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> 'json':
        # Warning : This method is blocking
        node         = ServerProxy(f'http://{addr.ip}:{addr.port}')
        json_request = json.dumps(request)
        rpc_function = getattr(node, rpc_name)
        response     = json.loads(rpc_function(json_request))
        self.__print_log(response)
        return response

    # Inter-node RPCs
    def heartbeat(self, json_request: str) -> 'json':
        # TODO : Implement heartbeat
        response = {
            'heartbeat_response': 'ack',
            'address':            self.address,
        }
        return json.dumps(response)


    # Client RPCs
    def execute(self, json_request: str) -> 'json':
        request = json.loads(json_request)
        # TODO : Implement execute
        return json.dumps(request)
