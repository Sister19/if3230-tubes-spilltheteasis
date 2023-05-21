import asyncio
from threading     import Thread, Timer
from xmlrpc.client import ServerProxy
from typing        import Any, List
from enum          import Enum
from .struct.address       import Address
from .app                  import MessageQueue
from math         import ceil
import socket
import time
import json
import random


class RaftNode:
    HEARTBEAT_INTERVAL      = 1
    ELECTION_TIMEOUT_MIN    = 5
    ELECTION_TIMEOUT_MAX    = 10
    HEARTBEAT_TIMEOUT_MIN   = 1.5
    HEARTBEAT_TIMEOUT_MAX   = 2.5
    RPC_TIMEOUT             = 0.5

    class NodeType(Enum):
        LEADER    = 1
        CANDIDATE = 2
        FOLLOWER  = 3
        LOG_MSG_IDX   = 0
        LOG_TERM_IDX  = 1

    def __init__(self, application : Any, addr: Address, contact_addr: Address = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address:             Address           = addr
        self.type:                RaftNode.NodeType = RaftNode.NodeType.FOLLOWER
        self.log:                 List[str, str]    = []
        self.app:                 MessageQueue      = application
        self.election_term:       int               = 0
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        self.election_thread:     Thread            = None
        self.election_timer:      Timer             = None
        self.heartbeat_thread:    Thread            = None

        self.votes_received = []
        self.sent_length: List[Address, int] = []
        self.acked_length = 0
        self.commit_length: int = 0

        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.__print_log("Initialize as leader node...")
        self.cluster_leader_addr = self.address
        self.type                = RaftNode.NodeType.LEADER
        request = {
            "cluster_leader_addr": self.address,
            "cluster_addr_list": self.cluster_addr_list,
            "log": self.log,
            "election_term": self.election_term,
        }
        print(self.cluster_addr_list)
        # TODO : Inform to all node this is new leader
        for addr in self.cluster_addr_list:
            if addr != self.address:
                try:
                    self.__send_request(request, "announce_as_leader", addr)
                except:
                    self.__print_log(f"Failed to send announce_as_leader request to {addr}")

        self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()
    
    def __initialize_as_candidate(self):
        self.__print_log("Initialize as candidate node...")
        self.cluster_leader_addr = None
        self.type = RaftNode.NodeType.CANDIDATE
        self.election_term += 1
        self.voted_for = self.address
        self.votes = 1

        # if self.election_timer is None:
        #     while self.cluster_leader_addr is None or self.election_timer:
        #         self.election_timer = Timer(self.__election_timeout(), self.__leader_election)
        #         self.election_timer.start()

        if self.election_thread is None:
            self.election_thread = Thread(target=asyncio.run,args=[self.__leader_election()])
            self.election_thread.start()

    def __initialize_as_follower(self):
        self.__print_log("Initialize as follower node...")
        self.type = RaftNode.NodeType.FOLLOWER
        self.heartbeat_timer = 0
        self.current_hearbeat_timeout = self.__heartbeat_timeout()
        self.voted_for = None
        self.votes = 0
        # while self.cluster_leader_addr is None:
        #     print("Waiting for leader...")
        if self.cluster_leader_addr is not None:
            self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__follower_heartbeat()])
            self.heartbeat_thread.start()

    async def __leader_election(self):
        # TODO : Start leader election
        # while self.cluster_leader_addr is None:
        self.election_timer = 0
        self.current_election_timeout = self.__election_timeout()
        timeout_election = False

        self.__initialize_as_candidate()
        self.__print_log("Start leader election...")
        self.__vote_request()

        while self.cluster_leader_addr is None or timeout_election:
            if self.election_timer == 0:
                timeout_election = False
            
            self.election_timer += 1
            if self.election_timer >= self.current_election_timeout:
                if self.cluster_leader_addr is None:
                    self.__print_log("Election timeout, new leader not found. Start new election...")
                    timeout_election = True
                    self.election_timer = 0
                    self.__initialize_as_candidate()
                    self.__print_log("Start leader election...")
                    self.__vote_request()
                else:
                    self.election_thread = None
                    return

            await asyncio.sleep(1)
    
    def __vote_request(self):
        # TODO : Voting
        request = {
            "election_term": self.election_term,
            "candidate_addr": self.address
        }
        for addr in self.cluster_addr_list:
            if addr != self.address:
                response = self.__send_request(request, "vote_request", addr)
                if response is not None:
                    if self.type ==  RaftNode.NodeType.CANDIDATE and response["election_term"] == self.election_term and response["vote_response"] == "granted":
                        self.votes += 1
                    elif response["election_term"] > self.election_term:
                        self.election_term = response["election_term"]
                        self.__print_log("Election term is not valid")
                        self.__initialize_as_follower()
                        if self.election_thread is not None:
                            self.election_timer = 0
                        return
                    
                    if self.votes >= len(self.cluster_addr_list) // 2 + 1:
                        self.__initialize_as_leader()
                        self.__print_log("Elected as leader")
                        if self.election_thread is not None:
                            self.election_timer = self.current_election_timeout                    
        
        return True
    
    async def __leader_heartbeat(self):
        # TODO : Send periodic heartbeat
        while self.type == RaftNode.NodeType.LEADER:
            self.__print_log("[Leader] Sending heartbeat...")
            for addr in self.cluster_addr_list:
                addr = Address(addr["ip"], addr["port"])
                if addr != self.address:
                    Thread(target=self.__send_request, args=[self.address, "heartbeat", addr]).start()

            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
    
    async def __follower_heartbeat(self):
        # TODO : Listen for heartbeat
        self.heartbeat_timer = 0
        self.current_hearbeat_timeout = self.__heartbeat_timeout()
        while self.type == RaftNode.NodeType.FOLLOWER:
            self.heartbeat_timer += RaftNode.HEARTBEAT_INTERVAL
            if self.heartbeat_timer >= self.current_hearbeat_timeout:
                self.__print_log("Heartbeat not received")
                self.__initialize_as_candidate()
                return
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
    
    def __heartbeat_timeout(self):
        return random.uniform(RaftNode.HEARTBEAT_TIMEOUT_MIN, RaftNode.HEARTBEAT_TIMEOUT_MAX)
    
    def __election_timeout(self):
        return random.uniform(RaftNode.ELECTION_TIMEOUT_MIN, RaftNode.ELECTION_TIMEOUT_MAX)

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        response = self.__send_request(self.address, "apply_membership", redirected_addr)

        while response is None or response["status"] != "success":
            redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
            response        = self.__send_request(self.address, "apply_membership", redirected_addr)

        self.log                 = response["log"]
        self.cluster_addr_list   = list(map(lambda addr: Address(addr["ip"], addr["port"]), response["cluster_addr_list"]))
        self.cluster_leader_addr = redirected_addr

        for addr in self.cluster_addr_list:
            if addr != self.address and addr != self.cluster_leader_addr:
                self.__send_request(self.address, "announce_new_member", addr)
                print(f"Announce new member to {addr}")

        self.__initialize_as_follower()

    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> "json":
        # Warning : This method is blocking
        try:
            node         = ServerProxy(f"http://{addr.ip}:{addr.port}")
            json_request = json.dumps(request)
            rpc_function = getattr(node, rpc_name)
            response     = json.loads(rpc_function(json_request))
            self.__print_log(response)
            return response
        except Exception as e:
            self.__print_log(f"Erorr: {e}")
            self.__print_log(f"Failed to send request to {addr}")  
    
    # Inter-node RPCs for apply membership
    def apply_membership(self, json_request: str) -> "json":
        request = json.loads(json_request)
        # TODO : Implement apply_membership
        if self.type == RaftNode.NodeType.LEADER:
            self.__print_log("[Leader] Received request for membership")
            self.__print_log(f"[Leader] Adding node {request} to cluster")
            addr = Address(request["ip"], request["port"])
            if addr not in self.cluster_addr_list:
                self.cluster_addr_list.append(addr)
            
            response = {
                "status": "success",
                "log": self.log,
                "cluster_addr_list": self.cluster_addr_list
            }
        else:
            self.__print_log("[Follower] Received request for membership")
            self.__print_log(f"[Follower] Redirecting request to leader {self.cluster_leader_addr}")
            response = {
                "status": "redirected",
                "address": {
                    "ip":   self.cluster_leader_addr.ip,
                    "port": self.cluster_leader_addr.port,
                }
            }
        
        return json.dumps(response)
    
    def announce_new_member(self, json_request: str) -> "json":
        request = json.loads(json_request)
        self.__print_log(f"Received announcement from {request['ip']}:{request['port']}")
        addr = Address(request["ip"], request["port"])
        if addr not in self.cluster_addr_list:
            self.cluster_addr_list.append(addr)
        response = {
            "status": "success",
        }
        return json.dumps(response)

    # Inter-node RPCs for leader announcement
    def announce_as_leader(self, json_request: str) -> "json":
        request = json.loads(json_request)
        self.__print_log(f"Received leader announcement from {request['ip']}:{request['port']}")
        self.cluster_leader_addr    = Address(request["ip"], request["port"])
        self.cluster_addr_list      = list(map(lambda addr: Address(addr["ip"], addr["port"]), request["cluster_addr_list"]))
        self.log                    = request["log"]
        self.type                   = RaftNode.NodeType.FOLLOWER
        self.__initialize_as_follower()
        response = {
            "status": "success",
        }
        return json.dumps(response)

    # Inter-node RPCs for heartbeat
    def heartbeat(self, json_request: str) -> "json":
        # TODO: Implement heartbeat
        response = {
            "heartbeat_response": "ack",
            "address":            self.address,
        }
        
        if self.type != RaftNode.NodeType.LEADER:
            self.__print_log("[Follower] Received heartbeat")
            self.heartbeat_timer = 0
            return json.dumps(response)

        return json.dumps(response)

    # Inter-node RPCs for vote request
    def vote_request(self, json_request: str) -> "json":
        # TODO : Implement vote request
        request = json.loads(json_request)
        response = {
            "vote_response": "not granted",
            "address":       self.address,
            "election_term": self.election_term,
        }

        if request["election_term"] > self.election_term:
            self.election_term  = request["election_term"]
            response["election_term"] = self.election_term
            self.__print_log("Election term updated")
            self.__initialize_as_follower()

            if self.election_thread is not None:
                self.election_timer = 0
        
        if request["election_term"] == self.election_term and self.voted_for is None:
            self.voted_for = request["candidate_addr"]
            self.__print_log(f"Voted for {self.voted_for}")
            response["vote_response"] = "granted"
        else:
            self.__print_log(f"Already voted for {self.voted_for}")
        
        return json.dumps(response)

    # Client RPCs
    def execute(self, json_request: str) -> "json":
        request = json.loads(json_request)

        command = request["command"]
        params = request["params"]

        popped_elmt = None
        result = None

        if command == 'enqueue':
            self.app.push(params)
        elif command == 'dequeue':
            popped_elmt = self.app.pop()

        if not popped_elmt:
            result = "Success enqueue element"
        else:
            result = f'Success dequeue element {popped_elmt}'

        response = { "result": result }
        
        return json.dumps(response)
    
    
    def on_request_broadcast_msg(self, json_request: str) -> "json":
        request = json.loads(json_request)
        node_id = request["node_id"]
        msg     = request["msg"]
        if self.type == RaftNode.NodeType.LEADER:
            self.__print_log(f"[Leader] Received request broadcast msg from {node_id}")
            self.__print_log(f"[Leader] Broadcasting request to all nodes")
            # append the record (msg, term) to the log
            self.log.append((msg, self.election_term))
            self.acked_length[node_id] = len(self.log)
            
            for follower in self.cluster_addr_list:
                if follower != self.address:
                    self.replicate_log(self.address, follower)
        # forward the request to the leader via a fifo link
        else:
            self.forward_request(node_id, msg)
            
    def forward_request(self, json_request: str) -> "json":
        request = json.loads(json_request)
        node_id = request["node_id"]
        msg     = request["msg"]
        if self.cluster_leader_addr is not None:
            message = {
                'type': 'ForwardRequest',
                'leader_id': self.cluster_leader_addr,
                'msg': msg
            }
            self.send_message(message, self.cluster_leader_addr)
            
    def send_message(self, message, recipient):
        print(f"Sending message to node {recipient}: {message}") 
    
    def periodically(self):
        if self.type == RaftNode.NodeType.LEADER:
            for follower in self.cluster_addr_list:
                if follower != self.address:
                    self.replicate_log(self.address, follower)
    
    def replicate_log(self, leader_id: Address, follower_id: Address):
        prefix_len = [i for i in range(len(self.sent_length)) if self.sent_length[i][0] == follower_id] # sent_length[follower_id]
        
        suffix = self.log[self.sent_length[prefix_len[0]][1]:]
        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len -1][1]
            
        self.__print_log(f"[Leader] Sending replicate_log to {follower_id}")
        request = {
            "leader_id": leader_id,
            "current_term": self.election_term,
            "prefix_len": prefix_len,
            "prefix_term": prefix_term,
            "commit_length": self.acked_length,
            # e number of log entries that have been committed
            "suffix": suffix,
        }
        
        response = self.__send_request(request, "on_receiving", follower_id)
        response = json.loads(response)
        follower = response["node_id"]
        term = response["current_term"]
        ack = response["ack"]
        success = response["success"]
        
        if term == self.election_term and self.type == RaftNode.NodeType.LEADER:
            if success and ack >= self.acked_length[follower]:
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] = self.sent_length[follower] - 1
                self.replicate_log(self.address, follower)
        elif term > self.election_term:
            self.election_term = term
            self.type = RaftNode.NodeType.FOLLOWER
            self.voted_for = None
            self.__cancel_election_timer

    def on_receiving(self, json_request: str) -> "json":
        """ On receiving replicate_log from leader """
        
        request = json.loads(json_request)
        leader_id = request["leader_id"]
        term = request["current_term"]
        prefix_len = request["prefix_len"]
        prefix_term = request["prefix_term"]
        leader_commit = request["commit_length"]
        suffix = request["entries"]
        
        if term > self.election_term:
            self.election_term = term
            self.vote_for = None
            self.__cancel_election_timer()
            
        if term == self.election_term:
            self.type = RaftNode.NodeType.FOLLOWER
            self.cluster_leader_addr = leader_id
        
        log_ok = len(self.log >= prefix_len) and (prefix_len == 0 or self.log[prefix_len - 1][1] == prefix_term)
        
        if term == self.election_term and log_ok:
            self.append_entries(prefix_len, leader_commit, suffix)
            ack = prefix_len + len(suffix)
            success = True
        else:
            ack = 0
            success = False
        
        response = {
            "node_id": self.address,
            "current_term": self.election_term,
            "ack": ack,
            "success": success,
        }
        return json.dumps(response)
        
    def __cancel_election_timer(self):
        self.heartbeat_timer = 0
        self.current_election_timeout = self.__election_timeout()
    
    def append_entries(self, prefix_len, leader_commit, suffix):
        if len(suffix) > 0 and len(self.log):
            index = min(len(self.log), prefix_len + len(suffix) - 1)
            if self.log[index][RaftNode.NodeType.LOG_IDX_TERM] != suffix[index - prefix_len][RaftNode.NodeType.LOG_IDX_TERM]:
                self.log = self.log[:prefix_len - 1]
            
            if prefix_len + len(suffix) > len(self.log):
                for i in range(len(self.log), len(suffix) - 1):
                    self.log.append(suffix[i])

            # TODO: need to declare self.commit length on follower
            if leader_commit > len(self.commit_length):
                for i in range(len(self.log) - prefix_len, len(suffix) - 1):
                    self.log.append(suffix[i])

            if leader_commit > self.commit_length:
                for i in range(self.commit_length, leader_commit - 1):
                    # deliver log[i].msg to the application
                    command = self.log[i][RaftNode.NodeType.LOG_IDX_MSG]

                    request = {
                        'command': command[:7] if 'dequeue' in command else command[:5],
                        'params': command[:9] if 'dequeue' in command else command[:7]
                    }
                    
                    self.execute(json.dumps(request))
                    self.commit_length == leader_commit
    
    def __acks_(self, length):
        return len([i for i in range(len(self.acked_length)) if self.acked_length[i] >= length])
    
    def commit_log_entries(self):
        min_acks = ceil(len(self.cluster_addr_list) + 1 / 2)
        ready = [i for i in range(len(self.log)) if self.__acks_(i) >= min_acks]
        if ready != [] and max(ready) > self.commit_length and self.log[max(ready) - 1][1] == self.election_term:
            for i in range(self.commit_length, max(ready)):
                # deliver log[i].msg to the application
                command = self.log[i][RaftNode.NodeType.LOG_IDX_MSG]
                
                request = {
                    'command': command[:7] if 'dequeue' in command else command[:5],
                    'params': command[:9] if 'dequeue' in command else command[:7]
                }
                
                self.execute(json.dumps(request))
                
            self.commit_length = max(ready)
                
    