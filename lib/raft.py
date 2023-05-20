import asyncio
from threading     import Thread
from xmlrpc.client import ServerProxy
from typing        import Any, List
from enum          import Enum
from .struct.address       import Address
from math         import ceil
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
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address:             Address           = addr
        self.type:                RaftNode.NodeType = RaftNode.NodeType.FOLLOWER
        self.log:                 List[str, str]    = []
        self.app:                 Any               = application
        self.election_term:       int               = 0
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        self.heartbeat_thread:    Thread            = None
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)
        # self.commit.length: int = 0

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
    
    def __iniitialize_as_candidate(self):
        self.__print_log("Initialize as candidate node...")
        self.cluster_leader_addr = None
        self.type = RaftNode.NodeType.CANDIDATE
        self.election_term += 1
        self.current_election_timeout = self.__election_timeout()
        self.voted_for = self.address
        self.votes = 1

        self.__send_vote_request()

    def __initialize_as_follower(self):
        if self.type == RaftNode.NodeType.LEADER:
            self.__print_log("Demote to follower node...")
            self.heartbeat_thread.join()

        self.__print_log("Initialize as follower node...")
        self.type = RaftNode.NodeType.FOLLOWER
        self.heartbeat_timer = 0
        self.current_election_timeout = self.__election_timeout()
        self.voted_for = None
        self.votes = 0

        self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__follower_heartbeat()])
        self.heartbeat_thread.start()

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
        self.current_election_timeout = self.__election_timeout()
        while self.type == RaftNode.NodeType.FOLLOWER:
            self.heartbeat_timer += RaftNode.HEARTBEAT_INTERVAL
            if self.heartbeat_timer >= self.current_election_timeout:
                self.__print_log("Election timeout")
                self.__iniitialize_as_candidate()
                return
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

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
                self.__send_request(self.address, "announce_new_member", redirected_addr)

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
        
    
    def __send_vote_request(self):
        # TODO : Send vote request to all node
        # while self.cluster_leader_addr is None:
        print("Sending vote request...")
        request = {
            "election_term": self.election_term,
            "candidate_addr": self.address
        }
        for addr in self.cluster_addr_list:
            if addr != self.address:
                response = self.__send_request(request, "vote_request", addr)
                if response is not None:
                    if response["vote_response"] == "granted":
                        self.votes += 1
    
        if self.votes >= len(self.cluster_addr_list) // 2 + 1:
            self.__initialize_as_leader()
            self.__print_log("Elected as leader")
        else:
            self.__initialize_as_follower()
            self.__print_log("Failed to be elected as leader")
    
    def __election_timeout(self):
        return random.uniform(RaftNode.ELECTION_TIMEOUT_MIN, RaftNode.ELECTION_TIMEOUT_MAX)
    
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
        self.cluster_leader_addr = Address(request["ip"], request["port"])
        self.cluster_addr_list   = list(map(lambda addr: Address(addr["ip"], addr["port"]), request["cluster_addr_list"]))
        self.log                 = request["log"]
        self.type               = RaftNode.NodeType.FOLLOWER
        # self.__initialize_as_follower()
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
        
        # # send heartbeat to all nodes
        # for addr in self.cluster_addr_list:
        #     if addr != self.address:
        #         try:
        #             self.__send_request(response, "heartbeat", addr)
        #         except:
        #             self.__print_log(f"[Leader] Node {addr} is not responding")

        return json.dumps(response)

    # Inter-node RPCs for vote request
    def vote_request(self, json_request: str) -> "json":
        # TODO : Implement vote request
        request = json.loads(json_request)
        response = {
            "vote_response": "granted",
            "address":       self.address,
        }
        if self.type == RaftNode.NodeType.LEADER:
            self.__print_log("[Leader] Received vote request")
        
        if self.type == RaftNode.NodeType.FOLLOWER or self.type == RaftNode.NodeType.CANDIDATE:
            self.__print_log("Received vote request")
            if self.voted_for is None:
                self.voted_for = request["candidate_addr"]
                self.__print_log(f"Voted for {self.voted_for}")
            elif self.election_term > request["election_term"]:
                self.__print_log(f"Received vote request from higher term {request['election_term']}")
                response["vote_response"] = "not granted"
            else:
                self.__print_log(f"Already voted for {self.voted_for}")
                response["vote_response"] = "not granted"
        
        return json.dumps(response)

    # Client RPCs
    def execute(self, json_request: str) -> "json":
        request = json.loads(json_request)
        # TODO : Implement execute
        command = request["command"]
        result = self.app.execute(command)
        response = { "result": result }
        
        return json.dumps(response)
    
    # # replication log bohongan
    # def append_log_entry(self, term: int, message: str):
    #     entry = (term, message)
    #     self.log.append(entry)
    #     # TODO: Replicate log entry across the cluster

    # def replicate_log_entries(self, start_index:int):
    #     entries = self.log[start_index:]
    #     #TODO: send replicate request to follow log entries

    # def commit_log_entries(self):
    #     min_acks = (len(self.cluster_addr_list) + 1) // 2
    #     ready_entries = [(i, entry) for i, entry in enumerate(self.log, start=1)
    #                      if self.acks(i) >= min_acks]
        
    #     if ready_entries and max(ready_entries)[0] > self.commit_log_entries and ready_entries[-1][1][0] == self.election_term:
    #         for i in range(self.commit_length, max(ready_entries)[0]):
    #             self.deliver_log_entry(self.log[i]][1])
    #         self.commit_log_entries = max(ready_entries)[0]

    # def deliver_log_entry(self,message: str):
    #     #TODO: Deliver log entry to the application
    #     self.app.process_log_entry(message)

    # def acks(self, length: int) -> int:
    #     return sum(1 for node in self.cluster_addr_list if self.acked_length[node] >= length)