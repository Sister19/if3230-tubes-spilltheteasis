import asyncio
from threading     import Thread
from xmlrpc.client import ServerProxy
from typing        import Any, List
from enum          import Enum
from .struct.address       import Address
import socket
import time
import json
import random


class RaftNode:
    HEARTBEAT_INTERVAL   = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT          = 0.5
    REQUEST_TIMEOUT      = 0.5

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
        self.type = RaftNode.NodeType.CANDIDATE
        self.election_term += 1
        self.current_election_timeout = self.__election_timeout()
        self.voted_for = self.address
        self.votes = 1

        self.__send_vote_request()

    def __initialize_as_follower(self):
        self.__print_log("Initialize as follower node...")
        self.type = RaftNode.NodeType.FOLLOWER
        self.heartbeat_timer = 0
        self.current_election_timeout = self.__election_timeout()
        self.voted_for = None
        self.votes = 0

        self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__follower_hearbeat()])
        self.heartbeat_thread.start()

    async def __leader_heartbeat(self):
        # TODO : Send periodic heartbeat
        while self.type == RaftNode.NodeType.LEADER:
            self.__print_log("[Leader] Sending heartbeat...")
            ack = 0
            for addr in self.cluster_addr_list:
                addr = Address(addr["ip"], addr["port"])
                if addr != self.address:
                    try:
                        response = self.__send_request(None, "heartbeat", addr)
                        if response["heartbeat_response"] == "ack":
                            ack += 1
                    except socket.timeout:
                        self.__print_log(f"[Leader] Node {addr} is not responding")

            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
    
    async def __follower_hearbeat(self):
        # TODO : Listen for heartbeat
        self.heartbeat_timer = 0
        self.current_election_timeout = self.__election_timeout()
        while self.type == RaftNode.NodeType.FOLLOWER:
            self.heartbeat_timer += RaftNode.HEARTBEAT_INTERVAL
            if self.heartbeat_timer >= self.current_election_timeout:
                self.__print_log("Election timeout")
                self.__iniitialize_as_candidate()
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        response = {
            "status": "redirected",
            "address": {
                "ip":   contact_addr.ip,
                "port": contact_addr.port,
            }
        }
        while response["status"] != "success":
            redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
            response        = self.__send_request(self.address, "apply_membership", redirected_addr)

        self.log                 = response["log"]
        self.cluster_addr_list   = list(map(lambda addr: Address(addr["ip"], addr["port"]), response["cluster_addr_list"]))
        self.cluster_leader_addr = redirected_addr

        self.__initialize_as_follower()

    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> "json":
        # Warning : This method is blocking
        node         = ServerProxy(f"http://{addr.ip}:{addr.port}")
        json_request = json.dumps(request)
        rpc_function = getattr(node, rpc_name)
        response     = json.loads(rpc_function(json_request))
        self.__print_log(response)
        # await asyncio.sleep(RaftNode.REQUEST_TIMEOUT)
        return response
    
    def __send_vote_request(self):
        # TODO : Send vote request to all node
        print("Sending vote request...")
        request = {
            "election_term": self.election_term,
            "candidate_addr": self.address
        }
        for addr in self.cluster_addr_list:
            if addr != self.address:
                try:
                    response = self.__send_request(request, "vote_request", addr)
                    if response["vote_response"] == "ack":
                        self.votes += 1
                except socket.timeout:
                    self.__print_log(f"Node {addr} is not responding")
                    self.cluster_addr_list.remove(addr)
        
        if self.votes >= len(self.cluster_addr_list) // 2 + 1:
            self.__initialize_as_leader()
            self.__print_log("Elected as leader")
        else:
            self.__initialize_as_follower()
    
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

    # Inter-node RPCs for leader announcement
    def announce_as_leader(self, json_request: str) -> "json":
        request = json.loads(json_request)
        # TODO : Implement leader_announce
        if self.type == RaftNode.NodeType.CANDIDATE:
            self.__print_log("[Candidate] Received leader announcement")
            if self.election_term <= request["election_term"]:
                self.__print_log(f"[Candidate] Demoting self to follower")
                response = {
                    "status": "success",
                }
                self.__initialize_as_follower()
                self.cluster_leader_addr = Address(request["ip"], request["port"])
                self.cluster_addr_list   = list(map(lambda addr: Address(addr["ip"], addr["port"]), request["cluster_addr_list"]))
                self.log                 = request["log"]
            else:
                self.__print_log(f"[Candidate] Ignoring leader announcement from {request}")
                response = {
                    "status": "failure",
                }
        else:
            self.__print_log(f"[Follower] Received leader announcement from {request}")
            self.cluster_addr_list   = []
            self.cluster_addr_list   = list(map(lambda addr: Address(addr["ip"], addr["port"]), request["cluster_addr_list"]))
            self.log                 = request["log"]
            response = {
                "status": "success",
            }
            self.__initialize_as_follower()
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
        
        # send heartbeat to all nodes
        for addr in self.cluster_addr_list:
            if addr != self.address:
                try:
                    self.__send_request(response, "heartbeat", addr)
                except:
                    self.__print_log(f"[Leader] Node {addr} is not responding")
                    self.__print_log(f"[Leader] Removing node {addr} from cluster")
                    self.cluster_addr_list.remove(addr)

        return json.dumps(response)

    # Inter-node RPCs for vote request
    def vote_request(self, json_request: str) -> "json":
        # TODO : Implement vote request
        request = json.loads(json_request)
        response = {
            "vote_response": "ack",
            "address":       self.address,
        }
        if self.type == RaftNode.NodeType.LEADER:
            self.__print_log("[Leader] Received vote request")
            return json.dumps(response)
        
        if self.type == RaftNode.NodeType.FOLLOWER:
            self.__print_log("[Follower] Received vote request")
            if self.voted_for is None:
                self.voted_for = request["candidate_addr"]
                self.__print_log(f"[Follower] Voted for {self.voted_for}")
                return json.dumps(response)
            elif self.election_term > request["election_term"]:
                self.__print_log(f"[Candidate] Received vote request from higher term {request['election_term']}")
                response["vote_response"] = "nack"
                return json.dumps(response)
            else:
                self.__print_log(f"[Follower] Already voted for {self.voted_for}")
                response["vote_response"] = "nack"
                return json.dumps(response)
        
        if self.type == RaftNode.NodeType.CANDIDATE:
            self.__print_log("[Candidate] Received vote request")
            if self.voted_for is None:
                self.voted_for = request["candidate_addr"]
                self.__print_log(f"[Candidate] Voted for {self.voted_for}")
                return json.dumps(response)
            elif self.election_term > request["election_term"]:
                self.__print_log(f"[Candidate] Received vote request from higher term {request['election_term']}")
                response["vote_response"] = "nack"
                return json.dumps(response)
            else:
                self.__print_log(f"[Candidate] Already voted for {self.voted_for}")
                response["vote_response"] = "nack"
                return json.dumps(response)

    # Client RPCs
    def execute(self, json_request: str) -> "json":
        request = json.loads(json_request)
        # TODO : Implement execute
        command = request["command"]
        result = self.app.execute(command)
        response = { "result": result }
        
        return json.dumps(response)