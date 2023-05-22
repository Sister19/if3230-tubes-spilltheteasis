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
    ELECTION_TIMEOUT_MIN    = 45
    ELECTION_TIMEOUT_MAX    = 90
    HEARTBEAT_TIMEOUT_MIN   = 30
    HEARTBEAT_TIMEOUT_MAX   = 60
    RPC_TIMEOUT             = 10

    LOG_MSG_IDX             = 0
    LOG_TERM_IDX            = 1

    SEND_ADDR_IDX           = 0
    SEND_LEN_IDX            = 1

    ACK_ADDR_IDX            = 0
    ACK_LEN_IDX             = 1

    class NodeType(Enum):
        LEADER    = 1
        CANDIDATE = 2
        FOLLOWER  = 3
        

    def __init__(self, application : Any, addr: Address, contact_addr: Address = None):
        # Address Info
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address:             Address           = addr
        self.type:                RaftNode.NodeType = RaftNode.NodeType.FOLLOWER
        
        # Cluster
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        
        # Heartbeat
        self.heartbeat_thread:          Thread          = None
        self.heartbeat_timer:           int             = 0
        self.current_hearbeat_timeout:  int             = 0

        # Leader election
        self.election_term:       int                   = 0
        self.election_thread:     Thread                = None
        self.election_timer:      int                   = 0
        self.election_timeout:    int                   = 0
        self.voted_for:           Address               = None
        self.votes:               int                   = 0

        # Log
        self.log:                 List[str, int]        = []
        self.replicate_thread:    Thread                = None

        # Message Queue
        self.app:                 MessageQueue          = application

        self.sent_length:         List[Address, int]    = []
        self.acked_length:        List[Address, int]    = []
        self.commit_length:       int                   = 0

        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.sent_length.append([self.address, 0])
            self.acked_length.append([self.address, 0])
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"{text}")

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
                    Thread(target=self.__send_request, args=[request, "announce_as_leader", addr]).start()
                except:
                    self.__print_log(f"Failed to send announce_as_leader request to {addr}")

        self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()
    
    def __initialize_as_candidate(self):
        self.__print_log("Initialize as candidate node...")
        self.cluster_leader_addr    = None
        self.type                   = RaftNode.NodeType.CANDIDATE
        self.election_term          += 1
        self.voted_for              = self.address
        self.votes                  = 1

        if self.election_thread is None:
            self.election_thread = Thread(target=asyncio.run,args=[self.__leader_election()])
            self.election_thread.start()

    def __initialize_as_follower(self):
        self.__print_log("Initialize as follower node...")
        self.type                       = RaftNode.NodeType.FOLLOWER
        self.heartbeat_timer            = 0
        self.current_hearbeat_timeout   = self.__heartbeat_timeout()
        self.voted_for                  = None
        self.votes                      = 0

        if self.cluster_leader_addr is not None:
            self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__follower_heartbeat()])
            self.heartbeat_thread.start()

    async def __leader_election(self):
        # TODO : Start leader election
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
                    self.__cancel_election_timer()
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
            "candidate_addr": self.address,
            "election_term": self.election_term,
            "log_length": len(self.log),
            "log_term": self.log[-1][1] if len(self.log) > 0 else 0,
        }
        for addr in self.cluster_addr_list:
            if addr != self.address:
                self.__print_log(f"Send vote request to {addr}")
                Thread(target=self.__send_request, args=[request, "vote_request", addr]).start()
    
    def __election_timeout(self):
        return random.uniform(RaftNode.ELECTION_TIMEOUT_MIN, RaftNode.ELECTION_TIMEOUT_MAX)
    
    def __cancel_election_timer(self):
        if self.election_thread is None:
            self.election_thread = Thread(target=asyncio.run,args=[self.__leader_election()])

        self.election_timer = 0
        self.current_election_timeout = self.__election_timeout()

    async def __leader_heartbeat(self):
        # TODO : Send periodic heartbeat
        while self.type == RaftNode.NodeType.LEADER:
            self.__print_log("[Leader] Sending heartbeat...")
            self.__print_log(f'[Leader] {self.log}')
            for addr in self.cluster_addr_list:
                addr = Address(addr["ip"], addr["port"])
                if addr != self.address:
                    self.__print_log(f"[Leader] Heartbeat to {addr}")
                    Thread(target=self.__send_request, args=[self.address, "heartbeat", addr]).start()
                    self.replicate_log(self.address, addr)

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
            self.__print_log(f'[Follower] {self.log}')
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
    
    def __heartbeat_timeout(self):
        return random.uniform(RaftNode.HEARTBEAT_TIMEOUT_MIN, RaftNode.HEARTBEAT_TIMEOUT_MAX)

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        response = self.__send_request(self.address, "apply_membership", redirected_addr)

        while response is None:
            self.__print_log("Failed to contact leader node")
            response = self.__send_request(self.address, "apply_membership", redirected_addr)
            time.sleep(1)

        while response["status"] != "success":
            redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
            response        = self.__send_request(self.address, "apply_membership", redirected_addr)

        self.log                 = response["log"]
        self.cluster_addr_list   = list(map(lambda addr: Address(addr["ip"], addr["port"]), response["cluster_addr_list"]))
        self.cluster_leader_addr = redirected_addr

        self.sent_length.append([self.address, 0])
        self.acked_length.append([self.address, 0])

        for (i, addr) in enumerate(self.cluster_addr_list):
            if addr != self.address and addr != self.cluster_leader_addr:
                self.sent_length.append([addr, response["sent_length"][i]])
                self.acked_length.append([addr, response["acked_length"][i]])
                Thread(target=self.__send_request, args=[self.address, "announce_new_member", addr]).start()
                # print(f"Announce new member to {addr}")

        self.__initialize_as_follower()

    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> "json":
        # Warning : This method is blocking
        try:
            node         = ServerProxy(f"http://{addr.ip}:{addr.port}")
            json_request = json.dumps(request)
            rpc_function = getattr(node, rpc_name)
            response     = json.loads(rpc_function(json_request))
            return response
        except Exception as e:
            self.__print_log(f"Error: {e}")
            self.__print_log(f"Failed to send request to {addr}")  
    
    # Inter-node RPCs for apply membership
    def apply_membership(self, json_request: str) -> "json":
        request = json.loads(json_request)
        # TODO : Implement apply_membership
        if self.type == RaftNode.NodeType.LEADER:
            addr = Address(request["ip"], request["port"])
            self.__print_log("[Leader] Received request for membership")
            self.__print_log(f"[Leader] Adding node {addr} to cluster")
            if addr not in self.cluster_addr_list:
                self.cluster_addr_list.append(addr)
                self.sent_length.append([addr, 0])
                self.acked_length.append([addr, 0])
            
            response = {
                "status": "success",
                "log": self.log,
                "cluster_addr_list": self.cluster_addr_list,
                "sent_length": self.sent_length,
                "acked_length": self.acked_length
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
        addr = Address(request["ip"], request["port"])

        self.__print_log(f"Received announcement from {addr} as new member")

        if addr not in self.cluster_addr_list:
            self.__print_log(f"Adding node {addr} to cluster")
            self.cluster_addr_list.append(addr)
            self.sent_length.append([addr, 0])
            self.acked_length.append([addr, 0])

        response = {
            "status": "success",
        }
        return json.dumps(response)

    # Inter-node RPCs for leader announcement
    def announce_as_leader(self, json_request: str) -> "json":
        request = json.loads(json_request)
        leader_addr = Address(request["cluster_leader_addr"]["ip"], request["cluster_leader_addr"]["port"])
        self.__print_log(f"Received leader announcement from {leader_addr}")
        self.cluster_leader_addr    = leader_addr
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
        # print(f'json request: {json_request}')
        json_request = json.loads(json_request)
        leader_addr = Address(json_request["ip"], json_request["port"])
        response = {
            "heartbeat_response": "success",
            "address":            self.address,
        }
        
        if self.type != RaftNode.NodeType.LEADER: # follower
            self.__print_log("[Follower] Received heartbeat")
            self.heartbeat_timer = 0

        return json.dumps(response)

    # Inter-node RPCs for vote request
    def vote_request(self, json_request: str) -> "json":
        # TODO : Implement vote request
        request  = json.loads(json_request)
        # print(f'vote request: {request}')
        req_addr = Address(request["candidate_addr"]["ip"], request["candidate_addr"]["port"])
        req_log_length = request["log_length"]
        req_log_term   = request["log_term"]

        response = {
            "address":       self.address,
            "election_term": self.election_term,
            "vote_response": "not granted",
        }

        # print(f'leader election term: {self.election_term}')
        # print(f'candidate election term: {request["election_term"]}')

        if request["election_term"] > self.election_term:
            self.election_term  = request["election_term"]
            response["election_term"] = self.election_term
            self.__print_log("Election term updated")
            self.__initialize_as_follower()
            self.__cancel_election_timer()

        last_term = self.log[-1][1] if len(self.log) > 0 else 0

        log_ok = (req_log_term > last_term) or (req_log_term == last_term and req_log_length >= len(self.log))
        
        if request["election_term"] == self.election_term and log_ok and self.voted_for is None:
            self.voted_for = req_addr
            self.__print_log(f"Voted for {self.voted_for}")
            response["vote_response"] = "granted"
        else:
            self.__print_log(f"Refuse voted for {req_addr}")

        # print(f'vote response: {response}')

        Thread(target=self.__send_request, args=[response, "vote_response", req_addr]).start()
        # print(f'after vote response')
        return json.dumps({"status": "ok"})
    
    def vote_response(self, json_request: str) -> "json":
        # print("vote response")
        request         = json.loads(json_request)
        term            = request["election_term"]
        vote_response   = request["vote_response"]
        granted         = vote_response == "granted"

        if self.type ==  RaftNode.NodeType.CANDIDATE and term == self.election_term and granted:
            self.votes += 1

            if self.votes >= len(self.cluster_addr_list) // 2 + 1:
                self.__initialize_as_leader()
                self.__print_log("Elected as leader")
                self.__cancel_election_timer()
                self.sent_length = []
                self.acked_length = []
                for addr in self.cluster_addr_list:
                    if addr != self.address:
                        self.sent_length.append([addr, len(self.log)])
                        self.acked_length.append([addr, 0])
                        Thread(target=self.replicate_log, args=[self.address ,addr]).start()   

        elif term > self.election_term:
            self.election_term = term
            self.__print_log("Election term is not valid")
            self.__initialize_as_follower()
            self.__cancel_election_timer()

        return json.dumps({"status": "ok"})
        
    # Client RPCs
    def execute(self, json_request: str) -> "json":
        # request = json.loads(json_request)

        # command = request["command"]
        # params = request["params"]

        # popped_elmt = None
        # result = None

        # # vote request commit
        # # mayority vote

        # if command == 'enqueue':
        #     self.app.push(params)
        # elif command == 'dequeue':
        #     popped_elmt = self.app.pop()

        # if not popped_elmt:
        #     result = "Success enqueue element"
        # else:
        #     result = f'Success dequeue element {popped_elmt}'

        # response = { "result": result }

        response = self.broadcast_msg(json_request)
        response = json.loads(response)
        
        return json.dumps(response)
    
    def commit(self, json_request: str) -> "json":
        request = json.loads(json_request)

        command = request["command"]
        params = request["params"]

        status = "success"

        if command == 'queue':
            self.app.push(params)
            result = "Success enqueue element"
        elif command == 'dequeue':
            popped_elmt = self.app.pop()
            result = f'Success dequeue element {popped_elmt}'
        else:
            status = "error"
            result = "Invalid command"

        response = {
            "status": status,
            "result": result,
        }

        # print(f'response: {response}')

        return json.dumps(response)
    
    def broadcast_msg(self, json_request: str) -> "json":
        request = json.loads(json_request)
        request_addr    = Address(request["request_addr"]["ip"], request["request_addr"]["port"])
        node_id         = request["node_id"]
        msg             = request["command"]

        node_addr = Address(node_id["ip"], node_id["port"])

        response = {
            "status": "ok",
        }

        if self.type == RaftNode.NodeType.LEADER:
            self.__print_log(f"==================================")
            self.__print_log(f"[Leader] Received request broadcast msg from {request_addr}")
            self.__print_log(f"[Leader] Broadcasting request to all nodes")
            self.__print_log(f"==================================")

            # append the record (msg, term) to the log
            self.log.append((msg, self.election_term))
            idx = [i for i in range(len(self.sent_length)) if self.sent_length[i][RaftNode.SEND_ADDR_IDX] == node_addr][0]
            # print(f"idx: {idx}")
            self.acked_length[idx][RaftNode.SEND_LEN_IDX] = len(self.log)
            
            i = 0
            for follower in self.cluster_addr_list:
                if follower != self.address:
                    self.replicate_log(self.address, follower)
                i += 1
            
            if 'request_log' in msg:
                log = []
                for entry in self.log:
                    log.append(entry[0])
                response["log"] = log

        # forward the request to the leader via a fifo link
        else:
            self.__print_log(f"forwarding request to leader {self.cluster_leader_addr}")
            response = self.forward_request(json_request)
        
        return json.dumps(response)
            
    def forward_request(self, json_request: str):
        request = json.loads(json_request)
        node_id = request["node_id"]
        request_addr = request["request_addr"]
        msg     = request["command"]

        if self.cluster_leader_addr is not None:
            message = {
                'type': 'ForwardRequest',
                'leader_id': self.cluster_leader_addr,
                'command': msg,
                'node_id': node_id,
                'request_addr': request_addr
            }
            # print(type(self.cluster_leader_addr))
            response = self.send_message(message, self.cluster_leader_addr)
            return response
        
        return json.dumps({"status": "failed", "message": "No leader found"})
            
    def send_message(self, message: Any, recipient: Address):
        
        print(f"Sending message to node {recipient}")
        response = self.__send_request(message, "broadcast_msg", recipient)
        print(f"Response from node {recipient}: {response}")

        return response
    
    def replicate_log(self, leader_id: Address, follower_id: Address):
        # if (self.type == RaftNode.NodeType.LEADER):
        # print('masuk replicate log')
        # find sent_length with address follower_id
        idx = [i for i in range(len(self.sent_length)) if self.sent_length[i][int(RaftNode.SEND_ADDR_IDX)] == follower_id][0]
        prefix_len = self.sent_length[idx][RaftNode.SEND_LEN_IDX]
        suffix = self.log[prefix_len:]

        # print(f'prefix_len: {prefix_len}')
        # print(f"self.log: {self.log}")
        # print(f'suffix: {suffix}')

        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len - 1][1]
            
        # self.__print_log(f"[Leader] Sending replicate_log to {follower_id}")
        request = {
            "leader_id": leader_id,
            "current_term": self.election_term,
            "prefix_len": prefix_len,
            "prefix_term": prefix_term,
            "commit_length": len(self.log),
            # e number of log entries that have been committed
            "suffix": suffix,
        }
        # print('before on receiving---------------')
        Thread(target=self.__send_request, args=[request, "log_request", follower_id]).start()

    def log_request(self, json_request: str) -> "json":
        request = json.loads(json_request)
        leader_id = request["leader_id"]
        term = request["current_term"]
        prefix_len = request["prefix_len"]
        prefix_term = request["prefix_term"]
        leader_commit = request["commit_length"]
        suffix = request["suffix"]

        # print(f'leader_id: {leader_id}')
        # print(f'term: {term}')
        # print(f'prefix_len: {prefix_len}')
        # print(f'prefix_term: {prefix_term}')
        # print(f'leader_commit: {leader_commit}')
        # print(f'suffix: {suffix}')

        # print("---here 1---")
        if term > self.election_term:
            self.election_term = term
            self.vote_for = None
            self.__cancel_election_timer()
        # print("---here 2---")
        if term == self.election_term:
            self.type = RaftNode.NodeType.FOLLOWER
            self.cluster_leader_addr = Address(leader_id["ip"], leader_id["port"])
        # print("---here 3---")
        log_ok = (len(self.log) >= prefix_len) and (prefix_len == 0 or self.log[prefix_len - 1][1] == prefix_term)
        # print("---here 4---")
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
        # print("---here 5---")
        # print('before send log response')
        # print(f'leader id type {type(leader_id)}')
        # print(f'leader id {leader_id}')
        # print(f'response {response}')
        Thread(target=self.__send_request, args=[response, "log_response", Address(leader_id['ip'], leader_id['port'])]).start()
        # print('after log response')
        return json.dumps({'status': 'ok'})
    
    def append_entries(self, prefix_len, leader_commit, suffix):
        # print('masuk append entries')
        # print('here 4.1 -------------------')
        if len(suffix) > 0 and len(self.log) > prefix_len:
            # print('before index ------------------------')
            index = min(len(self.log), prefix_len + len(suffix) - 1)
            # print('after index ------------------------')
            if self.log[index][RaftNode.LOG_TERM_IDX] != suffix[index - prefix_len][RaftNode.LOG_TERM_IDX]:
                # print("masuk if 1")
                self.log = self.log[:prefix_len - 1]
            
        # print("otw masuk if 2")
        # print(f'prefix len: {prefix_len}')
        # print(f'len suffix: {len(suffix)}')
        if prefix_len + len(suffix) > len(self.log):
            # print("masuk if 2")
            for i in range(len(self.log), len(suffix)):
                # print(suffix[i], end='. ')
                self.log.append(suffix[i])
            # print('here 4.2 -------------------')
            # TODO: need to declare self.commit length on follower
        if leader_commit > self.commit_length:
            # print("masuk print 3")
            for i in range(len(self.log) - prefix_len, len(suffix)):
                self.log.append(suffix[i])
        # print('here 4.3 -------------------')
        # print(type(leader_commit))
        # print(f'leader commit: {leader_commit}')
        # print(f'commit length: {self.commit_length}')
        if leader_commit > self.commit_length:
            # print('here 4.4 -------------------')
            for i in range(self.commit_length, leader_commit - 1):
                print(i, end='. ')
                # deliver log[i].msg to the application
                command = self.log[i][RaftNode.LOG_MSG_IDX]

                # print(f"Committing log {command} to application")

                request = {
                    'command': command[:7] if 'dequeue' in command else command[:5],
                    'params': command[9:-2] if 'dequeue' in command else command[7:-2]
                }

                # print(f"Committing log {request} to application")
                self.commit(json.dumps(request))

            self.commit_length = leader_commit

    def log_response(self, json_request: str) -> "json":
        # print('masuk log response -------------------')
        request = json.loads(json_request)
        follower = request["node_id"]
        follower = Address(follower['ip'], follower['port'])
        term = request["current_term"]
        ack = request["ack"]
        success = request["success"]
        # print('before if')
        if term == self.election_term and self.type == RaftNode.NodeType.LEADER:
            # print('masuk if')
            # print(f'follower: {follower}')
            # print(f'self.acked_length: {self.acked_length}')
            # find item in self.acked_length where address is follower
            # self.acked_length[i][0] get the address
            followerIdx = [i for i in range(len(self.acked_length)) if self.acked_length[i][RaftNode.ACK_ADDR_IDX] == follower][0]
            # if followerIdx is None:
            #     pass
            # else:
            #     followerIdx = followerIdx[0]
            # print(f'follower idx: {followerIdx}')
            if success and ack >= self.acked_length[followerIdx][RaftNode.ACK_LEN_IDX]:
                self.sent_length[followerIdx][RaftNode.SEND_LEN_IDX] = ack
                self.acked_length[followerIdx][RaftNode.ACK_LEN_IDX] = ack
                # print('before commit log entries')
                self.commit_log_entries()

            elif self.sent_length[followerIdx][RaftNode.SEND_LEN_IDX] > 0:
                self.sent_length[followerIdx][RaftNode.SEND_LEN_IDX] -= 1
                # print('before replicate log')
                self.replicate_log(self.address, follower)
        elif term > self.election_term:
            self.election_term = term
            self.type = RaftNode.NodeType.FOLLOWER
            self.voted_for = None
            # print('follower term is bigger than leader term')
            self.__cancel_election_timer
        # print('log_response: after if')

        return json.dumps({'status': 'ok'})
    
    def __acks_(self, length):
        return len([i for i in range(len(self.acked_length)) if self.acked_length[i][RaftNode.ACK_LEN_IDX] >= length])
    
    def commit_log_entries(self):
        # print('entering commit log entries')
        min_acks = ceil((len(self.cluster_addr_list) + 1) / 2)
        # print(f"min_acks: {min_acks}")
        # print(f"self.log: {self.log}")
        ready = [i for i in range(len(self.log)) if self.__acks_(i) >= min_acks]
        
        # print(f"ready: {ready}")
        # print("before if ready")
        # print(f'commit length {self.commit_length}')
        # print(f"self.election_term: {self.election_term}")
        if ready != [] and max(ready) + 1 > self.commit_length and self.log[max(ready)][1] == self.election_term:
            print("after if ready")
            for i in range(self.commit_length, max(ready) + 1):
                # deliver log[i].msg to the application
                command = self.log[i][RaftNode.LOG_MSG_IDX]
  
                request = {
                    'command': command[:7] if 'dequeue' in command else command[:5],
                    'params': command[9:-2] if 'dequeue' in command else command[7:-2]
                }
                # print('before commit in commit log entries')
                self.commit(json.dumps(request))
                # print("after self.commit(json.dumps(request))")
                
            self.commit_length = max(ready) + 1
            # print("commit length: ", self.commit_length)
                
    