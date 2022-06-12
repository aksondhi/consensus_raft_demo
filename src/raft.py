import random
import uuid
from enum import Enum
from typing import List, Any
from uuid import uuid4

from pydantic import BaseModel

from src.server import Server

HEARTBEAT_TIMEOUT = 400
ELECTION_TIMEOUT = 100


class Role(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class LogMessage(BaseModel):
    term: int
    message: Any


class LogRequest(BaseModel):
    term: int
    leader_id: uuid.UUID
    entries: List[LogMessage]
    previous_log_index: int
    previous_log_term: int
    commit_length: int
    to_node: uuid.UUID


class LogResponse(BaseModel):
    node_id: uuid.UUID
    term: int
    success: bool
    acknowledged: int


class VoteRequest(BaseModel):
    candidate_id: uuid.UUID
    term: int
    last_log_index: int
    last_log_term: int


class VoteResponse(BaseModel):
    node_id: uuid.UUID
    candidate_id: uuid.UUID
    term: int
    vote_granted: bool


class Node:
    def __init__(self, server: Server):
        self.server = server
        self.current_term = 0
        self.voted_for = None
        self.log: List[LogMessage] = []
        self.commit_length = 0
        self.current_role = Role.FOLLOWER
        self.current_leader = None
        self.votes_received = {}
        self.sent_length = {}
        self.acked_length = {}
        self.node_id = uuid4()

        # time tracking
        self.clock = 0
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT
        self.election_timeout = ELECTION_TIMEOUT

    def set_heartbeat_timeout(self):
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT * random.randint(1, len(self.server.nodes) * 10)
        if self.current_role is Role.LEADER:
            self.heartbeat_timeout /= 2

    def set_election_timeout(self):
        self.election_timeout = ELECTION_TIMEOUT * random.randint(1, len(self.server.nodes) * 10)

    def replicate_log(self, leader_id, follower_id):
        previous_log_index = self.sent_length[follower_id]
        entries = self.log[previous_log_index:]
        prefix_term = 0
        if previous_log_index > 0:
            prefix_term = self.log[previous_log_index - 1].term
        self.server.broadcast_message(
            LogRequest(
                leader_id=leader_id,
                term=self.current_term,
                previous_log_index=previous_log_index,
                previous_log_term=prefix_term,
                commit_length=self.commit_length,
                entries=entries,
                to_node=follower_id,
            )
        )

    def broadcast_heartbeat(self):
        # this is hacky and can be fundamentally fixed by allowing more direct message sending
        for node in self.get_follower_nodes():
            # node.set_heartbeat_timeout()
            self.server.broadcast_message(
                LogRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    entries=[],
                    previous_log_index=len(self.log),
                    previous_log_term=0 if len(self.log) == 0 else self.log[-1].term,
                    to_node=node.node_id,
                    commit_length=self.commit_length,
                )
            )
        self.set_heartbeat_timeout()

    def broadcast_log_message(self, message):
        if self.current_role is Role.LEADER:
            message_to_send = LogMessage(term=self.current_term, message=message)
            self.log.append(message_to_send)
            self.acked_length[self.node_id] = len(self.log)
            for follower in self.get_follower_nodes():
                if follower.node_id == self.node_id:
                    continue
                self.replicate_log(self.node_id, follower.node_id)

    def append_entries(self, previous_log_index, leader_commit, entries: List[LogMessage]):
        if len(entries) > 0 and len(self.log) > previous_log_index:
            index = min(len(self.log), previous_log_index + len(entries)) - 1
            if self.log[index].term != entries[index - previous_log_index].term:
                self.log = self.log[: previous_log_index - 1]
        if previous_log_index + len(entries) > len(self.log):
            self.log += entries[len(self.log) - previous_log_index :]
        if leader_commit > self.commit_length:
            self.server.add_app_msgs(self.log[self.commit_length : leader_commit - 1])
        self.commit_length = leader_commit

    def handle_log_request(self, message: LogRequest):
        if message.term > self.current_term:
            self.current_term = message.term
            self.voted_for = None
            self.set_election_timeout()
        if message.term == self.current_term:
            self.current_role = Role.FOLLOWER  # effectively pauses election timer
            self.set_election_timeout()  # actually reset election timer
            self.current_leader = message.leader_id
        log_ok = (len(self.log) >= message.previous_log_index) and (
            message.previous_log_index == 0
            or self.log[message.previous_log_index - 1].term == message.previous_log_term
        )
        if message.term == self.current_term and log_ok:
            self.append_entries(message.previous_log_index, message.commit_length, message.entries)
            ack = message.previous_log_index + len(message.entries)
            self.server.broadcast_message(
                LogResponse(node_id=self.node_id, term=self.current_term, acknowledged=ack, success=True)
            )
        else:
            self.server.broadcast_message(
                LogResponse(node_id=self.node_id, term=self.current_term, acknowledged=0, success=False)
            )

    def handle_vote_request(self, message: VoteRequest):
        if message.term > self.current_term:
            self.current_term = message.term
            self.current_role = Role.FOLLOWER
            self.voted_for = None
        last_term = 0
        if len(self.log) > 0:
            last_term = self.log[-1].term
        log_ok = (message.last_log_term > last_term) or (
            message.last_log_term == last_term and message.last_log_index >= len(self.log)
        )

        if message.term == self.current_term and log_ok and self.voted_for in [message.candidate_id, None]:
            self.voted_for = message.candidate_id
            self.server.broadcast_message(
                VoteResponse(
                    node_id=self.node_id,
                    candidate_id=message.candidate_id,
                    term=self.current_term,
                    vote_granted=True,
                )
            )
        else:
            self.server.broadcast_message(
                VoteResponse(
                    node_id=self.node_id,
                    candidate_id=message.candidate_id,
                    term=self.current_term,
                    vote_granted=False,
                )
            )

    def handle_vote_response(self, message: VoteResponse):
        if self.current_role is Role.CANDIDATE and message.term == self.current_term and message.vote_granted:
            self.votes_received[message.node_id] = message.vote_granted
            if (
                len([voter for voter in self.votes_received.keys() if self.votes_received.get(voter)])
                >= (len(self.server.nodes) + 1) / 2
            ):
                self.current_role = Role.LEADER
                self.current_leader = self.node_id
                self.set_election_timeout()  # canceling by ensuring it's not close to 0
                for follower in self.get_follower_nodes():
                    if follower.node_id == self.node_id:
                        continue
                    self.sent_length[follower.node_id] = len(self.log)
                    self.acked_length[follower.node_id] = 0
                    self.replicate_log(self.node_id, follower.node_id)
        elif message.term > self.current_term:
            self.current_term = message.term
            self.current_role = Role.FOLLOWER
            self.voted_for = None
            self.set_election_timeout()
            self.set_heartbeat_timeout()

    def handle_log_response(self, message: LogResponse):
        if message.term == self.current_term and self.current_role is Role.LEADER:
            if message.success and message.acknowledged >= self.acked_length[message.node_id]:
                self.sent_length[message.node_id] = message.acknowledged
                self.acked_length[message.node_id] = message.acknowledged
                self.commit_log_entries()
            elif self.sent_length[message.node_id] > 0:
                self.sent_length[message.node_id] -= 1
                self.replicate_log(self.node_id, message.node_id)
        elif message.term > self.current_term:
            self.current_term = message.term
            self.current_role = Role.FOLLOWER
            self.voted_for = None
            self.set_election_timeout()  # canceling by ensuring it's not close to 0
        self.set_heartbeat_timeout()

    def handle_message(self, message):
        if isinstance(message, VoteRequest) and message.candidate_id != self.node_id:
            self.handle_vote_request(message)
        elif isinstance(message, VoteResponse) and message.candidate_id == self.node_id:
            self.handle_vote_response(message)
        elif isinstance(message, LogRequest) and message.to_node == self.node_id:
            self.handle_log_request(message)
        elif isinstance(message, LogResponse) and message.node_id != self.node_id:
            self.handle_log_response(message)

    def handle_crash(self):
        self.current_role = Role.FOLLOWER
        self.current_leader = None
        self.votes_received = {}
        self.sent_length = []
        self.acked_length = []

    def handle_heartbeat_timeout_or_election_timeout(self):
        self.current_term += 1
        self.current_role = Role.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id: True}
        last_term = 0
        if len(self.log) > 0:
            last_term = self.log[-1].term
        self.server.broadcast_message(
            VoteRequest(
                candidate_id=self.node_id,
                term=self.current_term,
                last_log_index=len(self.log),
                last_log_term=last_term,
            )
        )
        self.set_election_timeout()
        self.set_heartbeat_timeout()

    def tick(self):
        self.clock += 1
        self.heartbeat_timeout -= 1
        if self.current_role is Role.CANDIDATE:
            self.election_timeout -= 1
        if self.current_role is Role.LEADER and self.heartbeat_timeout <= 0:
            self.broadcast_heartbeat()
        elif self.heartbeat_timeout <= 0 or (self.current_role is Role.CANDIDATE and self.election_timeout <= 0):
            self.handle_heartbeat_timeout_or_election_timeout()

    def get_follower_nodes(self):
        return [
            node
            for node in self.server.nodes
            if (node.current_leader == self.node_id or node.voted_for == self.node_id)
            and node.current_term == self.current_term
        ]

    def commit_log_entries(self):
        while self.commit_length < len(self.log):
            acks = 0
            for node in self.get_follower_nodes():
                if self.acked_length[node.node_id] > self.commit_length:
                    acks += 1
            if acks >= (len(self.server.nodes) + 1) / 2:
                self.server.add_app_msgs([self.log[self.commit_length].message])
                self.commit_length += 1
            else:
                break
