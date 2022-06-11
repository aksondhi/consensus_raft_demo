import random
import uuid
from enum import Enum
from typing import List, Any
from uuid import uuid4

from pydantic import BaseModel

from src.server import Server

HEARTBEAT_TIMEOUT = 3
ELECTION_TIMEOUT = 2


class Role(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class LogMessage(BaseModel):
    term: int
    message: Any


class AppendEntryRequest(BaseModel):
    term: int
    leader_id: int
    entries: List[Any]
    previous_log_index: int
    previous_log_term: int
    leader_commit: int


class VoteRequest(BaseModel):
    candidate_id: uuid.UUID
    term: int
    last_log_index: int
    last_log_term: int


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
        self.sent_length = []
        self.acked_length = []
        self.node_id = uuid4()

        # time tracking
        self.clock = 0
        self.heartbeat_timeout = 0
        self.election_timeout = 0
        self.set_heartbeat_timeout()
        self.set_election_timeout()  # prevents shortcircuit when transitioning from follower to candidate

    def set_heartbeat_timeout(self):
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT * random.randint(1, 10)

    def set_election_timeout(self):
        self.election_timeout = ELECTION_TIMEOUT * random.randint(1, 10)

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

    def tick(self):
        self.clock += 1
        self.heartbeat_timeout -= 1
        if self.current_role is Role.CANDIDATE:
            self.election_timeout -= 1
        if self.heartbeat_timeout <= 0 or (self.current_role is Role.CANDIDATE and self.election_timeout <= 0):
            self.handle_heartbeat_timeout_or_election_timeout()
