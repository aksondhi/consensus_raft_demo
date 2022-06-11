import random
from enum import Enum

from src.server import Server

HEARTBEAT_TIMEOUT = 3
ELECTION_TIMEOUT = 2


class Role(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class Node:
    def __init__(self, server: Server):
        self.server = server
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_length = 0
        self.current_role = Role.FOLLOWER
        self.current_leader = None
        self.votes_received = {}
        self.sent_length = []
        self.acked_length = []

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
