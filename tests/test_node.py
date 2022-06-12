import random
import unittest
from uuid import uuid4

from src.raft import Node, Server, Role, VoteRequest, LogMessage, VoteResponse, LogRequest, LogResponse


class NodeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.server = Server()

    def test_initialization(self):
        node = Node(self.server)
        assert node.current_term == 0
        assert node.voted_for is None
        assert len(node.log) == 0
        assert node.commit_length == 0
        assert node.current_role is Role.FOLLOWER
        assert node.current_leader is None
        assert len(node.votes_received) == 0
        assert len(node.sent_length) == 0
        assert len(node.acked_length) == 0

    def test_recovery_from_crash(self):
        node = Node(self.server)
        node.current_role = Role.LEADER
        node.current_leader = node.node_id
        node.votes_received = {"some_key": "some_value"}
        node.sent_length = {"some_other_key": "some_other_value"}
        node.acked_length = {"another_key": "another_value"}

        node.handle_crash()
        assert node.current_role is Role.FOLLOWER
        assert node.current_leader is None
        assert len(node.votes_received) == 0
        assert len(node.sent_length) == 0
        assert len(node.acked_length) == 0

    def test_clock_ticks(self):
        node = Node(self.server)
        initial_heartbeat_timeout = node.heartbeat_timeout
        node.tick()
        assert node.clock == 1
        assert initial_heartbeat_timeout - 1 == node.heartbeat_timeout

    def test_heartbeat_timeout(self):
        node = Node(self.server)
        self.server.add_node(node)
        node.heartbeat_timeout = 1
        node.tick()
        assert node.current_term == 1
        assert node.current_role is Role.CANDIDATE
        assert node.voted_for == node.node_id
        assert node.votes_received == {node.node_id: True}
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteRequest)
        assert self.server.message_queue[0].candidate_id == node.node_id
        assert self.server.message_queue[0].term == node.current_term
        assert self.server.message_queue[0].last_log_index == len(node.log)
        assert self.server.message_queue[0].last_log_term == 0
        assert node.election_timeout > 0
        assert node.heartbeat_timeout > 0

    def test_heartbeat_timeout_with_log_data(self):
        node = Node(self.server)
        self.server.add_node(node)
        node.heartbeat_timeout = 1
        node.log = [LogMessage(term=random.randint(1, 100), message="some_message")]
        node.current_term = node.log[-1].term
        node.tick()
        assert node.current_term == node.log[-1].term + 1
        assert node.current_role is Role.CANDIDATE
        assert node.voted_for == node.node_id
        assert node.votes_received == {node.node_id: True}
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteRequest)
        assert self.server.message_queue[0].candidate_id == node.node_id
        assert self.server.message_queue[0].term == node.current_term
        assert self.server.message_queue[0].last_log_index == len(node.log)
        assert self.server.message_queue[0].last_log_term == node.log[-1].term
        assert node.election_timeout > 0
        assert node.heartbeat_timeout > 0

    def test_election_timeout(self):
        node = Node(self.server)
        self.server.add_node(node)
        node.election_timeout = 1
        node.current_role = Role.CANDIDATE
        node.tick()
        assert node.current_term == 1
        assert node.current_role is Role.CANDIDATE
        assert node.voted_for == node.node_id
        assert node.votes_received == {node.node_id: True}
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteRequest)
        assert self.server.message_queue[0].candidate_id == node.node_id
        assert self.server.message_queue[0].term == node.current_term
        assert self.server.message_queue[0].last_log_index == len(node.log)
        assert self.server.message_queue[0].last_log_term == 0
        assert node.election_timeout > 0
        assert node.heartbeat_timeout > 0

    def test_election_timeout_with_log_data(self):
        node = Node(self.server)
        self.server.add_node(node)
        node.current_role = Role.CANDIDATE
        node.election_timeout = 1
        node.log = [LogMessage(term=random.randint(1, 100), message="some_message")]
        node.current_term = node.log[-1].term
        node.tick()
        assert node.current_term == node.log[-1].term + 1
        assert node.current_role is Role.CANDIDATE
        assert node.voted_for == node.node_id
        assert node.votes_received == {node.node_id: True}
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteRequest)
        assert self.server.message_queue[0].candidate_id == node.node_id
        assert self.server.message_queue[0].term == node.current_term
        assert self.server.message_queue[0].last_log_index == len(node.log)
        assert self.server.message_queue[0].last_log_term == node.log[-1].term
        assert node.election_timeout > 0
        assert node.heartbeat_timeout > 0

    def test_election(self):
        node1 = Node(self.server)
        node2 = Node(self.server)
        self.server.add_node(node1)
        self.server.add_node(node2)

        # node1 requests a vote
        node1.heartbeat_timeout = 1
        self.server.iterate()
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteRequest)
        assert self.server.message_queue[0].term == 1
        assert self.server.message_queue[0].candidate_id == node1.node_id
        assert self.server.message_queue[0].last_log_index == 0
        assert self.server.message_queue[0].last_log_term == 0

        # only node2 responds with vote
        self.server.iterate()
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteResponse)
        assert self.server.message_queue[0].term == node1.current_term == node2.current_term
        assert self.server.message_queue[0].node_id == node2.node_id
        assert self.server.message_queue[0].candidate_id == node1.node_id
        assert self.server.message_queue[0].vote_granted

        # node 1 accepts and replicates log
        self.server.iterate()

        assert len(self.server.message_queue) == 1
        assert node1.current_role is Role.LEADER
        assert node1.current_leader is node1.node_id
        assert isinstance(self.server.message_queue[0], LogRequest)
        assert self.server.message_queue[0].previous_log_index == 0
        assert self.server.message_queue[0].previous_log_term == 0
        assert self.server.message_queue[0].commit_length == 0

        # node2 acknowledges 0
        self.server.iterate()
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], LogResponse)
        assert node2.current_leader is node1.node_id
        assert self.server.message_queue[0].node_id == node2.node_id
        assert self.server.message_queue[0].term == 1
        assert self.server.message_queue[0].success
        assert self.server.message_queue[0].acknowledged == 0

    def test_election_already_voted(self):
        # node2 is leader
        node1 = Node(self.server)
        node2 = Node(self.server)
        self.server.add_node(node1)
        self.server.add_node(node2)
        node2.current_term = 1
        node2.voted_for = uuid4()

        # node1 requests a vote
        node1.heartbeat_timeout = 1
        self.server.iterate()
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteRequest)
        assert self.server.message_queue[0].term == 1
        assert self.server.message_queue[0].candidate_id == node1.node_id
        assert self.server.message_queue[0].last_log_index == 0
        assert self.server.message_queue[0].last_log_term == 0

        # only node2 responds with vote
        self.server.iterate()
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], VoteResponse)
        assert self.server.message_queue[0].term == node1.current_term == node2.current_term
        assert self.server.message_queue[0].node_id == node2.node_id
        assert self.server.message_queue[0].candidate_id == node1.node_id
        assert not self.server.message_queue[0].vote_granted

    def test_broadcast_log_message_consensus(self):
        node1 = Node(self.server)
        node2 = Node(self.server)
        self.server.add_node(node1)
        self.server.add_node(node2)

        node1.current_role = Role.LEADER
        node1.current_leader = node1.node_id
        node1.current_term = 1
        node1.acked_length = {node2.node_id: 0}
        node1.sent_length = {node2.node_id: 0}

        node2.current_leader = node1.node_id
        node2.current_term = 1
        self.server.message_queue = [LogResponse(node_id=node2.node_id, term=1, success=True, acknowledged=0)]

        # leader processes LogResponse
        node1.broadcast_log_message(LogMessage(term=1, message="hello"))
        self.server.iterate()
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], LogRequest)

        self.server.iterate()
        assert len(self.server.message_queue) == 1
        assert isinstance(self.server.message_queue[0], LogResponse)

        self.server.iterate()
        assert len(self.server.app_messages) == 1
        assert isinstance(self.server.app_messages[0], LogMessage)
        assert self.server.app_messages[0].term == 1
        assert self.server.app_messages[0].message == "hello"

    def test_consensus_in_random_set_of_nodes(self):
        nodes = [Node(self.server) for i in range(random.randint(1, 100))]
        for node in nodes:
            self.server.add_node(node)

        while not any([node.current_role is Role.LEADER for node in nodes]):
            self.server.iterate()
        leader = [node for node in nodes if node.current_role is Role.LEADER][0]
        message = LogMessage(term=leader.current_term, message="Hello World!")
        leader.broadcast_log_message(message)
        self.server.iterate()

        while not self.server.app_messages:
            self.server.iterate()
        assert len(self.server.app_messages) == 1
        assert message == self.server.app_messages[0]
