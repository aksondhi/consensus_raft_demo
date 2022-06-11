import random
import unittest
from src.raft import Node, Server, Role, VoteRequest, LogMessage


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
