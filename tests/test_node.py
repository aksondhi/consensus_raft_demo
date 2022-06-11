import unittest
from src.raft import Node, Server, Role


class NodeTestCase(unittest.TestCase):
    def test_initialization(self):
        server = Server()
        node = Node(server)
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
        server = Server()
        node = Node(server)
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
        server = Server()
        node = Node(server)
        initial_heartbeat_timeout = node.heartbeat_timeout
        node.tick()
        assert node.clock == 1
        assert initial_heartbeat_timeout - 1 == node.heartbeat_timeout

    def test_heartbeat_timeout(self):
        pass

    def test_election_timeout(self):
        pass
