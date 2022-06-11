import unittest

from src.raft import Node, Server


class NodeTestCase(unittest.TestCase):
    def test_initialization(self):
        server = Server()
        node = Node(server)
        assert node.current_term == 0
        assert node.voted_for is None
        assert len(node.log) == 0
        assert node.commit_length == 0
        assert node.current_leader is None
        assert len(node.votes_received) == 0
        assert len(node.sent_length) == 0
        assert len(node.acked_length) == 0

    def test_recovery_from_crash(self):
        pass

    def test_heartbeat_timeout(self):
        pass

    def test_election_timeout(self):
        pass
