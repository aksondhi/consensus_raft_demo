class Server:
    def __init__(self):
        self.nodes = []
        self.message_queue = []
        self.app_messages = []

    def __distribute_message(self):
        if self.message_queue:
            message = self.message_queue.pop(0)
            for node in self.nodes:
                node.handle_message(message)

    def broadcast_message(self, message):
        self.message_queue.append(message)

    def __tick_clock(self):
        for node in self.nodes:
            node.tick()

    def iterate(self):
        self.__distribute_message()
        self.__tick_clock()

    def add_node(self, node):
        self.nodes.append(node)

    def add_app_msgs(self, messages):
        self.app_messages += messages
