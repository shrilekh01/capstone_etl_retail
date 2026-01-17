class BaseConnector:
    def connect(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
