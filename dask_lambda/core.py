

from distributed.deploy import Cluster

class LambdaCluster(Cluster):
    def __init__(self, lambda_arn):
        self.lambda_arn = lambda_arn

    def scale(self):
        pass

