class Delivery:
    """One-shot happy-path sender; persistent attempts/redrive are new work."""
    def __init__(self,loader):self.loader=loader;self.results=[]
    def send(self,event):
        result=self.loader(event);self.results.append(result);return result
