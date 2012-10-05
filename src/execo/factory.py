from action import Remote, Put, Get, TaktukRemote, TaktukPut, TaktukGet
from config import configuration
from execo.config import SSH, TAKTUK, SCP
from process import Process, SshProcess

class Factory:

    def __init__(self, remote_tool = None, fileput_tool = None, fileget_tool = None):
        if remote_tool == None:
            remote_tool = configuration.get("remote_tool")
        if remote_tool not in [SSH, TAKTUK]:
            raise KeyError, "no such remote tool: %s" % remote_tool
        if fileput_tool == None:
            fileput_tool = configuration.get("fileput_tool")
        if fileput_tool not in [SCP, TAKTUK]:
            raise KeyError, "no such fileput tool: %s" % fileput_tool
        if fileget_tool == None:
            fileget_tool = configuration.get("fileget_tool")
        if fileget_tool not in [SCP, TAKTUK]:
            raise KeyError, "no such fileget tool: %s" % fileget_tool
        self.remote_tool = remote_tool
        self.fileput_tool = fileput_tool
        self.fileget_tool = fileget_tool

    def process(self, *args, **kwargs):
        if kwargs.get("host") != None:
            return SshProcess(*args, **kwargs)
        else:
            del kwargs["host"]
            if "connexion_params" in kwargs: del kwargs["connexion_params"]
            return Process(*args, **kwargs)

    def remote(self, *args, **kwargs):
        if self.remote_tool == SSH:
            return Remote(*args, **kwargs)
        elif self.remote_tool == TAKTUK:
            return TaktukRemote(*args, **kwargs)

    def fileput(self, *args, **kwargs):
        if self.fileput_tool == SCP:
            return Put(*args, **kwargs)
        elif self.fileput_tool == TAKTUK:
            return TaktukPut(*args, **kwargs)

    def fileget(self, *args, **kwargs):
        if self.fileget_tool == SCP:
            return Get(*args, **kwargs)
        elif self.fileget_tool == TAKTUK:
            return TaktukGet(*args, **kwargs)

_default_factory = Factory()

def get_process(*args, **kwargs):
    return _default_factory.process(*args, **kwargs)

def get_remote(*args, **kwargs):
    return _default_factory.remote(*args, **kwargs)

def get_fileput(*args, **kwargs):
    return _default_factory.fileput(*args, **kwargs)

def get_fileget(*args, **kwargs):
    return _default_factory.fileget(*args, **kwargs)
