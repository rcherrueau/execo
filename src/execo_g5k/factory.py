from execo.config import SSH, SCP
from execo.factory import Factory

frontend_factory = Factory(remote_tool = SSH,
                           fileput_tool = SCP,
                           fileget_tool = SCP)
