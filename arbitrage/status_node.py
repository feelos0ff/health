__author__ = 'kkg'
import time
import os


class BaseNode(object):
    def __init__(self, config, update_config=False):

        self._pgdata = os.environ.get("PGDATA")
        self._update_flag = update_config
        self.trigger_file = os.path.join(config.data_path, "postgresql.trigger")
        self.recovery_conf = os.path.join(config.data_path, "recovery.conf")
        self.recovery_done = os.path.join(config.data_path, "recovery.done")

    def commit(self):
        if self._update_flag:
            self._update_config()
            self._pg_restart()
            self._update_flag = False
        time.sleep(2)

    def _update_config(self):
        # override in child class
        pass

    def _pg_restart(self):
        # toDo restart postgres
        pass


class PrimaryNode(BaseNode):
    def __init__(self, config, update_config=False):
        BaseNode.__init__(self, config, update_config=update_config)

    def _update_config(self):
        if not os.path.exists(self.trigger_file):
            open(self.trigger_file, 'a').close()


class StandbyNode(BaseNode):
    def __init__(self, config, update_config=False, primary_iface="localhost", primary_port=5432):
        BaseNode.__init__(self, config, update_config=update_config)

        self._repl_conninfo = "host=%s port=%d user=%s password=%s" % (
            primary_iface,
            primary_port,
            config.repl_user,
            config.repl_password)

        self.primary_iface = primary_iface
        self.primary_port = primary_port

    @staticmethod
    def _remove_files(files):
        for removing_file in files:
            if os.path.exists(removing_file):
                os.remove(removing_file)

    def _update_config(self):

        StandbyNode._remove_files([self.trigger_file, self.recovery_done])

        source = (
            """standby_mode = 'on'"""
            """primary_conninfo = '%s'"""
            """trigger_file = '%s/postgresql.trigger'""") % \
            (self._repl_conninfo, self._pgdata)

        with open(self.recovery_conf, 'w', encoding='utf-8') as f:
            f.write(source)


class TransitionState(BaseNode):
    def __init__(self, config):
        BaseNode.__init__(self, config, update_config=False)