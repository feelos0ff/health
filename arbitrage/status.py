__author__ = 'kkg'
from status_node import PrimaryNode, StandbyNode, TransitionState
import psycopg2
import requests
import base64
import json
import time
import os


def test_connection(host, port, user, password, db):
    for _ in xrange(10):
        time.sleep(1)
        try:
            psycopg2.connect("host=%s port=%d user=%s password=%s dbname=%s" %
                             (host, port, user, password, db))
            return True
        except:
            print ("can't access to %s:%d dbname %s with user %s" % (host, port, db, user))
    return False


class Config(object):
    def __init__(self):
        self.bound_iface = ""
        self.bound_port = ""

        self.user = ""
        self.password = ""

        self.repl_user = ""
        self.repl_password = ""
        self.data_path = ""

        self.cluster_name = ""


class NodeStatus(object):
    def __init__(self, conf, consul_connect="http://localhost:8500"):
        self._config = conf
        self._old_primary_iface = ""
        self._old_primary_port = ""

        self._current_primary_iface = ""
        self._current_primary_port = ""

        self._consul_connect = os.path.join(consul_connect, 'v1', 'kv', self._config.cluster_name, "primary")
        self._modified_index = 0

    def get_current_primary(self):
        try:
            response = requests.get(self._consul_connect)
            value = json.loads(response.content)
            primary_connect = base64.b64decode(value[0]["Value"])

            self._modified_index = value[0]["ModifyIndex"]
            self._current_primary_iface, self._current_primary_port = primary_connect.split(':')
            self._current_primary_port = int(self._current_primary_port)

            return response.status_code

        except Exception as e:
            print (e)
            return 504

    def _is_primary(self):
        return (
            self._current_primary_iface == self._config.bound_iface and
            self._current_primary_port == self._config.bound_port)

    def _is_migrated_primary(self):
        return not (
            self._current_primary_iface == self._old_primary_iface and
            self._current_primary_port == self._old_primary_port)

    def _is_unreached_primary(self):
        return not test_connection(
            self._current_primary_iface,
            self._current_primary_port,
            self._config.user,
            self._config.password,
            "postgres")

    def _is_not_init(self, status):
        return status == 404 or self._modified_index == 0

    @staticmethod
    def _is_error(status):
        return status >= 400

    def _attempt_to_set_primary(self):
        url = self._consul_connect + "?cas=%d" % self._modified_index
        value = "%s:%d" % (self._config.bound_iface, self._config.bound_port)
        requests.put(url, data=value)

    def create_node_type(self, node_type=None):
        available_status = self.get_current_primary()

        if NodeStatus._is_error(available_status):
            if self._is_not_init(available_status):
                self._attempt_to_set_primary()
            return node_type

        if self._is_primary():
            update_config = False
            if self._is_migrated_primary():
                self._old_primary_iface = self._config.bound_iface
                self._old_primary_port = self._config.bound_port
                update_config = True

            return PrimaryNode(self._config, update_config)

        if self._is_unreached_primary():
            self._attempt_to_set_primary()
            return TransitionState(self._config)

        if self._is_migrated_primary():
            self._old_primary_iface = self._current_primary_iface
            self._old_primary_port = self._current_primary_port
            return StandbyNode(
                self._config,
                update_config=True,
                primary_iface=self._current_primary_iface,
                primary_port=self._current_primary_port)

        return StandbyNode(
            self._config,
            update_config=False,
            primary_iface=self._current_primary_iface,
            primary_port=self._current_primary_port)
