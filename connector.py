import os

import os_brick.initiator.connectors.iscsi
from os_brick.initiator import connector
from os_brick.privileged import rootwrap

NSENTER_NAMESPACE = os.getenv('NSENTER_NAMESPACE', '/proc/1/ns')


def connect_volume(connector_properties):
    cls = get_connector_cls()
    return cls.connect_volume(connector_properties)


def disconnect_volume(connector_properties, device_info=None):
    cls = get_connector_cls()
    cls.disconnect_volume(connector_properties, device_info)


def get_connector_cls() -> os_brick.initiator.connectors.iscsi.ISCSIConnector:
    return connector.InitiatorConnector.factory('ISCSI', 'sudo',
                                                use_multipath=True,
                                                execute=nsenter_execute,
                                                device_scan_attempts=5)


def nsenter_execute(*cmd, **kwargs):
    kwargs.pop('run_as_root', False)
    kwargs.pop('root_helper', None)
    cmd = ['nsenter', f'--mount={NSENTER_NAMESPACE}/mnt', f'--ipc={NSENTER_NAMESPACE}/ipc', '--'] + list(cmd)
    return rootwrap.custom_execute(*cmd, **kwargs)
