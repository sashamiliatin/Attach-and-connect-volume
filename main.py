import shutil
import uuid
import requests
import math
from zadarapy import session
import zadarapy.vpsa.volumes as volumes
import zadarapy.vpsa.servers as servers
import config
import connector
from oslo_concurrency import lockutils
import os
import logging
from cleanup import CleanUp
import hashlib
from pathlib import Path


connector_properties = {
    'target_portals': ['{0}:3260'.format(config.ip)],
    'target_iqns': ['iqn.2011-04.com.zadara:vsa-00000006:CD2295A547E046A98FAFA18AF1BBCB8D:1'],
    'target_luns': [0],
}
CHUNKSIZE_BYTES = 4000000


def get_size_of_file(url):
    res = requests.head(url=url, verify=False)
    res.raise_for_status()
    size = res.headers.get('Content-Length')
    return int(math.ceil(float(size) / (2 ** 30)))

def handle_data(url, local_filename):
    data_length = 0
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    return data_length


def create_volume(name, size):
    vpsa_session = session.Session(host=config.host, key=config.token)
    volume = volumes.create_volume(session=vpsa_session, pool_id='pool-00010001', block='YES', capacity=size, display_name=name)
    return volume['response']['vol_name']


def attach_volume_to_host(volume_id):
    server = None
    vpsa_session = session.Session(host=config.host, key=config.token)
    server_list = servers.get_all_servers(session=vpsa_session)
    if len(server_list['response']['servers']) > 0:
        server = server_list['response']['servers'][0]['name']
        servers.attach_servers_to_volume(session=vpsa_session, volume_id=volume_id, servers=server)
    return server


def mount(device):
    vpsa_session = session.Session(host=config.host, key=config.token)
    return ''


def upload_file(path, url):
    try:
        data = handle_data(url, path)
    except:
        logging.getLogger(__name__).exception('Failed to write to device: %s', path)
        raise
    return data

def umaunt(path):
    pass


def detach_from_host(volume_id, host):
    vpsa_session = session.Session(host=config.host, key=config.token)
    volumes.detach_servers_from_volume(session=vpsa_session, volume_id=volume_id, servers=host)
    pass


def transform(volume_id):
    snapshot_id = uuid.uuid4()
    return snapshot_id


def connect_volume(device, volume_id):
    lun = None
    vpsa_session = session.Session(host=config.host, key=config.token)
    attached_volumes = servers.get_volumes_attached_to_server(session=vpsa_session, server_id=device)
    for volume in attached_volumes['response']['volumes']:
        if volume['name'] == volume_id:
            lun = volume['lun']
    connector_properties['target_luns'] = [int(lun)]
    connection_prop  = connector_properties
    connection = connector.connect_volume(connection_prop)
    return connection


def create_from_url(url=None):
    size = get_size_of_file(url=url)
    volume_id = create_volume(name='temp_volume_5', size=size)
    # volume_id = 'volume-00007113'
    device = attach_volume_to_host(volume_id=volume_id)
    connection_info = connect_volume(device, volume_id)
    upload_file(path=connection_info['path'], url=url)
    umaunt(path=connection_info['path'])
    # detach_from_host(volume_id=volume_id, host=device)
    # return transform(volume_id=volume_id)
    return connection_info['path']


if __name__ == '__main__':
    lockutils.set_defaults(lock_path='/home/fedora/tmp/')
    url = 'http://logstack.dc1.strato:8001/bitnami-tomcatstack-8.5.28-0-linux-debian-9-x86_64-disk1.qcow2'
    path = create_from_url(url)
