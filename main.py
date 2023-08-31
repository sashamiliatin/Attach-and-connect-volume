import uuid
import requests
import math
from zadarapy import session
import zadarapy.vpsa.volumes as volumes
import zadarapy.vpsa.servers as servers
import config
import connector
from oslo_concurrency import lockutils


def get_size_of_file(url):
    res = requests.head(url=url, verify=False)
    res.raise_for_status()
    size = res.headers.get('Content-Length')
    return int(math.ceil(float(size) / (2 ** 30)))


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
        res = servers.attach_servers_to_volume(session=vpsa_session, volume_id=volume_id, servers=server)
        print(res)
    connector_properties = {
        'target_portals': ['{0}:3260'.format(config.ip)],
        'target_iqns': ['iqn.2011-04.com.zadara:vsa-00000006:CD2295A547E046A98FAFA18AF1BBCB8D:1'],
        'target_luns': [2],
    }
    connection = connector.connect_volume(connector_properties)
    print(connection)
    return server


def mount(device):
    vpsa_session = session.Session(host=config.host, key=config.token)
    return ''


def upload_file(path, url):
    pass


def umaunt(path):
    pass


def detach_from_host(volume_id, host):
    vpsa_session = session.Session(host=config.host, key=config.token)
    volumes.detach_servers_from_volume(session=vpsa_session, volume_id=volume_id, servers=host)
    pass


def transform(volume_id):
    snapshot_id = uuid.uuid4()
    return snapshot_id


def create_from_url(url=None):
    size = get_size_of_file(url=url)
    # volume_id = create_volume(name='temp_volume', size=size)
    volume_id = 'volume-00007110'
    device = attach_volume_to_host(volume_id=volume_id)
    path = mount(device=device)
    upload_file(path=path, url=url)
    umaunt(path=path)
    detach_from_host(volume_id=volume_id, host=device)
    return transform(volume_id=volume_id)


if __name__ == '__main__':
    lockutils.set_defaults(lock_path='/home/fedora/tmp/')
    url = 'http://logstack.dc1.strato:8001/bitnami-tomcatstack-8.5.28-0-linux-debian-9-x86_64-disk1.qcow2'
    create_from_url(url)
