#!/usr/bin/env python3
import argparse
import json
import os
import requests
import subprocess
import sys
import time

from datetime import datetime, timedelta

def hostname():
    p = subprocess.run('hostname', stdout=subprocess.PIPE, encoding='utf-8')
    return p.stdout.strip()

def oarsub_command(nb_host=1) -> str:
    frontend_to_cluster = {
        'fgrenoble': 'dahu',
        'fnancy': 'gros',
    }

    host = hostname()
    cluster_selection = ''
    if host not in frontend_to_cluster:
        raise RuntimeError(f'Please specify which cluster to use from host {host}')
    cluster = frontend_to_cluster[host]

    start_dt = datetime.now()
    end_dt = end_of_reservation(start_dt)
    walltime_str = oar_walltime(start_dt, end_dt)

    return f'''oarsub -I -l {{"cluster='{cluster}'}}"/host={nb_host},walltime={walltime_str}'''

def end_of_reservation(start_dt: datetime, delta_before_end: timedelta=timedelta(minutes=5)) -> datetime:
    overnight_delta = timedelta()
    if start_dt.hour >= 9 and start_dt.hour < 19:
        target_hour = 19
    elif start_dt.hour < 9:
        target_hour = 9
    else:
        target_hour = 9
        overnight_delta = timedelta(days=1)
    target = start_dt.replace(hour=target_hour, minute=0, second=0, microsecond=0) + overnight_delta

    if delta_before_end.total_seconds() < 0:
        raise ValueError(f"delta_before_end should be positive (got '{delta_before_end}')")

    target = target - delta_before_end
    if target < start_dt:
        raise RuntimeError(f"start_dt '{start_dt}' is too close to a day/night boundary")

    return target

def oar_walltime(start_dt: datetime, target_dt: datetime) -> str:
    if start_dt is None:
        start_dt = datetime.now()

    delta = target_dt - start_dt

    delta_seconds = int(delta.total_seconds())
    if delta_seconds <= 0:
        raise ValueError(f"target_dt ('{target_dt}') should be after start_dt ('{start_dt}')")

    hours = delta_seconds // 3600
    delta_seconds = delta_seconds % 3600

    minutes = delta_seconds // 60
    delta_seconds = delta_seconds % 60

    return f"{hours:02d}:{minutes:02d}:{delta_seconds:02d}"

def test_reservation_walltime():
    import pytest
    # valid diurnal activity
    dt = datetime(2023,3,9,9,0,0)
    edt = end_of_reservation(dt)
    assert edt == datetime(2023,3,9,18,55,0)
    wt = oar_walltime(dt, edt)
    assert wt == '09:55:00'

    dt = datetime(2023,3,9,9,0,0)
    edt = end_of_reservation(dt, delta_before_end=timedelta())
    assert edt == datetime(2023,3,9,19,0,0)
    wt = oar_walltime(dt, edt)
    assert wt == '10:00:00'

    dt = datetime(2023,3,9,11,30,0)
    edt = end_of_reservation(dt)
    assert edt == datetime(2023,3,9,18,55,0)
    wt = oar_walltime(dt, edt)
    assert wt == '07:25:00'

    # valid nocturnal activity
    dt = datetime(2023,3,9,19,0,0)
    edt = end_of_reservation(dt)
    assert edt == datetime(2023,3,10,8,55,0)
    wt = oar_walltime(dt, edt)
    assert wt == '13:55:00'

    dt = datetime(2023,3,9,23,55,0)
    edt = end_of_reservation(dt)
    assert edt == datetime(2023,3,10,8,55,0)
    wt = oar_walltime(dt, edt)
    assert wt == '09:00:00'

    dt = datetime(2023,3,10,4,0,0)
    edt = end_of_reservation(dt)
    assert edt == datetime(2023,3,10,8,55,0)
    wt = oar_walltime(dt, edt)
    assert wt == '04:55:00'

    # bad delta input
    with pytest.raises(ValueError):
        end_of_reservation(dt, delta_before_end=timedelta(seconds=-1))
    with pytest.raises(ValueError):
        end_of_reservation(dt, delta_before_end=timedelta(minutes=-5))

    # close to day/night boundary
    try:
        end_of_reservation(datetime(2023,3,9,18,55))
    except:
        assert False, "unexpected exception raised"
    with pytest.raises(RuntimeError):
        end_of_reservation(datetime(2023,3,9,18,55,1))

    # bad walltime inputs
    with pytest.raises(ValueError):
        oar_walltime(dt, dt-timedelta(seconds=1))

def do_g5k_cluster_status_request(site: str, cluster: str, url: str='https://api.grid5000.fr/stable', nodes=True, waiting=True, job_details=True, disks=False) -> dict:
    headers = {"accept": "application/vnd.grid5000.item+json"}
    bool_to_str = {
        True: 'yes',
        False: 'no',
    }

    fields = {
        'disks': disks,
        'nodes': nodes,
        'waiting': waiting,
        'job_details': job_details
    }
    fields_str = '&'.join(f"{x}:{bool_to_str[y]}" for x,y in fields.items())

    full_url = f"{url}/sites/{site}/clusters/{cluster}/status"

    response = requests.get(full_url, data=fields_str, headers=headers)
    if not response.ok:
        response.raise_for_status()
    return json.loads(response.text)

def natural_sort(l):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)

def get_usable_nodes(site: str, cluster: str, target_dt: datetime):
    response = do_g5k_cluster_status_request(site, cluster)
    nodes = response['nodes']

    def node_usable(node: dict) -> bool:
        if node['soft'] != 'free':
            return False
        if len(node['reservations']) > 0:
            first_reservation_start_timestamp = min([int(r['scheduled_at']) for r in node['reservations']])
            first_reservation_start_dt = datetime.fromtimestamp(first_reservation_start_timestamp)
            if first_reservation_start_dt < target_dt:
                return False
        return True

    return {name:node for name,node in nodes.items() if node_usable(node)}


def select_cluster_first_fit(preferred_clusters: list[tuple[str, str]], target_dt: datetime):
    for cluster, site in preferred_clusters:
        nodes = get_usable_nodes(site, cluster, target_dt)
        if len(nodes) > 0 :
            return (cluster, site)
    raise RuntimeError(f'No cluster with available nodes in the given cluster list {[x[0] for x in preferred_clusters]}')

def reserve_job(site: str, cluster: str, target_dt: datetime, cmd: str, log_prefix: str, url: str='https://api.grid5000.fr/stable'):
    wt = oar_walltime(datetime.now(), target_dt)

    headers = {"Content-Type": "application/json"}
    fields = {
        'command': cmd,
        'properties': f"(cluster='{cluster}')",
        'resources': f'nodes=1,walltime={wt}',
        'stdout': f'{log_prefix}.stdout',
        'stderr': f'{log_prefix}.stderr',
        'types': ['exotic'],
    }
    full_url = f"{url}/sites/{site}/jobs"

    response = requests.post(full_url, data=json.dumps(fields), headers=headers)
    if not response.ok:
        print(response.text)
        response.raise_for_status()
    return json.loads(response.text)

def get_job_info(job_id: str, site: str, url: str='https://api.grid5000.fr/stable'):
    full_url = f"{url}/sites/{site}/jobs/{job_id}"

    response = requests.get(full_url)
    if not response.ok:
        print(response.text)
        response.raise_for_status()
    return json.loads(response.text)

def wait_for_job_to_start(job_id: str, site: str, sleep_duration: int=5, url: str='https://api.grid5000.fr/stable'):
    print(f'Waiting for job {job_id} to start on site {site}')
    while True:
        info = get_job_info(job_id, site, url)
        job_state = info['state'].lower()

        if job_state not in ['waiting', 'launching']:
            print(f"  state is '{job_state}' -> it has started")
            return info
        print(f"  state is '{job_state}' -> sleeping for {sleep_duration} seconds")
        time.sleep(sleep_duration)

def wait_for_setup_to_finish(job_id: str, site: str, log_prefix: str, sleep_duration: int=10, url: str='https://api.grid5000.fr/stable'):
    print(f'Waiting for interactive setup of job {job_id} on site {site} to finish')
    while True:
        job_current_stdout = open(f'{log_prefix}.stdout', 'r', encoding='utf-8').read()

        if 'Setup has run successfully' in job_current_stdout:
            print(f'  done')
            return True
        if 'Setup has run UNsuccessfully' in job_current_stdout:
            print(f'  finished but failed :(')
            return False

        # checking whether the job is still running
        info = get_job_info(job_id, site, url)
        job_state = info['state'].lower()
        if job_state != 'running':
            print(f'  job state is not running anymore (state={job_state}) -> aborting')
            raise RuntimeError('aborting wait for interactive setup to finish beacause the job is not running anymore')

        print(f'  setup is ongoing -> sleeping for {sleep_duration} seconds')
        time.sleep(sleep_duration)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--clear-log-files', action='store_true')
    parser.add_argument('--clear-subscript', action='store_true')
    parser.add_argument('--wait-job-running', action='store_true')
    parser.add_argument('--wait-job-setup-finished', action='store_true')
    args = parser.parse_args()

    if args.wait_job_setup_finished:
        assert(args.clear_log_files)

    user = os.environ['USER']
    cmd = f'/home/{user}/interactive/job-script.sh'
    log_prefix = f'/home/{user}/OAR.interactive'

    if args.clear_log_files:
        log_files = [f'{log_prefix}.stdout', f'{log_prefix}.stderr']
        print(f'Removing log files {log_files}')
        for filename in log_files:
            try:
                os.remove(filename)
            except OSError:
                pass

    target_dt = end_of_reservation(datetime.now())

    my_cluster_preference = [
        ('dahu', 'grenoble'),
        #('troll', 'grenoble'),
        #('yeti', 'grenoble'),
        #('gros', 'nancy'),
    ]

    (cluster, site) = select_cluster_first_fit(my_cluster_preference, target_dt)
    response_dict = reserve_job(site, cluster, target_dt, cmd, log_prefix, url='https://api.grid5000.fr/stable')

    job_id = response_dict["uid"]
    subscript_path = response_dict['directory'] + '/' + response_dict['command']
    print(f'job {job_id} has been reserved on {site}/{cluster} ; will call {subscript_path}')

    if args.wait_job_running or args.clear_subscript or args.wait_job_setup_finished:
        wait_for_job_to_start(job_id, site)

    if args.clear_subscript:
        print(f'Removing subscript file {subscript_path}')
        try:
            os.remove(subscript_path)
        except OSError:
            print('  could not remove file (this is normal if job is not run on this site)')
            sys.exit(1)

    if args.wait_job_setup_finished:
        wait_for_setup_to_finish(job_id, site, log_prefix)

if __name__ == '__main__':
    main()
