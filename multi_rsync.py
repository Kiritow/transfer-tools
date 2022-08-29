import os
import sys
import subprocess
import traceback
import logging
import time
import random
import getopt
import hashlib
import concurrent.futures
from concurrent.futures.thread import ThreadPoolExecutor


def get_logger():
    # formatter = logging.Formatter("%(asctime)s @%(module)s [%(levelname)s] %(funcName)s: %(message)s")
    formatter = logging.Formatter("%(asctime)s: %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    return logger


def get_sha256_str(content):
    return hashlib.sha256(content.encode()).hexdigest()


def single_rsync(local_path, remote_dir_path, remote_addr, remote_ssh_opt, rsync_opts):
    call_args = ["rsync", "-e", remote_ssh_opt] + rsync_opts + [local_path, "{}:{}/".format(remote_addr, remote_dir_path)]
    print(call_args)

    while True:
        try:
            time.sleep(3 + random.randint(0, 40) / 10)  # 3 to 7 sec
            subprocess.check_call(call_args)
            break
        except subprocess.CalledProcessError:
            print(traceback.format_exc())
            sleep_time = 15 + random.randint(0, 150) / 10  # 15 to 30 sec
            print('rsync exit unexpectedly, restart in {} seconds...'.format(sleep_time))
            time.sleep(sleep_time)


def multi_rsync(ssh_username, ssh_host, local_dir, remote_dir, ssh_port=22, rsync_timeout=90, max_workers=25, check_interval=5):
    logger = get_logger()
    remote_addr = "{}@{}".format(ssh_username, ssh_host)
    remote_ssh_opt = "ssh -p {}".format(ssh_port)
    rsync_transfer_opts = ('-a', '-v', '--protect-args', '--partial', '--progress', '--stats', '--timeout={}'.format(rsync_timeout))

    logger.info("syncing directories...")
    subprocess.check_call(["rsync", "-a", "-e", remote_ssh_opt, "-f", "+ */", "-f", "- *", local_dir, "{}:{}/".format(remote_addr, remote_dir)])

    pool = ThreadPoolExecutor(max_workers=max_workers)
    tasks = []

    for root, _, files in os.walk(local_dir):
        rel_path = os.path.relpath(root, start=local_dir)
        remote_root = os.path.join(remote_dir, os.path.basename(local_dir), rel_path) if rel_path != '.' else os.path.join(remote_dir, os.path.basename(local_dir))

        for filename in files:
            full_path = os.path.join(root, filename)
            filename_hash = get_sha256_str(filename)
            current_rsync_transfer_opts = list(rsync_transfer_opts) + ['--partial-dir=.rsync-part-{}'.format(filename_hash)]

            logger.info("Submitting: {}".format(full_path))
            t = pool.submit(single_rsync, full_path, remote_root, remote_addr, remote_ssh_opt, current_rsync_transfer_opts)
            tasks.append(t)
            logger.info("{} tasks submitted.".format(len(tasks)))

    while tasks:
        logger.info("{} tasks left...".format(len(tasks)))
        result = concurrent.futures.wait(tasks, timeout=check_interval)
        if result.done:
            logger.info("removed {} finished tasks".format(len(result.done)))
        tasks = result.not_done
        time.sleep(1)

    logger.info("finished.")


if __name__ == "__main__":
    username = None
    host = None
    kws = {}

    opts, args = getopt.getopt(sys.argv[1:], "u:h:p:t:w:T:", [])
    for tup in opts:
        op, val = tup
        if op == '-u':
            username = val
        elif op == '-h':
            host = val
        elif op == '-p':
            kws["ssh_port"] = int(val)
        elif op == '-t':
            kws["rsync_timeout"] = int(val)
        elif op == '-w':
            kws["max_workers"] = int(val)
        elif op == '-T':
            kws["check_interval"] = int(val)

    local_dir = args[0]
    remote_dir = args[1]

    if not username or not host:
        sys.stderr.write("Need SSH username and host\n")
        exit(1)

    # python3 multi_rsync.py -u user -h 127.0.0.1 -p 22 /home/user/todo /mnt/storage
    multi_rsync(username, host, local_dir, remote_dir, **kws)
