import os
import pytest
import utils
from utils import kill_all_processes, logging_setup, Size, Unit
from rafs import Backend, RafsConf, RafsImage, NydusDaemon, Compressor
import time
import tempfile
from supervisor import RafsSupervisor
from workload_gen import WorkloadGen
from nydus_anchor import NydusAnchor, inspect_sys_fuse
from nydusd_client import NydusAPIClient
from distributor import Distributor


logging_setup()


def test_upgrade(nydus_anchor: NydusAnchor, rafs_conf: RafsConf, nydus_scratch_image):
    """
    title: Hot upgrade test
        description: Use a newly started nydusd process to replace old one.
                     - Start a nydus process
                     - Start a nydus process with -o upgrade
                     - Open a file and read it before upgrading and read it again after upgrade
                     - Hold fuse fd into this test program
                     - Send message to old process by nydusd_ctl making it quit without umounting
                     - Send fuse fd to new nydusd process
                     - Release torture slaves to read from mount point in parallel
                     - Umount rafs

        pass_criteria:
          - Read io can be carried on after hot upgrade.
          - Only one fuse connection for all the time.

    """

    # Socket file may be residual
    watch_sock_path = os.path.join(os.getcwd(), "supervisor")
    try:
        os.unlink(watch_sock_path)
    except FileNotFoundError:
        pass
    rafs_supervisor = RafsSupervisor(watch_sock_path, 0)

    dist = Distributor(nydus_scratch_image.rootfs(), 5, 5)
    dist.generate_tree()
    dist.put_single_file(Size(8, Unit.MB))
    victim = dist.files[0]
    victim_path = os.path.join(nydus_anchor.mountpoint, victim)

    nydus_scratch_image.set_backend(Backend.BACKEND_PROXY).create_image()

    rafs_conf.set_rafs_backend(Backend.BACKEND_PROXY)
    # rafs_conf.enable_validation()
    rafs_conf.enable_rafs_blobcache()
    rafs_conf.enable_fs_prefetch()

    rafs1 = NydusDaemon(nydus_anchor, nydus_scratch_image, rafs_conf)
    nc = NydusAPIClient(rafs1.get_apisock())

    rafs1.supervisor(watch_sock_path).id("old-daemon").thread_num(8).mount()
    time.sleep(1)
    nc.get_wait_daemon(wait_state="RUNNING")

    victim_fd = os.open(victim_path, os.O_RDONLY)
    os.read(victim_fd, 100000)

    workload_gen = WorkloadGen(nydus_anchor.mountpoint, nydus_scratch_image.rootfs())
    workload_gen.setup_workload_generator()
    workload_gen.torture_read(8, 8)

    # Wait until this program already binds UDS socket.
    event = rafs_supervisor.recv_fd()
    event.wait()

    # TODO: Workaround, sleep for a little time so it can begin to listen.
    time.sleep(1)
    nc.send_fuse_fd(watch_sock_path)
    nc.get_wait_daemon()

    utils.clean_pagecache()
    time.sleep(5)

    inspect_sys_fuse()
    rafs2 = (
        NydusDaemon(nydus_anchor, nydus_scratch_image, rafs_conf)
        .prefetch_files("/")
        .thread_num(6)
    )
    rafs2.apisock("new-apisock").supervisor(watch_sock_path).id(
        "new-daemon"
    ).upgrade().mount(wait_mount=False)

    rafs_supervisor.send_fd()
    nc2 = NydusAPIClient(rafs2.get_apisock())
    nc2.get_wait_daemon(wait_state="INIT")
    nc.do_exit()
    nc2.takeover()
    nc2.start_service()
    nc2.get_wait_daemon()

    os.read(victim_fd, 100000)

    os.close(victim_fd)
    assert rafs1.mountpoint == rafs2.mountpoint

    inspect_sys_fuse()

    event = rafs_supervisor.recv_fd()

    # Wait until this program already binds UDS socket.
    event.wait()

    # TODO: Workaround, sleep for a little time so it can begin to listen.
    time.sleep(1)
    nc2.send_fuse_fd(watch_sock_path)
    nc2.get_wait_daemon()

    nc2.do_exit()
    rafs_supervisor.send_fd()
    rafs3 = (
        NydusDaemon(nydus_anchor, nydus_scratch_image, rafs_conf)
        .prefetch_files("/")
        .thread_num(12)
    )
    rafs3.apisock("new-apisock3").supervisor(watch_sock_path).id(
        "new-daemon3"
    ).upgrade().mount(wait_mount=False)

    nc3 = NydusAPIClient(rafs3.get_apisock())
    nc3.get_wait_daemon(wait_state="INIT")
    nc3.takeover()
    nc3.get_wait_daemon()

    workload_gen.finish_torture_read()

    inspect_sys_fuse()
    rafs1.p.wait()
    rafs2.p.wait()

    assert NydusAnchor.check_nydusd_health()
    rafs3.umount()


@pytest.mark.parametrize("failover_policy", ["resend"])
def test_failover(
    nydus_anchor: NydusAnchor, rafs_conf: RafsConf, nydus_scratch_image, failover_policy
):

    # Socket file may be residual
    watch_sock_path = os.path.join(os.getcwd(), "supervisor")
    try:
        os.unlink(watch_sock_path)
    except FileNotFoundError:
        pass
    rafs_supervisor = RafsSupervisor(watch_sock_path, 0)

    rafs_conf.enable_rafs_blobcache().set_rafs_backend(
        Backend.BACKEND_PROXY
    ).enable_fs_prefetch()

    dist = Distributor(nydus_scratch_image.rootfs(), 5, 5)
    dist.generate_tree()
    dist.put_single_file(Size(8, Unit.MB))
    victim = dist.files[0]
    victim_path = os.path.join(nydus_anchor.mountpoint, victim)

    nydus_scratch_image.set_backend(Backend.BACKEND_PROXY).create_image()

    rafs1 = NydusDaemon(nydus_anchor, nydus_scratch_image, rafs_conf)
    rafs1.supervisor(watch_sock_path).id("old-daemon").thread_num(8).mount()
    assert rafs1.is_mounted()
    time.sleep(1)
    nc = NydusAPIClient(rafs1.get_apisock())
    nc.get_wait_daemon(wait_state="RUNNING")

    victim_fd = os.open(victim_path, os.O_RDONLY)
    os.read(victim_fd, 100000)

    event = rafs_supervisor.recv_fd()
    # Wait until this program already binds UDS socket.
    event.wait()

    # TODO: Workaround, sleep for a little time so it can begin to listen.
    time.sleep(1)
    nc.takeover_nocheck()
    nc.send_fuse_fd(watch_sock_path)
    nc.get_wait_daemon()

    inspect_sys_fuse()

    workload_gen = WorkloadGen(nydus_anchor.mountpoint, nydus_scratch_image.rootfs())
    workload_gen.setup_workload_generator()
    workload_gen.torture_read(10, 16)

    utils.kill_all_processes("nydusd")
    utils.clean_pagecache()
    inspect_sys_fuse()

    rafs_supervisor.send_fd()

    rafs2 = NydusDaemon(nydus_anchor, nydus_scratch_image, rafs_conf)
    rafs2.supervisor(watch_sock_path).id("new-daemon").thread_num(8).failover_policy(
        failover_policy
    ).mount()
    nc2 = NydusAPIClient(rafs2.get_apisock())
    nc2.get_wait_daemon(wait_state="INIT")
    nc2.takeover()
    nc2.start_service()
    nc2.get_wait_daemon()
    nc2.takeover_nocheck()

    os.read(victim_fd, 100000)
    os.close(victim_fd)

    inspect_sys_fuse()
    workload_gen.finish_torture_read()

    assert workload_gen.io_error == False

    rafs1.p.wait()
    rafs2.umount()


def test_api_mounted_rafs(nydus_anchor: NydusAnchor, rafs_conf: RafsConf):
    # Socket file may be residual
    watch_sock_path = os.path.join(os.getcwd(), "supervisor")
    try:
        os.unlink(watch_sock_path)
    except FileNotFoundError:
        pass
    rafs_supervisor = RafsSupervisor(watch_sock_path, 0)

    rafs_conf.enable_rafs_blobcache().set_rafs_backend(
        Backend.BACKEND_PROXY
    ).enable_fs_prefetch()

    rootfs_1 = tempfile.TemporaryDirectory(dir=nydus_anchor.workspace)

    dist1 = Distributor(rootfs_1.name, 2, 5)
    dist1.generate_tree()
    dist1.put_multiple_files(100, Size(1, Unit.MB))
    dist1.put_single_file(Size(4, Unit.MB))
    victim1 = dist1.files[0]
    victim1_path = os.path.join(nydus_anchor.mountpoint, "rafs1", victim1)
    rafs_image1 = RafsImage(
        nydus_anchor, rootfs_1.name, compressor=Compressor.LZ4_BLOCK
    )
    rafs_image1.set_backend(Backend.BACKEND_PROXY).create_image()

    rootfs_2 = tempfile.TemporaryDirectory(dir=nydus_anchor.workspace)

    dist2 = Distributor(rootfs_2.name, 4, 3)
    dist2.generate_tree()
    dist2.put_multiple_files(80, Size(1, Unit.MB))
    dist2.put_single_file(Size(4, Unit.MB))
    victim2 = dist2.files[0]
    victim2_path = os.path.join(nydus_anchor.mountpoint, "rafs2", victim2)
    rafs_image2 = RafsImage(
        nydus_anchor, rootfs_2.name, compressor=Compressor.LZ4_BLOCK
    )
    rafs_image2.set_backend(Backend.BACKEND_PROXY).create_image()

    rootfs_3 = tempfile.TemporaryDirectory(dir=nydus_anchor.workspace)

    dist3 = Distributor(rootfs_3.name, 10, 2)
    dist3.generate_tree()
    dist3.put_multiple_files(90, Size(1, Unit.MB))
    dist3.put_single_file(Size(4, Unit.MB))
    victim3 = dist3.files[0]
    victim3_path = os.path.join(nydus_anchor.mountpoint, "rafs3", victim3)
    rafs_image3 = RafsImage(nydus_anchor, rootfs_3.name, compressor=Compressor.NONE)
    rafs_image3.set_backend(Backend.BACKEND_PROXY).create_image()

    daemon = (
        NydusDaemon(nydus_anchor, None, rafs_conf)
        .supervisor(watch_sock_path)
        .id("old-daemon")
        .thread_num(8)
        .mount()
    )
    time.sleep(1)

    nc = NydusAPIClient(daemon.get_apisock())
    nc.get_wait_daemon(wait_state="RUNNING")
    nc.mount_rafs(rafs_image1.bootstrap_path, "/rafs1", rafs_conf.path(), None)
    nc.mount_rafs(rafs_image2.bootstrap_path, "/rafs2", rafs_conf.path(), None)
    nc.mount_rafs(rafs_image3.bootstrap_path, "/rafs3", rafs_conf.path(), None)

    victim1_fd = os.open(victim1_path, os.O_RDONLY)
    os.read(victim1_fd, 10000)

    victim2_fd = os.open(victim2_path, os.O_RDONLY)
    os.read(victim2_fd, 10000)

    victim3_fd = os.open(victim3_path, os.O_RDONLY)
    os.read(victim2_fd, 10000)

    workload_gen1 = WorkloadGen(
        os.path.join(nydus_anchor.mountpoint, "rafs1"), rafs_image1.rootfs()
    )
    workload_gen1.setup_workload_generator()
    workload_gen1.torture_read(8, 8)

    workload_gen2 = WorkloadGen(
        os.path.join(nydus_anchor.mountpoint, "rafs2"), rafs_image2.rootfs()
    )
    workload_gen2.setup_workload_generator()
    workload_gen2.torture_read(8, 8)

    workload_gen3 = WorkloadGen(
        os.path.join(nydus_anchor.mountpoint, "rafs3"), rafs_image3.rootfs()
    )
    workload_gen3.setup_workload_generator()
    workload_gen3.torture_read(8, 8)

    nc.get_wait_daemon()

    event = rafs_supervisor.recv_fd()

    # Wait until this program already binds UDS socket.
    event.wait()

    # TODO: Workaround, sleep for a little time so it can begin to listen.
    time.sleep(1)
    nc.send_fuse_fd(watch_sock_path)
    nc.get_wait_daemon()

    utils.clean_pagecache()
    time.sleep(5)

    inspect_sys_fuse()
    daemon2 = (
        NydusDaemon(nydus_anchor, None, rafs_conf)
        .prefetch_files("/")
        .thread_num(6)
        .apisock("new-apisock")
        .supervisor(watch_sock_path)
        .id("new-daemon")
        .upgrade()
        .mount(wait_mount=False)
    )

    rafs_supervisor.send_fd()
    nc2 = NydusAPIClient(daemon2.get_apisock())
    nc2.get_wait_daemon(wait_state="INIT")
    nc.do_exit()
    nc2.takeover()
    nc2.start_service()
    nc2.get_wait_daemon()

    os.read(victim1_fd, 10000)
    os.read(victim2_fd, 10000)
    os.read(victim3_fd, 10000)

    inspect_sys_fuse()

    event = rafs_supervisor.recv_fd()

    # Wait until this program already binds UDS socket.
    event.wait()

    # TODO: Workaround, sleep for a little time so it can begin to listen.
    time.sleep(1)
    nc2.send_fuse_fd(watch_sock_path)
    nc2.get_wait_daemon()

    rafs_supervisor.send_fd()
    daemon3 = (
        NydusDaemon(nydus_anchor, None, rafs_conf)
        .prefetch_files("/")
        .thread_num(6)
        .apisock("new-apisock3")
        .supervisor(watch_sock_path)
        .id("new-daemon3")
        .upgrade()
        .mount(wait_mount=False)
    )

    nc3 = NydusAPIClient(daemon3.get_apisock())
    nc3.get_wait_daemon(wait_state="INIT")
    nc2.do_exit()
    nc3.takeover()
    nc3.start_service()
    nc3.get_wait_daemon()

    os.read(victim1_fd, 10000)
    os.read(victim2_fd, 10000)
    os.read(victim3_fd, 10000)

    workload_gen1.finish_torture_read()
    workload_gen2.finish_torture_read()
    workload_gen3.finish_torture_read()

    os.close(victim1_fd)
    os.close(victim2_fd)
    os.close(victim3_fd)
    daemon.p.wait()
    daemon2.p.wait()

    inspect_sys_fuse()

    assert NydusAnchor.check_nydusd_health()
    daemon3.umount()


def test_api_mounted_rafs_failover(nydus_anchor: NydusAnchor, rafs_conf: RafsConf):
    # Socket file may be residual
    watch_sock_path = os.path.join(os.getcwd(), "supervisor")
    try:
        os.unlink(watch_sock_path)
    except FileNotFoundError:
        pass
    rafs_supervisor = RafsSupervisor(watch_sock_path, 0)

    rafs_conf.enable_rafs_blobcache().set_rafs_backend(
        Backend.BACKEND_PROXY
    ).enable_fs_prefetch()

    rootfs_1 = tempfile.TemporaryDirectory(dir=nydus_anchor.workspace)

    dist1 = Distributor(rootfs_1.name, 2, 5)
    dist1.generate_tree()
    dist1.put_multiple_files(100, Size(1, Unit.MB))
    dist1.put_single_file(Size(4, Unit.MB))
    victim1 = dist1.files[0]
    victim1_path = os.path.join(nydus_anchor.mountpoint, "rafs1", victim1)
    rafs_image1 = RafsImage(
        nydus_anchor, rootfs_1.name, compressor=Compressor.LZ4_BLOCK
    )
    rafs_image1.set_backend(Backend.BACKEND_PROXY).create_image()

    rootfs_2 = tempfile.TemporaryDirectory(dir=nydus_anchor.workspace)

    dist2 = Distributor(rootfs_2.name, 4, 3)
    dist2.generate_tree()
    dist2.put_multiple_files(80, Size(1, Unit.MB))
    dist2.put_single_file(Size(4, Unit.MB))
    victim2 = dist2.files[0]
    victim2_path = os.path.join(nydus_anchor.mountpoint, "rafs2", victim2)
    rafs_image2 = RafsImage(
        nydus_anchor, rootfs_2.name, compressor=Compressor.LZ4_BLOCK
    )
    rafs_image2.set_backend(Backend.BACKEND_PROXY).create_image()

    rootfs_3 = tempfile.TemporaryDirectory(dir=nydus_anchor.workspace)

    dist3 = Distributor(rootfs_3.name, 10, 2)
    dist3.generate_tree()
    dist3.put_multiple_files(90, Size(1, Unit.MB))
    dist3.put_single_file(Size(4, Unit.MB))
    victim3 = dist3.files[0]
    victim3_path = os.path.join(nydus_anchor.mountpoint, "rafs3", victim3)
    rafs_image3 = RafsImage(nydus_anchor, rootfs_3.name, compressor=Compressor.NONE)
    rafs_image3.set_backend(Backend.BACKEND_PROXY).create_image()

    daemon = (
        NydusDaemon(nydus_anchor, None, rafs_conf)
        .supervisor(watch_sock_path)
        .id("old-daemon")
        .thread_num(8)
        .mount()
    )
    time.sleep(1)

    nc = NydusAPIClient(daemon.get_apisock())
    nc.get_wait_daemon(wait_state="RUNNING")
    time.sleep(1)
    nc.mount_rafs(rafs_image1.bootstrap_path, "/rafs1", rafs_conf.path(), None)
    time.sleep(1)
    nc.mount_rafs(rafs_image2.bootstrap_path, "/rafs2", rafs_conf.path(), None)
    time.sleep(1)
    nc.mount_rafs(rafs_image3.bootstrap_path, "/rafs3", rafs_conf.path(), None)

    victim1_fd = os.open(victim1_path, os.O_RDONLY)
    os.read(victim1_fd, 10000)

    victim2_fd = os.open(victim2_path, os.O_RDONLY)
    os.read(victim2_fd, 10000)

    victim3_fd = os.open(victim3_path, os.O_RDONLY)
    os.read(victim2_fd, 10000)

    workload_gen1 = WorkloadGen(
        os.path.join(nydus_anchor.mountpoint, "rafs1"), rafs_image1.rootfs()
    )
    workload_gen1.setup_workload_generator()
    workload_gen1.torture_read(8, 8)

    workload_gen2 = WorkloadGen(
        os.path.join(nydus_anchor.mountpoint, "rafs2"), rafs_image2.rootfs()
    )
    workload_gen2.setup_workload_generator()
    workload_gen2.torture_read(8, 8)

    workload_gen3 = WorkloadGen(
        os.path.join(nydus_anchor.mountpoint, "rafs3"), rafs_image3.rootfs()
    )
    workload_gen3.setup_workload_generator()
    workload_gen3.torture_read(8, 8)

    nc.get_wait_daemon()

    event = rafs_supervisor.recv_fd()

    # Wait until this program already binds UDS socket.
    event.wait()

    # TODO: Workaround, sleep for a little time so it can begin to listen.
    time.sleep(1)
    nc.send_fuse_fd(watch_sock_path)
    nc.get_wait_daemon()

    utils.kill_all_processes("nydusd")

    utils.clean_pagecache()
    time.sleep(5)

    inspect_sys_fuse()
    daemon2 = (
        NydusDaemon(nydus_anchor, None, None)
        .thread_num(6)
        .apisock(daemon.get_apisock())
        .supervisor(watch_sock_path)
        .failover_policy("resend")
        .id("new-daemon")
        .mount(wait_mount=False)
    )

    rafs_supervisor.send_fd()
    nc2 = NydusAPIClient(daemon2.get_apisock())
    nc2.get_wait_daemon(wait_state="INIT")
    nc2.takeover()
    nc2.start_service()
    nc2.get_wait_daemon()

    os.read(victim1_fd, 10000)
    os.read(victim2_fd, 10000)
    os.read(victim3_fd, 10000)

    inspect_sys_fuse()

    event = rafs_supervisor.recv_fd()

    # Wait until this program already binds UDS socket.
    event.wait()

    # TODO: Workaround, sleep for a little time so it can begin to listen.
    time.sleep(1)
    nc2.send_fuse_fd(watch_sock_path)
    nc2.get_wait_daemon()

    workload_gen4 = WorkloadGen(
        os.path.join(nydus_anchor.mountpoint, "rafs2"), rafs_image2.rootfs()
    )
    workload_gen4.setup_workload_generator()
    workload_gen4.torture_read(8, 8)

    rafs_supervisor.send_fd()

    utils.kill_all_processes("nydusd")

    daemon3 = (
        NydusDaemon(nydus_anchor, None, None)
        .thread_num(6)
        .apisock(daemon.get_apisock())
        .supervisor(watch_sock_path)
        .failover_policy("resend")
        .id("new-daemon3")
        .mount(wait_mount=False)
    )

    nc3 = NydusAPIClient(daemon3.get_apisock())
    nc3.get_wait_daemon(wait_state="INIT")
    nc3.takeover()
    nc3.start_service()
    nc3.get_wait_daemon()

    os.read(victim1_fd, 10000)
    os.read(victim2_fd, 10000)
    os.read(victim3_fd, 10000)

    assert workload_gen4.verify_entire_fs()

    workload_gen1.finish_torture_read()
    workload_gen2.finish_torture_read()
    workload_gen3.finish_torture_read()

    os.close(victim1_fd)
    os.close(victim2_fd)
    os.close(victim3_fd)
    daemon.p.wait()
    daemon2.p.wait()

    workload_gen4.finish_torture_read()

    assert not workload_gen1.io_error
    assert not workload_gen2.io_error
    assert not workload_gen3.io_error
    assert not workload_gen4.io_error

    nc.umount_rafs("/rafs1")
    nc.umount_rafs("/rafs2")
    nc.umount_rafs("/rafs3")

    inspect_sys_fuse()

    assert NydusAnchor.check_nydusd_health()
    daemon3.umount()
