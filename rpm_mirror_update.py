#!/usr/bin/python
import sys, requests, sqlite3, bz2, pprint, os
import errno, urllib2
from bs4 import BeautifulSoup
from tqdm import tqdm
import xml.etree.ElementTree as ET
import gzip

#proxy = 'http://10.120.x.x:3128'

#os.environ['http_proxy'] = proxy
#os.environ['https_proxy'] = proxy


mirrors = [{"url": "http://centos-mirror.cyberhost.ro/6/os/x86_64/", "filter": {"rpm_group": ["System Environment/Base", "System Environment/Libraries","System Environment/Daemons","Development/Libraries"]}, "download_path": "/var/www/centos/base/6/os/x86_64/"}, {"url": "http://centos-mirror.cyberhost.ro/6/updates/x86_64/", "filter": {"rpm_group": ["System Environment/Base", "System Environment/Libraries","System Environment/Daemons","Development/Libraries"]}, "download_path": "/var/www/centos/base/6/updates/x86_64/"}, {"url": "http://centos-mirror.cyberhost.ro/6/extras/x86_64/", "filter": {"rpm_group": ["System Environment/Base", "System Environment/Libraries","System Environment/Daemons","Development/Libraries"]}, "download_path": "/var/www/centos/base/6/extras/x86_64/"}, {"url": "http://centos-mirror.cyberhost.ro/7/os/x86_64/", "filter": {"rpm_group": ["System Environment/Base", "System Environment/Libraries","System Environment/Daemons","Development/Libraries"]}, "download_path": "/var/www/centos/base/7/os/x86_64/"}, {"url": "http://centos-mirror.cyberhost.ro/7/updates/x86_64/", "filter": {"rpm_group": ["System Environment/Base", "System Environment/Libraries","System Environment/Daemons","Development/Libraries"]}, "download_path": "/var/www/centos/base/7/updates/x86_64/"}, {"url": "http://centos-mirror.cyberhost.ro/7/extras/x86_64/", "filter": {"rpm_group": ["System Environment/Base", "System Environment/Libraries","System Environment/Daemons","Development/Libraries"]}, "download_path": "/var/www/centos/base/7/extras/x86_64/"}, {"url": "http://dl.fedoraproject.org/pub/epel/7Server/x86_64/", "filter": {"rpm_group": ["System Environment/Base", "System Environment/Libraries","System Environment/Daemons","Development/Libraries"]}, "download_path": "/var/www/centos/epel/7Server/x86_64/"},{"url": "https://yum.dockerproject.org/repo/main/centos/7/", "download_path": "/var/www/centos/docker/7/"},{"url": "http://repo.zabbix.com/zabbix/3.0/rhel/7/x86_64/", "download_path": "/var/www/centos/zabbix/3.0/rhel/7/x86_64/"},{"url": "https://repo.saltstack.com/yum/redhat/7.4/x86_64/archive/2017.7.1/",  "download_path": "/var/www/centos/salt/7.4/x86_64/archive/2017.7.1/","url": "http://repo.zabbix.com/zabbix/3.4/rhel/7/x86_64/","download_path": "/var/www/centos/zabbix/3.4/rhel/7/x86_64/"}]

blacklist = []
whitelist = ["python-pycurl", "centos-release", "yum-plugin-fastestmirror", "setup", "mariadb-libs", "basesystem", "kbd-misc", "kbd-legacy", "firewalld-filesystem", "tuned", "firewalld", "libsepol", "libselinux", "info", "nspr", "audit", "nss-util", "chrony", "libcom_err","bc","chkconfig", "rsyslog", "bzip2-libs", "grep", "elfutils-libelf", "libffi", "man-db", "libattr", "xfsprogs", "libcap", "iprutils", "audit-libs", "jansson", "dbus-libs", "which", "rootfiles", "diffutils", "file-libs", "libmnl", "libnl3-cli", "p11-kit", "e2fsprogs-libs", "gpg-pubkey", "xz", "ncurses-base", "libunistring", "bash", "libedit", "sysvinit", "binutils", "ethtool", "nss-softokn-freebl", "libnfnetlink", "lzo", "keyutils-libs", "less", "ipset-libs", "tar", "acl", "ncurses", "pinentry", "libgcc", "mozjs17", "grub2-pc-modules", "make", "tzdata", "libselinux-utils", "glibc", "elfutils-libs", "systemd-libs", "systemd-resolved", "grub2-tools-minimal", "pth", "util-linux", "numactl-libs", "cryptsetup-libs", "nss-sysinit", "ustr", "libcurl", "libndp", "kmod-libs", "systemd", "libdaemon", "grub2-tools", "qrencode-libs", "grub2-pc", "device-mapper-persistent-data", "systemd-sysv", "libestr", "NetworkManager", "lsscsi", "linux-firmware", "libtasn1", "bind-libs-lite", "ca-certificates", "NetworkManager-wifi", "coreutils", "grub2", "python-libs", "openssh-server", "curl", "gzip", "kernel-tools", "libgomp", "shared-mime-info", "libpciaccess", "python-decorator", "libstdc++", "cracklib-dicts", "selinux-policy-targeted", "pam", "mlocate", "procps-ng", "procps", "python-pyudev", "gettext-libs", "dbus-glib", "pkgconfig", "libutempter", "python-iniparse", "python-jinja2","libselinux-python", "python-slip-dbus", "python-linux-procfs", "python-configobj", "openssl", "nss-pem", "centos-logos", "logrotate", "fipscheck-lib", "rpm-libs", "openldap", "gnupg2", "kpartx", "device-mapper-libs", "polkit", "iputils", "os-prober", "cronie-anacron", "lvm2-libs", "fxload", "plymouth-scripts", "gpgme", "rpm-build-libs", "dhcp-common", "python-urlgrabber", "yum","yum-utils", "yum-plugin-versionlock","yum-plugin-fastestmirror", "filesystem", "kbd", "lvm2", "pcre", "zlib", "xz-libs", "irqbalance", "sed", "haveged", "popt", "libdb", "biosdevname", "readline", "parted", "gawk", "passwd", "libxml2", "libxshmfence", "libgdiplus", "libgpg-erro", "btrfs-progs", "libacl", "libwayland-client", "libwayland-server", "e2fsprogs", "libcap-ng", "libsysfs", "libgcrypt", "libnl3", "expat", "lua", "findutils", "sqlite", "file", "libassuan", "cyrus-sasl-lib", "groff-base", "ncurses-libs", "libidn", "gmp", "cpio", "pciutils-libs", "hostname", "nss-softokn", "tcp_wrappers-libs", "gdbm", "libnetfilter_conntrack", "iproute", "libteam", "kernel", "kernel-headers", "kernel-firmware", "ipset", "vim-minimal", "sudo", "libdb-utils", "libss", "grub2-common", "selinux-policy", "GeoIP", "glibc-common", "elfutils-default-yama-scope", "libuuid", "freetype", "libblkid", "snappy", "libmount", "hardlink", "openssh", "libverto", "kernel-tools-libs", "dmidecode", "nss", "libsemanage", "NetworkManager", "kmod", "libseccomp", "dracut", "teamd", "grub2-tools-extra", "libaio", "dracut-network", "libpipeline", "wpa_supplicant", "bind-license", "libfastjson", "kernel", "p11-kit-trust", "NetworkManager-team", "openssl-libs", "kexec-tools", "krb5-libs", "dracut-config-rescue", "python", "microcode_ctl", "shadow-utils", "nss-tools", "cracklib", "openssh-clients", "glib2", "python-gobject-base", "python-perf", "libcroco", "iptables", "libpwquality", "plymouth-core-libs", "gettext", "dbus-python", "gobject-introspection", "yum-metadata-parser", "grubby", "pyliblzma", "python-slip", "python-firewall", "python-schedutils", "pyxattr", "alsa-lib", "fipscheck", "libssh2", "rpm", "dhcp-libs", "libuser", "policycoreutils", "device-mapper", "dbus", "polkit-pkla-compat", "initscripts", "device-mapper-event-libs", "crontabs", "cronie", "device-mapper-event", "ebtables", "hwdata", "libdrm", "plymouth", "virt-what", "pygpgme", "rpm-python", "dhclient","fontpackages-filesystem","libxcb","cairo","lyx-fonts","mono-wcf","mono-web","mono-extras","mono-mvc","mono-winforms","mono-data-sqlite","mono-data","mono-core","socat","open-vm-tools","libdnet","libverto-devel","wget","telnet","nmap-ncat","expect","tcl","libverto-libevent","clamav-update","clamav-server","clamav-filesystem","clamav-data","clamav-lib","java-1.8.0-openjdk","java-1.8.0-openjdk-headless","ntp","ntpdate","autogen-libopts","lz4","systemd-python","sysstat","perl","perl-macros","perl-libs","mc","iwl100-firmware", "iwl1000-firmware", "iwl105-firmware", "iwl135-firmware", "iwl2000-firmware", "iwl2030-firmware", "iwl3160-firmware", "iwl3945-firmware", "iwl4965-firmware", "iwl5000-firmware", "iwl5150-firmware", "iwl6000-firmware", "iwl6000g2a-firmware", "iwl6000g2b-firmware", "iwl6050-firmware", "iwl7260-firmware", "iwl7265-firmware","container-selinux","dejavu-fonts-common","libglvnd-egl","dejavu-sans-fonts","libglvnd-glx","libglvnd","python34","pciutils","checkpolicy","vim-common","python2-msgpack","python-ipaddress","lsof","vim-filesystem","vim-filesystem","vim-enhanced","python-backports-ssl", "python-backports-ssl_match_hostname","python2-psutil","xorg-x11-font-utils","python-urllib3","libsodium","lspci","keepalived","munin-node","perl-Net-CIDR","munin-common","postgresql-libs","hdparm"]

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def download_file(mirror, path, download_path):
    try:
        make_sure_path_exists(os.path.dirname(download_path + path))
        import urllib2
        u = urllib2.urlopen(mirror + path)
        f = open(download_path + path, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        print "Downloading: %s Bytes: %s" % (path.split('/')[-1], file_size)

        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            print status,

        f.close()
    except Exception as e:
        if e == KeyboardInterrupt:
            sys.exit(0)
        else:
            print("unable to download " + path)

def download_mirrors():
    for mirror in mirrors:
        print("#### " + mirror["url"] + " ####")
        try:
            links = {}
            files_list = []
            r  = urllib2.urlopen(mirror["url"] + 'repodata/')
            data = r.read()
            soup = BeautifulSoup(data, "html.parser")
            for link in soup.find_all('a'):
                try:
                    filename = link.get('href')
                    files_list.append(filename)
                    download_file(mirror["url"], "repodata/" + filename, mirror["download_path"])
                    if filename.endswith("primary.sqlite.bz2"):
                        links["sqlite"] = filename
                    elif filename.endswith("primary.xml.gz"):
                        links["xml"] = filename

                except Exception as e:
                    print(link.get('href') + " failed, " + str(e))

            if "sqlite" in links:
                with open('database.db', 'wb') as new_file, bz2.BZ2File(mirror["download_path"] + "repodata/" + links["sqlite"], 'rb') as file:
                    for data in iter(lambda : file.read(100 * 1024), b''):
                        new_file.write(data)
                conn = sqlite3.connect('database.db')

                cur = conn.cursor()
                filter_string = ""
                if "filter" in mirror and len(mirror["filter"]) != 0:
                    filter_string += "WHERE "
                    if len(whitelist) != 0:
                        filter_string += "name IN (\"" + ("\",\"").join(whitelist) + "\") OR "
                    for key, value in mirror["filter"].items():
                        if type(value) == list:
                            filter_string += key + " IN (\"" + ("\",\"").join(value) + "\"), "
                        else:
                            filter_string += key + " = \"" + value + "\", "
                    filter_string = filter_string[:-2]
                print("SELECT name, location_href FROM packages " + filter_string)
                cur.execute("SELECT name, location_href FROM packages " + filter_string)
                #result = cur.fetchall()
                for result in cur:
                    file_name = result[1].split('/')[-1]
                    if file_name not in blacklist:
                        files_list.append(file_name)
                        if not os.path.isfile(mirror["download_path"] + result[1]):
                            download_file(mirror["url"], result[1], mirror["download_path"])
                    else:
                        print(file_name + " is blaklisted")

            elif "xml" in links:
                file_content = ""
                with gzip.open(mirror["download_path"] + "repodata/" + links["xml"], 'rb') as f:
                    file_content = f.read()
                root = ET.fromstring(file_content)
                for package in root.findall('{http://linux.duke.edu/metadata/common}package'):
                    file_name = package.find('{http://linux.duke.edu/metadata/common}name').text
                    href = package.find('{http://linux.duke.edu/metadata/common}location').attrib["href"]
                    if file_name in whitelist:
                        files_list.append(href.split('/')[-1])
                        if not os.path.isfile(mirror["download_path"] + href):
                            download_file(mirror["url"], href, mirror["download_path"])


            ##cleanup
            for path, dirs, files in os.walk(mirror["download_path"]):
                for f in files:
                    if f not in files_list:
                        print(" remove " + path + "/" + f)
                        os.remove(path + "/" + f)
        except Exception as e:
            print(mirror["url"] + " failed, " + str(e))

if __name__ == "__main__":
    try:
        download_mirrors()
    except Exception as e:
        print("download stopped " + str(e))
