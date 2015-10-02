# -*- Mode:Python; indent-tabs-mode:nil; tab-width:4 -*-
import duplicity.backend
from duplicity import log
from duplicity import path
from duplicity import util
from duplicity.errors import * #@UnusedWildImport
import hashlib
import json


class AcdBackend(duplicity.backend.Backend):
    """Use Amazon Cloud Drive

    Urls look like acd://testfiles/output.
    """
    def __init__(self, parsed_url):
        duplicity.backend.Backend.__init__(self, parsed_url)
        if not parsed_url.path.startswith('//'):
            raise BackendException("Bad file:// path syntax.")
        self.remote_pathdir = path.Path(parsed_url.path[1:])
        directories = self.remote_pathdir.name.split("/")[1:]
        for i in range(len(directories)):
            command = "acdcli mkdir /%s" % "/".join(directories[:i+1])
            self.subprocess_popen(command)
 
    def put(self, source_path, remote_filename = None):
        log.Info("Writing %s" % target_path.name)
        command = "acdcli ul %s %s" % (source_path.name, self.remote_pathdir.name)
        self.subprocess_popen(command)
        remote_path = self.remote_pathdir.append(source_path.get_filename())
        if remote_filename:
            command = "acdcli rn %s %s" % (remote_path.name, remote_filename)
            self.subprocess_popen(command)
            remote_path = self.remote_pathdir.append(remote_filename)
        _, stdout, _ = self.subprocess_popen("acdcli metadata %s" % remote_path.name)
        target_md5 = json.loads(stdout)['contentProperties']['md5']
        source_md5 = self._md5(source_path)
        if source_md5 != target_md5:
            error = "md5 hashes do not match %s != %s" % (source_md5, target_md5)
            raise BackendException(error)
        
    def get(self, remote_filename, local_path):
        remote_path = self.remote_pathdir.append(remote_filename)
        command = "acdcli dl %s %s" % (remote_path.name, local_path.get_parent_dir())
        self.subprocess_popen(command)
        if remote_filename != local_path.get_filename():
            current_path = path.Path(local_path.get_parent_dir())
            current_path = current_path.append(remote_filename)
            current_path.rename(local_path)

    def _list(self):
        command = "acdcli ls %s" % self.remote_pathdir.name
        _, stdout, _ = self.subprocess_popen(command)
        filename_list = [line.split()[2] for line in stdout.split('\n')[:-1]]
        return filename_list
        
    def delete(self, filename_list):
        for filename in filename_list:
            remote_path = self.remote_pathdir.append(filename)
            command = "acdcli rm %s" % remote_path.name
            self.subprocess_popen(command)

    def _md5(self, fname):
        hash = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash.update(chunk)
        return hash.hexdigest()

duplicity.backend.register_backend("acd", AcdBackend)
