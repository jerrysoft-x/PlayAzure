import base64
import glob
import os

import fire
import math
from azure.datalake.store import lib, core
from azure.datalake.store.multithread import ADLUploader, ADLDownloader
import logging

from hdfs3 import HDFileSystem
from multiprocessing import Pool, Manager


class ADLUtils(object):
    """
    Azure Data Lake Store Utilities

    In case you have any questions please contact jerry.li@dnvgl.com

    Prerequisites:
    1. Install Python 3.6 or later version
    2. Install the libraries used in this script
        pip install azure-datalake-store
        pip install fire
        pip install hdfs3

    Common login parameters:
    --tenant_id         The Directory ID of Azure Active Directory of DNV GL
    --store_name        The name of the Azure Data Lake Store
    --client_id         The Application ID of the WebApp account
    --client_secret     The Key of the WebApp account
    --encrypted         If True, the values is supposed to be encrypted with utf-8 and base64 encoding.
                        Defaults to False.
    --hdfs_host         host of HDFS
    --hdfs_port         port of HDFS
    """

    def __init__(self, tenant_id='adf10e2b-b6e9-41d6-be2f-c12bb566019c', store_name='adljjlitest',
                 client_id='e649e5f4-0791-4447-9022-a4d3c6725f90',
                 client_secret='qE3Nb2Zvm4TLBIjI3+ueurEnFeTmBI13BGv/ItBZz/E=',
                 encrypted=False, hdfs_host='biccnode01', hdfs_port=8020):
        # Configure logging module
        # logging.basicConfig(filename='adlutils.log', level=logging.DEBUG, filemode='w',
        #                     format='%(asctime)s %(levelname)s:%(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)s:%(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
        logging.info('Initializing Azure Data Lake utility...')

        # The Directory ID of Azure Active Directory of DNV GL
        self._tenant_id = tenant_id
        # The name of the Azure Data Lake Store
        self._store_name = store_name
        # The Application ID of the WebApp account
        self._client_id = client_id
        # The Key of the WebApp account
        self._client_secret = client_secret
        # If True, the values is supposed to be encrypted with utf-8 and base64 encoding. Defaults to False.
        self._encrypted = encrypted
        # The host of HDFS
        self._hdfs_host = hdfs_host
        # The port of HDFS
        self._hdfs_port = hdfs_port

        # If authentication values are encrypted, take care of the decode.
        if encrypted:
            self._tenant_id = self._b64decode(self._tenant_id)
            self._store_name = self._b64decode(self._store_name)
            self._client_id = self._b64decode(self._client_id)
            self._client_secret = self._b64decode(self._client_secret)

        # Azure DataLake File System
        self._adl = None  # Instantiation is moved to a factory method _getADL

        # HDFS client
        self._hdfs = None  # Instantiation is moved to a factory method _getHDFS

        logging.debug('Finished initialing Azure Data Lake utility.')

    def _getadl(self):
        """
        The factory method to get a singleton of Azure Data Lake Store
        :return: A singleton of Azure Data Lake Store
        """
        logging.debug('Start method _getADL')
        if self._adl is None:
            self._token = lib.auth(tenant_id=self._tenant_id, client_id=self._client_id,
                                   client_secret=self._client_secret)
            self._adl = core.AzureDLFileSystem(store_name=self._store_name, token=self._token)
            logging.info('Finished building Azure Data Lake File System.')
        logging.debug('Finished method _getADL')
        return self._adl

    def _gethdfs(self):
        """
        The factory method to get a singleton of HDFS
        :return: A singleton of HDFS
        """
        logging.debug('Start building HDFS File System...')
        if self._hdfs is None:
            self._hdfs = HDFileSystem(host=self._hdfs_host,
                                      port=self._hdfs_port)
            logging.info('Finished building HDFS File System.')
        return self._hdfs

    @staticmethod
    def _getpool():
        """
        Get an instance of process pool with a pool size is same to the number of CPU cores.
        :return: An instance of process pool
        """
        logging.info(
            "Creating a process pool with pool size {processes} (the number of CPU cores)...".format(
                processes=os.cpu_count() or 1))
        return Pool()

    @staticmethod
    def version():
        """
        Print out the version of the utility.
        :return: None
        """
        print('version: 1.21')

    @staticmethod
    def release_notes():
        """
        Print out the release notes.
        :return: None
        """
        release_notes = '''
20170823 version 1.21
    - new method adls2hdfs and hdfs2adls for copying file(s) between Azure DataLake Store and HDFS,
      no access to local disk file, only pass through memory, using multiprocessing.
      So far logging is reporting some warning and error from HDFS3 library because HDFS is not well
      configured, although the copy is not badly affected. If required will consider add filter on those
      messages in later version.
    - Update method download, upload, hdfsput and hdfsget to use multiprocessing, use a process pool
      with processes number is same to the number of CPU cores by default, launch a sub-process for
      transferring each single file.
    - add recursive parameter in method download, upload and hdfsput for enabling the ability to
      search files in sub-directories.
      Why not in hdfsget is because hdfs3 lib is not giving such a function.
    - Change the default value of nthreads in upload and download methods to None, use the number of
      CPU cores.
    - Update all codes for compliance to PEP 8.
    - new method info for getting path information as a dict from Azure DataLake Store.
    - new method hdfsglob
    - Fix two bugs in glob method when recursive is False.
20170814 version 1.20
    - Enable basic logging and output to console.
    - For supporting wildcard, use glob method on different file systems rather than using re pattern.
    - upload method now is for uploading local file(s) to Azure Data Lake Store, support wildcard.
    - new method hdfsget for downloading file(s) from HDFS to local, support wildcard.
    - new method hdfsput for uploading file(s) from local to HDFS, support wildcard.
    - new method hdfsls for listing file(s) at path in HDFS
    - new method hdfsinfo for getting file information as a dict from HDFS
20170720 version 1.12
    - download from Azure Data Lake Store now support wildcard * and ?
20170712 version 1.11
    - remove help method, add docstrings to each open method for being used in -- --help flag
20170711 version 1.10
    - integrate with HDFS
20170710 version 1.00
    - initialize        
'''
        print(release_notes)

    def ls(self, path='', detail=False):
        """
        List the files and directories under the specific path. list and ls are synonyms.
        :param path: The specific path of Azure Data Lake Store, defaults to '' which is root path.
        :param detail: If True, list with details. Defaults to False.
        :return: None
        """
        logging.info(
            'Start listing files and directories under path: {path} {detail} details...'.format(
                path=path if path != '' else 'root', detail='with' if detail else 'without'))
        for item in self._getadl().ls(path, detail):
            print(item)
            logging.debug('item: {item}'.format(item=str(item)))
        logging.info('Finished listing files and directories.')

    def walk(self, path=''):
        """
        Get all files below given path
        :param path: The path in Azure Data Lake Store, defaults to '' which is the root path.
        :return: None
        """
        print(self._getadl().walk(path))

    def du(self, path='', total=True, deep=True, ):
        """
        Get bytes in keys at path
        :param path: The path in Azure Data Lake Store, defaults to '' which is the root path.
        :param total: If True, sum the bytes. Defaults to True.
        :param deep: If True, recursively count on all children in the sub-directories. Defaults to True.
        :return: None
        """
        print('Usage(bytes):', self._getadl().du(path, total, deep))  # total bytes usage

    def info(self, path):
        """
        File information in Azure DataLake Store
        :param path: path
        :return: information as a dict
        """
        return self._getadl().info(path)

    def glob(self, path, details=False, recursive=False):
        """
        Find files (not directories) by glob-matching.
        :param path: glob-matching pattern
        :param details: whether return with details as a dict
        :param recursive: whether search files under sub-directories, defaults to False
        :return: a list of matching files
        """
        level = len(path.split('/'))
        if path.startswith('/'):
            level -= 1
        if self._getadl().exists(path):
            if self._getadl().info(path)['type'] == 'DIRECTORY':
                level += 1
        matching_files = self._getadl().glob(path, details=details)
        if recursive:
            return matching_files
        else:
            return [f for f in matching_files if len((f['name'] if details else f).split('/')) == level]

    def download(self, lpath, rpath, recursive=False, nthreads=None, overwrite=True, buffersize=4194304,
                 blocksize=4194304):
        """
        Download file(s) from Azure DataLake Store to local, support downloading file and directory, when downloading
        file(s) wildcard * and ? are supported while * represent any number of characters and ? represent one character.
        :param lpath: local path
        :param rpath: remote path (the path in Azure Data Lake Store)
        :param recursive: Whether search files in sub-directories, defaults to False.
        :param nthreads: Number of threads to use. If None, uses the number of cores. Defaults to None.
        :param overwrite: Whether to forcibly overwrite existing files/directories.
        :param buffersize: Number of bytes for internal buffer. This block cannot be bigger than a chunk and cannot be
                            smaller than a block. Defaults to 2**22.
        :param blocksize: Number of bytes for a block. Within each chunk, we write a smaller block for each API call.
                           This block cannot be bigger than a chunk. Defaults to 2**22.
        :return: None
        """
        logging.debug('Starting method: download')
        logging.info(
            'Start downloading file {rpath} from Azure DataLake Store to local at {lpath}'.format(
                rpath=rpath, lpath='root' if lpath == '' else lpath))
        p = self._getpool()
        filelist = self.glob(rpath, recursive=recursive)
        logging.info('Find {num} file(s) to download: {list}'.format(num=len(filelist), list=filelist))
        for f in filelist:
            local_filename = f.split('/')[-1]
            local_path = local_filename if lpath == '' else lpath + local_filename if lpath.endswith(
                os.sep) else os.path.join(lpath, local_filename)
            logging.info('Launching a sub-process for downloading {azure_path}...'.format(azure_path=f))
            p.apply_async(ADLDownloader,
                          kwds={'adlfs': self._getadl(), 'lpath': local_path, 'rpath': f, 'nthreads': nthreads,
                                'overwrite': overwrite, 'buffersize': buffersize, 'blocksize': blocksize})
        p.close()
        p.join()
        logging.info('Download completed.')
        logging.debug('Finished method: download')

    def upload(self, lpath, rpath, recursive=False, nthreads=None, overwrite=True, buffersize=4194304,
               blocksize=4194304):
        """
        Upload file(s) from local to Azure Data Lake Store, support downloading file and directory, support wildcard.
        :param lpath: local path
        :param rpath: remote path (the path in Azure Data Lake Store)
        :param recursive: Whether search files in sub-directories, defaults to False.
        :param nthreads: Number of threads to use. If None, uses the number of cores. Defaults to None.
        :param overwrite: Whether to forcibly overwrite existing files/directories.
        :param buffersize: Number of bytes for internal buffer. This block cannot be bigger than a chunk and cannot be
                            smaller than a block. Defaults to 2**22.
        :param blocksize: Number of bytes for a block. Within each chunk, we write a smaller block for each API call.
                           This block cannot be bigger than a chunk. Defaults to 2**22.
        :return: None
        """
        logging.debug('Start method: upload')
        logging.info('Start uploading file {lpath} from local to Azure DataLake Store at {rpath}'.format(
            lpath=lpath, rpath='root' if rpath == '' else rpath))
        p = self._getpool()
        filelist = glob.glob(lpath, recursive=recursive)
        logging.info('Find {num} file(s) to upload: {list}'.format(num=len(filelist), list=filelist))
        for f in filelist:
            filename = f.split(os.sep)[-1]
            remote_path = filename if rpath == '' else rpath + filename if rpath.endswith('/') else '/'.join(
                (rpath, filename))
            logging.info('Launching a sub-process for uploading {local_path}...'.format(local_path=f))
            p.apply_async(ADLUploader,
                          kwds={'adlfs': self._getadl(), 'lpath': f, 'rpath': remote_path, 'nthreads': nthreads,
                                'overwrite': overwrite, 'buffersize': buffersize, 'blocksize': blocksize})
        p.close()
        p.join()
        logging.info('Upload completed.')
        logging.debug('Finished method: upload')

    def hdfsls(self, path, detail=False):
        """
        List files at path in HDFS
        :param path: Location at which to list files
        :param detail: If True, each list item is a dict of file properties; otherwise, return list of filenames.
                        Defaults to False.
        :return: List of files
        """
        logging.debug('Start method: hdfsls')
        file_list = self._gethdfs().ls(path, detail=detail)
        logging.info('HDFS: ls at {path}...'.format(path=path))
        if not file_list:
            logging.info('No entries found.')
        logging.debug('Finished method: hdfsls')
        return file_list

    def hdfsglob(self, path):
        """
        Get list of paths matching glob-like pattern (i.e. with * ?) in HDFS
        :param path: path
        :return: list of paths
        """
        return self._gethdfs().glob(path)

    def hdfsinfo(self, path):
        """
        Filesystem metadata about this file in HDFS
        :param path: path
        :return: information as a dict
        """
        return self._gethdfs().info(path)

    def hdfsget(self, hdfs_path, local_path):
        """
        Download file(s) from HDFS path to local path, wildcard is supported.
        :param hdfs_path: The source file path in HDFS
        :param local_path: local file path
        :return:
        """
        logging.debug('Start method: hdfsget')
        logging.info(
            'Start downloading {hdfs_path} from HDFS to local at {local_path}'.format(hdfs_path=hdfs_path,
                                                                                      local_path=local_path))
        p = self._getpool()
        filelist = self._gethdfs().glob(hdfs_path)
        logging.info('Find {num} file(s) to download: {list}'.format(num=len(filelist), list=filelist))
        for f in filelist:
            filename = f.split('/')[-1]
            target_path = None
            if local_path == '':
                target_path = '.' + os.sep + filename
            elif os.path.exists(local_path):
                if os.path.isdir(local_path):
                    target_path = os.path.join(local_path, filename)
            else:
                raise ValueError('path is not existing in the system')
            logging.info('Launching a sub-process for downloading {hdfs_path} to {local_path}...'.format(
                hdfs_path=f, local_path=target_path))
            p.apply_async(self._gethdfs().get, args=(f, target_path), callback=lambda x: logging.info(
                'File {hdfs_path} has been downloaded from HDFS to local at {local_path}'.format(
                    hdfs_path=f, local_path=target_path)))
        p.close()
        p.join()
        logging.info('HDFS: get completed.')
        logging.debug('Finish method: hdfsget')

    def hdfsput(self, local_path, hdfs_path, recursive=False):
        """
        Upload file(s) from local path to HDFS path, wildcard is supported.
        :param local_path: local file path
        :param hdfs_path: The target file path in HDFS
        :param recursive: whether to search files in sub-directories, defaults to False
        :return: None
        """
        logging.debug('Start method: hdfsput')
        logging.info(
            'Start uploading local file(s) from {local_path} to HDFS at {hdfs_path}'.format(
                local_path=local_path,
                hdfs_path=hdfs_path))
        p = self._getpool()
        filelist = glob.glob(local_path, recursive=recursive)
        logging.info('Find {num} file(s) to download: {list}'.format(num=len(filelist), list=filelist))
        for f in filelist:
            filename = f.split(os.sep)[-1]
            hdfs_path_info = self.hdfsinfo(hdfs_path)
            target_path = hdfs_path
            if hdfs_path_info:
                if hdfs_path_info['kind'] == 'directory':
                    target_path = ('' if hdfs_path.endswith('/') else '/').join([hdfs_path, filename])
            logging.info('Launching a sub-process for uploading {local_path} to {hdfs_path}...'.format(
                local_path=f, hdfs_path=target_path))
            p.apply_async(self._gethdfs().put, args=(f, target_path), callback=lambda x: logging.info(
                'Local file {local_path} has been uploaded to HDFS at path {hdfs_path}'.format(
                    local_path=f, hdfs_path=target_path)))
        p.close()
        p.join()
        logging.info('HDFS: put completed.')
        logging.debug('Finish method: hdfsput')

    def _copy_single_file(self, mode, adls_path, hdfs_path, length=2 ** 16):
        """
        function used to run a sub-process for copying a single file from Azure DataLake Store to HDFS
        :param mode: 2 options: 'adls2hdfs' or 'hdfs2adls', specify how the copy operation works
        :param adls_path: source path in Azure DataLake Store
        :param hdfs_path: target path in HDFS
        :param length: the number of bytes to read and write at a time
        :return: the logging message delivered to callback
        """
        if mode not in ('adls2hdfs', 'hdfs2adls'):
            raise ValueError('Invalid copy mode')
        adls2hdfs = (mode == 'adls2hdfs')
        with self._getadl().open(adls_path, mode='rb') \
                if adls2hdfs else self._gethdfs().open(hdfs_path, mode='rb') as reader:
            with self._gethdfs().open(hdfs_path, mode='wb') \
                    if adls2hdfs else self._getadl().open(adls_path, mode='wb') as writer:
                data = reader.read(length)
                while data:
                    writer.write(data)
                    data = reader.read(length)
        return 'File {source_path} has been transferred from {source} to {target} at {target_path}'.format(
            source_path=adls_path if adls2hdfs else hdfs_path,
            source='Azure DataLake Store' if adls2hdfs else 'HDFS',
            target_path=hdfs_path if adls2hdfs else adls_path,
            target='HDFS' if adls2hdfs else 'Azure DataLake Store')

    def adls2hdfs(self, adls_path, hdfs_path, recursive=False):
        """
        Copy file(s) from Azure DataLake Store to HDFS
        :param adls_path: source path in Azure DataLake Store
        :param hdfs_path: target path in HDFS
        :param recursive: whether to search file(s) in sub-directories in Azure DataLake Store
        :return: None
        """
        logging.debug('Start method: adls2hdfs')
        p = self._getpool()
        filelist = self.glob(adls_path, recursive=recursive)
        logging.info('Find {num} file(s) to download: {list}'.format(num=len(filelist), list=filelist))
        for f in filelist:
            filename = f.split('/')[-1]
            hdfs_path_info = self.hdfsinfo(hdfs_path)
            target_path = hdfs_path
            if hdfs_path_info:
                if hdfs_path_info['kind'] == 'directory':
                    target_path = ('' if hdfs_path.endswith('/') else '/').join([hdfs_path, filename])
            logging.info('Launching a sub-process for copying {adls_path} to {hdfs_path}...'.format(
                adls_path=f, hdfs_path=target_path))
            p.apply_async(self._copy_single_file,
                          kwds={'mode': 'adls2hdfs', 'adls_path': f, 'hdfs_path': target_path},
                          callback=lambda s: logging.info(s))
        p.close()
        p.join()
        logging.info('Copy completed.')
        logging.debug('Finish method: adls2hdfs')

    def hdfs2adls(self, hdfs_path, adls_path):
        """
        Copy file(s) from HDFS to Azure DataLake Store, not supporting recursive search because HDFS3 library so far
        does not provide such a function.
        :param hdfs_path: source path in HDFS
        :param adls_path: target path in Azure DataLake Store
        :return: None
        """
        logging.debug('Start method: hdfs2adls')
        p = self._getpool()
        filelist = self._gethdfs().glob(hdfs_path)
        logging.info('Find {num} file(s) to download: {list}'.format(num=len(filelist), list=filelist))
        for f in filelist:
            filename = f.split('/')[-1]
            target_path = filename if adls_path == '' else adls_path + filename if adls_path.endswith(
                '/') else '/'.join(
                (adls_path, filename))
            logging.info('Launching a sub-process for uploading {local_path}...'.format(local_path=f))
            p.apply_async(self._copy_single_file,
                          kwds={'mode': 'hdfs2adls', 'hdfs_path': f, 'adls_path': target_path},
                          callback=lambda s: logging.info(s))
        p.close()
        p.join()
        logging.info('Copy completed.')
        logging.debug('Finished method: hdfs2adls')

    @staticmethod
    def _b64decode(source):
        """
        Decode base64 input.
        :param source: encrypted input
        :return: decoded value
        """
        # decoding base64 to bytes
        b = base64.b64decode(source)
        # decoding bytes to str with utf-8
        return b.decode('utf-8')

    # ALIASES
    list = ls


if __name__ == '__main__':
    fire.Fire(ADLUtils)
