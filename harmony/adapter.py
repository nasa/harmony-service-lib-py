from abc import ABC, abstractmethod
from os import path, makedirs
import shutil
import os
from uuid import uuid4
from .util import download, stage, callback_with_error, callback_with_redirect

class BaseHarmonyAdapter(ABC):
    def __init__(self, message):
        """
        Constructs the adapter

        Parameters
        ----------
        self.message : harmony.Message
            The Harmony input which needs acting upon
        """
        self.message = message
        self.temp_paths = []
        self.is_complete = False

    def cleanup(self):
        # This is not really necessary if using Docker
        for temp_path in self.temp_paths:
            shutil.rmtree(temp_path)

        if 'tmp/harmony' in self.temp_paths:
            # Also clean up the root of our default temp dir
            try:
                os.rmdir('tmp')
            except OSError:
                pass # Happens if there's something in the tmp dir other than harmony things

    def download_granules(self, temp_dir='tmp/harmony'):
        # Using a local temp dir rather than the more conventional mkdtemp aids significantly in debugging,
        # as we can mount this directory to the docker container to view intermediate files.  Because this
        # is run in docker, files in production are ephemeral
        makedirs(temp_dir, exist_ok=True)
        self.temp_paths += [temp_dir]

        granules = self.message.granules

        # Download the remote file
        for granule in granules:
            granule.local_filename = download(granule.url, temp_dir)

    def stage(self, local_file, remote_filename=None, mime=None):
        # If no remote filename is provided, generate one with a UUID and the same extension as the local file
        if remote_filename is None:
            remote_filename = str(uuid4()) + path.splitext(local_file)

        if mime is None:
            mime = self.message.format.mime

        return stage(local_file, remote_filename, mime)

    def completed_with_error(self, error_message):
        if self.is_complete:
            raise Exception('Attempted to error an already-complete service call with message ' + error_message)
        callback_with_error(self.message, error_message)
        self.is_complete = True

    def completed_with_redirect(self, url):
        if not self.is_complete:
            raise Exception('Attempted to redirect an already-complete service call to ' + url)
        callback_with_redirect(self.message, url)
        self.is_complete = True

    def completed_with_local_file(self, filename, output_filename=None, mime=None):
        url = self.stage(filename, output_filename, mime)
        self.completed_with_redirect(url)

    @abstractmethod
    def invoke(self):
        """
        Invokes the service, calling back to Harmony as appropriate

        Returns
        -------
        None
        """
        pass