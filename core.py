import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional
import logging

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError


class StorageClient(ABC):
    """Abstract base class for storage clients."""

    @abstractmethod
    def list_files(self, path: str) -> List[str]:
        """List files at the given path."""
        pass

    @abstractmethod
    def download_file(self, source_path: str, local_path: str) -> None:
        """Download file from storage to local path."""
        pass

    @abstractmethod
    def upload_file(self, local_path: str, dest_path: str) -> None:
        """Upload file from local path to storage."""
        pass


class AzureDataLakeClient(StorageClient):
    def __init__(self, account_url: str, credential: Optional[str] = None):
        """
        Initialize Azure Data Lake client.

        :param account_url: URL of the storage account, e.g. https://<account_name>.dfs.core.windows.net
        :param credential: Credential string or token for authentication
        """
        self.service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
        logging.debug(f"AzureDataLakeClient initialized with account_url: {account_url}")

    def list_files(self, path: str) -> List[str]:
        """List files in the given filesystem and directory path.

        The path should be in the format 'filesystem/directory_path'.
        """
        try:
            filesystem_name, *dir_path_parts = path.split('/', 1)
            directory_path = dir_path_parts[0] if dir_path_parts else ''
            filesystem_client = self.service_client.get_file_system_client(filesystem_name)
            paths = filesystem_client.get_paths(path=directory_path)
            files = [p.name for p in paths if not p.is_directory]
            logging.debug(f"Listed {len(files)} files in {path}")
            return files
        except AzureError as e:
            logging.error(f"Failed to list files in Azure Data Lake path {path}: {e}")
            raise

    def download_file(self, source_path: str, local_path: str) -> None:
        """Download file from Azure Data Lake to local path.

        source_path format: 'filesystem/path/to/file'
        """
        try:
            filesystem_name, file_path = source_path.split('/', 1)
            filesystem_client = self.service_client.get_file_system_client(filesystem_name)
            file_client = filesystem_client.get_file_client(file_path)
            download = file_client.download_file()
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'wb') as f:
                download.readinto(f)
            logging.debug(f"Downloaded file from {source_path} to {local_path}")
        except AzureError as e:
            logging.error(f"Failed to download file from Azure Data Lake {source_path}: {e}")
            raise

    def upload_file(self, local_path: str, dest_path: str) -> None:
        """Upload file from local path to Azure Data Lake.

        dest_path format: 'filesystem/path/to/file'
        """
        try:
            filesystem_name, file_path = dest_path.split('/', 1)
            filesystem_client = self.service_client.get_file_system_client(filesystem_name)
            file_client = filesystem_client.get_file_client(file_path)
            with open(local_path, 'rb') as f:
                file_contents = f.read()
            file_client.upload_data(file_contents, overwrite=True)
            logging.debug(f"Uploaded file from {local_path} to {dest_path}")
        except AzureError as e:
            logging.error(f"Failed to upload file to Azure Data Lake {dest_path}: {e}")
            raise


class WindowsNetworkFolderClient(StorageClient):
    def __init__(self, base_path: str):
        """
        Initialize Windows network folder client.

        :param base_path: Base path of the network folder, e.g. \\server\share or mapped drive like Z:\
        """
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            raise ValueError(f"Base path {base_path} does not exist or is not accessible.")
        logging.debug(f"WindowsNetworkFolderClient initialized with base_path: {base_path}")

    def list_files(self, path: str) -> List[str]:
        """List files in the given relative path from base_path."""
        full_path = self.base_path / path
        if not full_path.exists():
            raise FileNotFoundError(f"Path {full_path} does not exist.")
        files = [str(p.relative_to(self.base_path)) for p in full_path.rglob('*') if p.is_file()]
        logging.debug(f"Listed {len(files)} files in {full_path}")
        return files

    def download_file(self, source_path: str, local_path: str) -> None:
        """Copy file from network folder to local path."""
        full_source_path = self.base_path / source_path
        if not full_source_path.exists():
            raise FileNotFoundError(f"Source file {full_source_path} does not exist.")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        shutil.copy2(full_source_path, local_path)
        logging.debug(f"Copied file from {full_source_path} to {local_path}")

    def upload_file(self, local_path: str, dest_path: str) -> None:
        """Copy file from local path to network folder."""
        full_dest_path = self.base_path / dest_path
        os.makedirs(full_dest_path.parent, exist_ok=True)
        shutil.copy2(local_path, full_dest_path)
        logging.debug(f"Copied file from {local_path} to {full_dest_path}")


class FileTransferManager:
    def __init__(self, source_client: StorageClient, dest_client: StorageClient):
        self.source_client = source_client
        self.dest_client = dest_client
        logging.debug(f"FileTransferManager initialized with source {type(source_client).__name__} and destination {type(dest_client).__name__}")

    def transfer_file(self, source_path: str, dest_path: str) -> None:
        """Transfer a single file from source to destination."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            local_temp_path = os.path.join(tmpdir, os.path.basename(source_path))
            logging.info(f"Starting transfer of {source_path} to {dest_path}")
            self.source_client.download_file(source_path, local_temp_path)
            self.dest_client.upload_file(local_temp_path, dest_path)
            logging.info(f"Completed transfer of {source_path} to {dest_path}")

    def transfer_files(self, source_dir: str, dest_dir: str) -> None:
        """Transfer all files from source directory to destination directory."""
        files = self.source_client.list_files(source_dir)
        logging.info(f"Found {len(files)} files to transfer from {source_dir} to {dest_dir}")
        for file_rel_path in files:
            source_file_path = f"{source_dir.rstrip('/')}/{file_rel_path}"
            dest_file_path = f"{dest_dir.rstrip('/')}/{file_rel_path}"
            self.transfer_file(source_file_path, dest_file_path)


def create_storage_client(storage_type: str, **kwargs) -> StorageClient:
    """
    Factory function to create storage client.

    :param storage_type: 'azure_datalake' or 'windows_network'
    :param kwargs: parameters for client initialization
    :return: StorageClient instance
    """
    storage_type = storage_type.lower()
    if storage_type == 'azure_datalake':
        return AzureDataLakeClient(**kwargs)
    elif storage_type == 'windows_network':
        return WindowsNetworkFolderClient(**kwargs)
    else:
        raise ValueError(f"Unsupported storage_type: {storage_type}")


# Configure logging for the library
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

