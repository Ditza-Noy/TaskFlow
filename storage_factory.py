# storage_factory.py
import os
from typing import Union
from file_storage import FileStorage
from s3_storage import S3Storage
def create_storage(use_s3: bool = False) -> Union[FileStorage, S3Storage]:
	"""Factory function to create appropriate storage implementation."""
	if use_s3 is False:
		use_s3 = os.getenv('USE_S3', 'false').lower() == 'true'
	if use_s3:
		print("Using Amazon S3 for file storage")
		return S3Storage()
	else:
		print("Using local file system storage")
		return FileStorage()