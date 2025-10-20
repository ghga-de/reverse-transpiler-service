# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DAO Port definition"""

import re
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from typing import Any

from gridfs import AsyncGridFS
from gridfs.errors import NoFile
from hexkit.protocols.dao import ResourceNotFoundError
from openpyxl import Workbook

from rts.models import StudyMetadata

__all__ = [
    "GridFSDao",
    "GridFSDaoFactoryPort",
    "MetadataGridFSDaoPort",
    "ResourceNotFoundError",
    "WorkbookGridFSDaoPort",
]


class GridFSDao[InputType: Any, OutputType: Any]:
    """A class that enables basic utilization of GridFS.

    This class could go into a library at some point.
    """

    def __init__(
        self,
        *,
        grid_fs: AsyncGridFS,
        prefix: str,
        file_extension: str = "",
        serialize_fn: Callable[[InputType], bytes],
        deserialize_fn: Callable[[bytes], OutputType],
    ):
        """Initialize the WorkbookDao with the provided AsyncGridFS instance.

        Args:
            grid_fs: Instantiated AsyncGridFS object
            prefix: A prefix used to attach to all stored file names. This is used
                in case objects of different kinds are stored with the same identifier.
            file_extension: An optional file extension to use when storing the data.
            serialize_fn: A function that serializes the input data to bytes before
                storage in GridFS.
            deserialize_fn: A function that deserializes the data from bytes to the
                desired format.
        """
        self._grid_fs = grid_fs
        self._prefix = prefix
        self._file_extension = file_extension
        self._serialize_fn = serialize_fn
        self._deserialize_fn = deserialize_fn

    def prefixed_id(self, id_: str) -> str:
        """Prepend the instance prefix to the data identifier"""
        return f"{self._prefix}{id_}"

    async def upsert(self, *, data: InputType, id_: str) -> None:
        """Upsert the data for a given identifier.

        If a file with the same identifier already exists, it will be replaced.

        To avoid conflicts with identically named files of other kinds, the identifier
        is automatically prefixed.
        """
        # Serialize the data:
        serialized_data = self._serialize_fn(data)

        # Delete the file if it already exists
        prefixed_id = self.prefixed_id(id_)
        await self._grid_fs.delete(prefixed_id)

        # Insert new file
        await self._grid_fs.put(
            data=serialized_data,
            _id=prefixed_id,
            filename=f"{prefixed_id}{self._file_extension}",
        )

    async def find(self, *, id_: str) -> OutputType:
        """Retrieve the file for a given identifier (do not include the prefix).

        Raises `ResourceNotFoundError` if the file does not exist for the
        given identifier.
        """
        prefixed_id = self.prefixed_id(id_)
        try:
            gridfs_file_object = await self._grid_fs.get(prefixed_id)
        except NoFile as err:
            raise ResourceNotFoundError(id_=id_) from err

        serialized_data = await gridfs_file_object.read()
        deserialized_data = self._deserialize_fn(serialized_data)
        return deserialized_data

    async def find_all(self) -> AsyncIterator[OutputType]:
        """Returns an iterator of all files beginning with this DAO's assigned prefix"""
        regx = re.compile(f"^{self._prefix}.+", re.IGNORECASE)
        async for file in self._grid_fs.find({"filename": {"$regex": regx}}):
            file_bytes = await file.read()
            yield self._deserialize_fn(file_bytes)

    async def delete(self, *, id_: str) -> None:
        """Delete the file for a given identifier (do not include the prefix).

        Does not raise an error if the file does not exist, as GridFS doesn't raise
        an error when trying to delete a non-existent file.
        """
        await self._grid_fs.delete(self.prefixed_id(id_))


WorkbookGridFSDaoPort = GridFSDao[Workbook, bytes]
MetadataGridFSDaoPort = GridFSDao[StudyMetadata, StudyMetadata]


class GridFSDaoFactoryPort(ABC):
    """Port definition of a factory that produces objects able to interact with GridFS"""

    @abstractmethod
    def get_metadata_dao(self) -> MetadataGridFSDaoPort:
        """Return a MetadataDaoPort instance"""

    @abstractmethod
    def get_workbook_dao(self) -> WorkbookGridFSDaoPort:
        """Return a WorkbookDaoPort instance"""
