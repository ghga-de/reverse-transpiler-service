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

"""DAO implementation"""

import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from io import BytesIO

from gridfs.asynchronous import AsyncGridFS
from hexkit.providers.mongodb.provider import ConfiguredMongoClient, MongoDbConfig
from openpyxl import Workbook

from rts.models import StudyMetadata
from rts.ports.outbound.dao import (
    GridFSDao,
    GridFSDaoFactoryPort,
    MetadataGridFSDaoPort,
    WorkbookGridFSDaoPort,
)

log = logging.getLogger(__name__)

__all__ = ["GridFSDaoFactory"]


class GridFSDaoFactory(GridFSDaoFactoryPort):
    """A factory that produces objects able to interact with GridFS"""

    @classmethod
    @asynccontextmanager
    async def construct(
        cls, *, config: MongoDbConfig
    ) -> AsyncGenerator["GridFSDaoFactory"]:
        """Instantiate a GridFSDaoFactory with a configured MongoDB client"""
        async with ConfiguredMongoClient(config=config) as client:
            db = client[config.db_name]
            grid_fs = AsyncGridFS(db)
            yield GridFSDaoFactory(grid_fs=grid_fs)

    def __init__(self, *, grid_fs: AsyncGridFS):
        self._grid_fs = grid_fs

    def get_metadata_dao(self) -> MetadataGridFSDaoPort:
        """Return a MetadataDaoPort instance"""

        def serialize(metadata: StudyMetadata) -> bytes:
            return bytes(metadata.model_dump_json(), encoding="utf-8")

        def deserialize(serialized_data: bytes) -> StudyMetadata:
            return StudyMetadata(**json.loads(serialized_data.decode()))

        return GridFSDao(
            grid_fs=self._grid_fs,
            prefix="metadata__",
            file_extension="",
            serialize_fn=serialize,
            deserialize_fn=deserialize,
        )

    def get_workbook_dao(self) -> WorkbookGridFSDaoPort:
        """Return a WorkbookDaoPort instance"""

        def serialize(workbook: Workbook) -> bytes:
            """Convert the workbook to bytes"""
            workbook_bytestream = BytesIO()
            workbook.save(workbook_bytestream)
            workbook_bytestream.seek(0)
            return workbook_bytestream.read()

        # Workbook data is returned as bytes - use default deserializer (return as-is)
        return GridFSDao(
            grid_fs=self._grid_fs,
            prefix="workbook__",
            file_extension=".xlsx",
            serialize_fn=serialize,
            deserialize_fn=lambda data: data,
        )
