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

"""Database migration logic for the WPS."""

from contextlib import suppress

from gridfs import AsyncGridFS, AsyncGridFSBucket
from hexkit.providers.mongodb.migrations import MigrationDefinition, Reversible

from rts.adapters.outbound.dao import GridFSDaoFactory
from rts.models import StudyMetadata
from rts.ports.outbound.dao import ResourceNotFoundError

METADATA = "metadata"
WORKBOOK_PREFIX = "workbook__"


class V2Migration(MigrationDefinition, Reversible):
    """Move the contents of the study metadata collection ("metadata") into GridFS.

    This can be reversed by removing the data from GridFS and storing it back into a
    collection with the original name ("metadata").
    """

    version = 2

    async def apply(self):
        """Perform the migration."""
        # Prefix all existing workbook files in grid fs with 'workbook__'
        grid_fs = AsyncGridFS(self._db)
        grid_fs_bucket = AsyncGridFSBucket(self._db)
        for filename in await grid_fs.list():
            if not filename.startswith(WORKBOOK_PREFIX):
                await grid_fs_bucket.rename_by_name(
                    filename, f"{WORKBOOK_PREFIX}{filename}"
                )

        # Iterate over the collection and move each item into GridFS
        collection = self._db[METADATA]
        gridfs_dao_factory = GridFSDaoFactory(grid_fs=grid_fs)
        gridfs_dao = gridfs_dao_factory.get_metadata_dao()
        async for doc in collection.find():
            doc["study_accession"] = doc.pop("_id")
            metadata = StudyMetadata(**doc)
            accession = metadata.study_accession

            # We don't want to accidentally overwrite something in this process,
            #  so be careful and double check there's no name collision:
            prefixed_name = f"metadata__{accession}"
            with suppress(ResourceNotFoundError):
                existing = await gridfs_dao.find(id_=accession)
                if existing:
                    raise RuntimeError(f"Unexpected name collision for {prefixed_name}")
            await gridfs_dao.upsert(data=metadata, id_=accession)
        await collection.drop()

    async def unapply(self):
        """Reverse the migration"""
        grid_fs = AsyncGridFS(self._db)
        collection = self._db[METADATA]
        gridfs_dao_factory = GridFSDaoFactory(grid_fs=grid_fs)
        gridfs_dao = gridfs_dao_factory.get_metadata_dao()
        async for metadata in gridfs_dao.find_all():
            doc = metadata.model_dump()
            doc["_id"] = doc.pop("study_accession")
            await collection.insert_one(doc)
            await gridfs_dao.delete(id_=doc["_id"])

        # Remove 'workbook__' prefix from workbooks
        grid_fs_bucket = AsyncGridFSBucket(self._db)
        for filename in await grid_fs.list():
            if filename.startswith(WORKBOOK_PREFIX):
                modified_name = filename.removeprefix(WORKBOOK_PREFIX)
                await grid_fs_bucket.rename_by_name(filename, modified_name)
