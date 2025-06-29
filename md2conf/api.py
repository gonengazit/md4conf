"""
Publish Markdown files to Confluence wiki.

Copyright 2022-2025, Levente Hunyadi

:see: https://github.com/hunyadi/md2conf
"""

import datetime
import enum
import io
import logging
import mimetypes
import typing
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Optional, TypeVar
from urllib.parse import urlencode, urlparse, urlunparse

import requests
from strong_typing.core import JsonType
from strong_typing.serialization import DeserializerOptions, json_dump_string, json_to_object, object_to_json

from .converter import ParseError, sanitize_confluence
from .metadata import ConfluenceSiteMetadata
from .properties import ArgumentError, ConfluenceConnectionProperties, ConfluenceError, PageError

T = TypeVar("T")


def _json_to_object(
    typ: type[T],
    data: JsonType,
) -> T:
    return json_to_object(typ, data, options=DeserializerOptions(skip_unassigned=True))


def build_url(base_url: str, query: Optional[dict[str, str]] = None) -> str:
    "Builds a URL with scheme, host, port, path and query string parameters."

    scheme, netloc, path, params, query_str, fragment = urlparse(base_url)

    if params:
        raise ValueError("expected: url with no parameters")
    if query_str:
        raise ValueError("expected: url with no query string")
    if fragment:
        raise ValueError("expected: url with no fragment")

    url_parts = (scheme, netloc, path, None, urlencode(query) if query else None, None)
    return urlunparse(url_parts)


LOGGER = logging.getLogger(__name__)


@enum.unique
class ConfluencePageContentType(enum.Enum):
    """
    Content types that a Confluece page can be.
    """

    PAGE = "page"
    WHITEBOARD = "whiteboard"
    DATABASE = "database"
    EMBED = "embed"
    FOLDER = "folder"


@enum.unique
class ConfluenceRepresentation(enum.Enum):
    STORAGE = "storage"
    ATLAS = "atlas_doc_format"
    WIKI = "wiki"


@enum.unique
class ConfluenceStatus(enum.Enum):
    CURRENT = "current"
    DRAFT = "draft"


@enum.unique
class ConfluenceLegacyType(enum.Enum):
    ATTACHMENT = "attachment"


@dataclass(frozen=True)
class ConfluenceLinks:
    next: str
    base: str


@dataclass(frozen=True)
class ConfluenceResultSet:
    results: list[JsonType]
    _links: ConfluenceLinks


@dataclass(frozen=True)
class ConfluenceContentVersion:
    number: int
    minorEdit: bool = False
    createdAt: Optional[datetime.datetime] = None
    message: Optional[str] = None
    authorId: Optional[str] = None


@dataclass(frozen=True)
class ConfluenceAttachment:
    """
    Holds data for an object uploaded to Confluence as a page attachment.

    :param id: Unique ID for the attachment.
    :param status: Attachment status.
    :param title: Attachment title.
    :param mediaType: MIME type for the attachment.
    :param comment: Description for the attachment.
    :param fileId: File ID of the attachment, distinct from the attachment ID.
    :param fileSize: Size in bytes.
    :param webui: WebUI link of the attachment.
    :param download: Download link of the attachment.
    :param version: Version information for the attachment.
    """

    id: str
    status: ConfluenceStatus
    title: Optional[str]
    mediaType: str
    comment: Optional[str]
    fileSize: int
    webui: str
    download: str
    version: ConfluenceContentVersion


@dataclass(frozen=True)
class ConfluenceSpace:
    key: str
    name: str


@dataclass(frozen=True)
class ConfluencePageProperties:
    """
    Holds Confluence page properties used for page synchronization.

    :param id: Confluence page ID.
    :param status: Page status.
    :param title: Page title.
    :param space: Confluence space.
    :param version: Page version. Incremented when the page is updated.
    """

    id: str
    type: ConfluencePageContentType
    status: ConfluenceStatus
    title: str
    space: ConfluenceSpace
    version: ConfluenceContentVersion


@dataclass(frozen=True)
class ConfluencePageStorage:
    """
    Holds Confluence page content.

    :param representation: Type of content representation used (e.g. Confluence Storage Format).
    :param value: Body of the content, in the format found in the representation field.
    """

    representation: ConfluenceRepresentation
    value: str


@dataclass(frozen=True)
class ConfluencePageBody:
    """
    Holds Confluence page content.

    :param storage: Encapsulates content with meta-information about its representation.
    """

    storage: ConfluencePageStorage


@dataclass(frozen=True)
class ConfluencePage(ConfluencePageProperties):
    """
    Holds Confluence page data used for page synchronization.

    :param body: Page content.
    """

    body: ConfluencePageBody

    @property
    def content(self) -> str:
        return self.body.storage.value


@dataclass(frozen=True, eq=True, order=True)
class ConfluenceLabel:
    """
    Holds information about a single label.

    :param name: Name of the label.
    :param prefix: Prefix of the label.
    """

    name: str
    prefix: str


@dataclass(frozen=True, eq=True, order=True)
class ConfluenceIdentifiedLabel(ConfluenceLabel):
    """
    Holds information about a single label.

    :param id: ID of the label.
    """

    id: str


@dataclass(frozen=True)
class ConfluenceAncestor:
    type: ConfluencePageContentType
    id: str
    title: Optional[str] = None


@dataclass(frozen=True)
class ConfluenceCreatePageRequest:
    type: ConfluencePageContentType
    space: ConfluenceSpace
    title: Optional[str]
    ancestors: list[ConfluenceAncestor]
    body: ConfluencePageBody


@dataclass(frozen=True)
class ConfluenceUpdatePageRequest:
    id: str
    type: ConfluencePageContentType
    title: str
    body: ConfluencePageBody
    version: ConfluenceContentVersion


@dataclass(frozen=True)
class ConfluenceUpdateAttachmentRequest:
    id: str
    type: ConfluenceLegacyType
    status: ConfluenceStatus
    title: str
    version: ConfluenceContentVersion


class ConfluenceAPI:
    """
    Represents an active connection to a Confluence server.
    """

    properties: ConfluenceConnectionProperties
    session: Optional["ConfluenceSession"] = None

    def __init__(self, properties: Optional[ConfluenceConnectionProperties] = None) -> None:
        self.properties = properties or ConfluenceConnectionProperties()

    def __enter__(self) -> "ConfluenceSession":
        session = requests.Session()
        if self.properties.user_name:
            session.auth = (self.properties.user_name, self.properties.api_key)
        else:
            session.headers.update({"Authorization": f"Bearer {self.properties.api_key}"})

        if self.properties.headers:
            session.headers.update(self.properties.headers)

        self.session = ConfluenceSession(
            session,
            api_url=self.properties.api_url,
            domain=self.properties.domain,
            base_path=self.properties.base_path,
            space_key=self.properties.space_key,
        )
        return self.session

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.session is not None:
            self.session.close()
            self.session = None


class ConfluenceSession:
    """
    Information about an open session to a Confluence server.
    """

    session: requests.Session
    api_url: str
    site: ConfluenceSiteMetadata

    _space_id_to_key: dict[str, str]
    _space_key_to_id: dict[str, str]

    def __init__(
        self,
        session: requests.Session,
        *,
        api_url: Optional[str],
        domain: Optional[str],
        base_path: Optional[str],
        space_key: Optional[str],
    ) -> None:
        self.session = session

        if api_url:
            self.api_url = api_url

            if not domain or not base_path:
                payload = self._invoke("/space", {"limit": "1"})
                print(payload)
                data = json_to_object(ConfluenceResultSet, payload)
                base_url = data._links.base

                _, domain, base_path, _, _, _ = urlparse(base_url)
                if not base_path.endswith("/"):
                    base_path = f"{base_path}/"

        if not domain:
            raise ArgumentError("Confluence domain not specified and cannot be inferred")
        if not base_path:
            raise ArgumentError("Confluence base path not specified and cannot be inferred")
        self.site = ConfluenceSiteMetadata(domain, base_path, space_key)
        if not api_url:
            self.api_url = f"http://{self.site.domain}{self.site.base_path}"

    def close(self) -> None:
        self.session.close()
        self.session = requests.Session()

    def _build_url(
        self,
        path: str,
        query: Optional[dict[str, str]] = None,
    ) -> str:
        """
        Builds a full URL for invoking the Confluence API.

        :param prefix: A URL path prefix that depends on the Confluence API version.
        :param path: Path of API endpoint to invoke.
        :param query: Query parameters to pass to the API endpoint.
        :returns: A full URL.
        """

        base_url = f"{self.api_url}rest/api{path}"
        return build_url(base_url, query)

    def _invoke(
        self,
        path: str,
        query: Optional[dict[str, str]] = None,
    ) -> JsonType:
        "Executes an HTTP request via Confluence API."

        url = self._build_url(path, query)
        response = self.session.get(url, headers={"Accept": "application/json"})
        if response.text:
            LOGGER.debug("Received HTTP payload:\n%s", response.text)
        response.raise_for_status()
        return typing.cast(JsonType, response.json())

    def _fetch(self, path: str, query: Optional[dict[str, str]] = None) -> list[JsonType]:
        "Retrieves all results of a REST API paginated result-set."

        items: list[JsonType] = []
        url = self._build_url(path, query)
        while True:
            response = self.session.get(url, headers={"Accept": "application/json"})
            response.raise_for_status()

            payload = typing.cast(dict[str, JsonType], response.json())
            results = typing.cast(list[JsonType], payload["results"])
            items.extend(results)

            links = typing.cast(dict[str, JsonType], payload.get("_links", {}))
            link = typing.cast(str, links.get("next", ""))
            if link:
                url = f"https://{self.site.domain}{link}"
            else:
                break

        return items

    def _save(self, path: str, data: JsonType) -> None:
        "Persists data via Confluence REST API."

        url = self._build_url(path)
        response = self.session.put(
            url,
            data=json_dump_string(data),
            headers={"Content-Type": "application/json"},
        )
        if response.text:
            LOGGER.debug("Received HTTP payload:\n%s", response.text)
        response.raise_for_status()

    def get_attachment_by_name(self, page_id: str, filename: str) -> ConfluenceAttachment:
        """
        Retrieves a Confluence page attachment by an unprefixed file name.
        """

        path = f"/content/{page_id}/child/attachment"
        query = {"filename": filename, "expand": "version"}
        payload = self._invoke(path, query)
        data = typing.cast(dict[str, JsonType], payload)

        results = typing.cast(list[JsonType], data["results"])
        if len(results) != 1:
            raise ConfluenceError(f"no such attachment on page {page_id}: {filename}")
        result = typing.cast(dict[str, JsonType], results[0])
        result["mediaType"] = result["extensions"]["mediaType"]
        result["fileSize"] = result["extensions"]["fileSize"]
        result["webui"] = result["_links"]["webui"]
        result["download"] = result["_links"]["download"]
        return _json_to_object(ConfluenceAttachment, result)

    def upload_attachment(
        self,
        page_id: str,
        attachment_name: str,
        *,
        attachment_path: Optional[Path] = None,
        raw_data: Optional[bytes] = None,
        content_type: Optional[str] = None,
        comment: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """
        Uploads a new attachment to a Confluence page.

        :param page_id: Confluence page ID.
        :param attachment_name: Unprefixed name unique to the page.
        :param attachment_path: Path to the file to upload as an attachment.
        :param raw_data: Raw data to upload as an attachment.
        :param content_type: Attachment MIME type.
        :param comment: Attachment description.
        :param force: Overwrite an existing attachment even if there seem to be no changes.
        """

        if attachment_path is None and raw_data is None:
            raise ArgumentError("required: `attachment_path` or `raw_data`")

        if attachment_path is not None and raw_data is not None:
            raise ArgumentError("expected: either `attachment_path` or `raw_data`")

        if content_type is None:
            if attachment_path is not None:
                name = str(attachment_path)
            else:
                name = attachment_name
            content_type, _ = mimetypes.guess_type(name, strict=True)

            if content_type is None:
                content_type = "application/octet-stream"

        if attachment_path is not None and not attachment_path.is_file():
            raise PageError(f"file not found: {attachment_path}")

        try:
            attachment = self.get_attachment_by_name(page_id, attachment_name)

            if attachment_path is not None:
                if not force and attachment.fileSize == attachment_path.stat().st_size:
                    LOGGER.info("Up-to-date attachment: %s", attachment_name)
                    return
            elif raw_data is not None:
                if not force and attachment.fileSize == len(raw_data):
                    LOGGER.info("Up-to-date embedded image: %s", attachment_name)
                    return
            else:
                raise NotImplementedError("parameter match not exhaustive")

            id = attachment.id.removeprefix("att")
            path = f"/content/{page_id}/child/attachment/{id}/data"

        except ConfluenceError:
            path = f"/content/{page_id}/child/attachment"

        url = self._build_url(path)

        if attachment_path is not None:
            with open(attachment_path, "rb") as attachment_file:
                file_to_upload: dict[str, tuple[Optional[str], Any, str, dict[str, str]]] = {
                    "comment": (
                        None,
                        comment,
                        "text/plain; charset=utf-8",
                        {},
                    ),
                    "file": (
                        attachment_name,  # will truncate path component
                        attachment_file,
                        content_type,
                        {"Expires": "0"},
                    ),
                }
                LOGGER.info("Uploading attachment: %s", attachment_name)
                response = self.session.post(
                    url,
                    files=file_to_upload,
                    headers={
                        "X-Atlassian-Token": "no-check",
                        "Accept": "application/json",
                    },
                )
        elif raw_data is not None:
            LOGGER.info("Uploading raw data: %s", attachment_name)

            raw_file = io.BytesIO(raw_data)
            raw_file.name = attachment_name
            file_to_upload = {
                "comment": (
                    None,
                    comment,
                    "text/plain; charset=utf-8",
                    {},
                ),
                "file": (
                    attachment_name,  # will truncate path component
                    raw_file,
                    content_type,
                    {"Expires": "0"},
                ),
            }
            response = self.session.post(
                url,
                files=file_to_upload,
                headers={
                    "X-Atlassian-Token": "no-check",
                    "Accept": "application/json",
                },
            )
        else:
            raise NotImplementedError("parameter match not exhaustive")

        response.raise_for_status()
        data = response.json()

        if "results" in data:
            result = data["results"][0]
        else:
            result = data

        attachment_id = result["id"]
        version = result["version"]["number"] + 1

        # ensure path component is retained in attachment name
        self._update_attachment(page_id, attachment_id, version, attachment_name)

    def _update_attachment(self, page_id: str, attachment_id: str, version: int, attachment_title: str) -> None:
        id = attachment_id.removeprefix("att")
        path = f"/content/{page_id}/child/attachment/{id}"
        request = ConfluenceUpdateAttachmentRequest(
            id=attachment_id,
            type=ConfluenceLegacyType.ATTACHMENT,
            status=ConfluenceStatus.CURRENT,
            title=attachment_title,
            version=ConfluenceContentVersion(number=version, minorEdit=True),
        )

        LOGGER.info("Updating attachment: %s", attachment_id)
        self._save(path, object_to_json(request))

    def get_page_ancestors(self, page_id: str) -> list[ConfluenceAncestor]:
        """
        Retrieve Confluence wiki page ancestors.

        :param page_id: The Confluence page ID.
        :param space_key: The Confluence space key (unless the default space is to be used).
        :returns: list of ancestors - topmost ancestor first
        """

        path = f"/content/{page_id}"
        query = {
            "expand": "ancestors",
        }
        data = typing.cast(dict[str, JsonType], self._invoke(path, query))
        ancestors = _json_to_object(list[ConfluenceAncestor], data["ancestors"])
        return ancestors

    def get_page_properties_by_title(
        self,
        title: str,
        *,
        space_key: Optional[str] = None,
    ) -> ConfluencePageProperties:
        """
        Looks up a Confluence wiki page ID by title.

        :param title: The page title.
        :param space_key: The Confluence space key (unless the default space is to be used).
        :returns: Confluence page ID.
        """

        LOGGER.info("Looking up page with title: %s", title)
        path = "/content"
        query = {"title": title, "expand": "space,version"}
        if space_key is not None:
            query["spaceKey"] = space_key

        payload = self._invoke(path, query)
        data = typing.cast(dict[str, JsonType], payload)
        results = typing.cast(list[JsonType], data["results"])
        if len(results) != 1:
            raise ConfluenceError(f"unique page not found with title: {title}")
        print(results[0])

        page = _json_to_object(ConfluencePageProperties, results[0])
        return page

    def get_page(self, page_id: str, space_key: Optional[str] = None) -> ConfluencePage:
        """
        Retrieves Confluence wiki page details and content.

        :param page_id: The Confluence page ID.
        :returns: Confluence page info and content.
        """

        path = f"/content/{page_id}"
        query = {
            "expand": "body.storage,version,space",
        }
        if space_key is not None:
            query["spaceKey"] = space_key

        payload = self._invoke(path, query)
        return _json_to_object(ConfluencePage, payload)

    def get_page_properties(self, page_id: str) -> ConfluencePageProperties:
        """
        Retrieves Confluence wiki page details.

        :param page_id: The Confluence page ID.
        :returns: Confluence page info.
        """

        path = f"/content/{page_id}"
        payload = self._invoke(path)
        return _json_to_object(ConfluencePageProperties, payload)

    def get_page_version(self, page_id: str) -> int:
        """
        Retrieves a Confluence wiki page version.

        :param page_id: The Confluence page ID.
        :returns: Confluence page version.
        """

        return self.get_page_properties(page_id).version.number

    def update_page(
        self,
        page_id: str,
        new_content: str,
        *,
        title: Optional[str] = None,
    ) -> None:
        """
        Updates a page via the Confluence API.

        :param page_id: The Confluence page ID.
        :param new_content: Confluence Storage Format XHTML.
        :param title: New title to assign to the page. Needs to be unique within a space.
        """

        page = self.get_page(page_id)
        new_title = title or page.title

        try:
            old_content = sanitize_confluence(page.content)
            if page.title == new_title and old_content == new_content:
                LOGGER.info("Up-to-date page: %s", page_id)
                return
        except ParseError as exc:
            LOGGER.warning(exc)

        path = f"/content/{page_id}"
        request = ConfluenceUpdatePageRequest(
            id=page_id,
            type=ConfluencePageContentType.PAGE,
            title=new_title,
            body=ConfluencePageBody(storage=ConfluencePageStorage(representation=ConfluenceRepresentation.STORAGE, value=new_content)),
            version=ConfluenceContentVersion(number=page.version.number + 1, minorEdit=True),
        )
        LOGGER.info("Updating page: %s", page_id)
        self._save(path, object_to_json(request))

    def create_page(
        self,
        parent_id: str,
        title: str,
        new_content: str,
    ) -> ConfluencePage:
        """
        Creates a new page via Confluence API.
        """

        LOGGER.info("Creating page: %s", title)

        parent_page = self.get_page_properties(parent_id)

        path = "/content/"
        request = ConfluenceCreatePageRequest(
            type=ConfluencePageContentType.PAGE,
            space=parent_page.space,
            title=title,
            body=ConfluencePageBody(
                storage=ConfluencePageStorage(
                    representation=ConfluenceRepresentation.STORAGE,
                    value=new_content,
                )
            ),
            ancestors=[ConfluenceAncestor(type=ConfluencePageContentType.PAGE, id=parent_page.id)],
        )

        url = self._build_url(path)
        response = self.session.post(
            url,
            data=json_dump_string(object_to_json(request)),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        response.raise_for_status()
        return _json_to_object(ConfluencePage, response.json())

    def delete_page(self, page_id: str, *, purge: bool = False) -> None:
        """
        Deletes a page via Confluence API.

        :param page_id: The Confluence page ID.
        :param purge: `True` to completely purge the page, `False` to move to trash only.
        """

        path = f"/content/{page_id}"

        # move to trash
        url = self._build_url(path)
        LOGGER.info("Moving page to trash: %s", page_id)
        response = self.session.delete(url)
        response.raise_for_status()

        if purge:
            # purge from trash
            query = {"status": "trashed"}
            url = self._build_url(path, query)
            LOGGER.info("Permanently deleting page: %s", page_id)
            response = self.session.delete(url)
            response.raise_for_status()

    def page_exists(
        self,
        title: str,
        space_key: str,
    ) -> Optional[str]:
        """
        Checks if a Confluence page exists with the given title.

        :param title: Page title. Pages in the same Confluence space must have a unique title.
        :param space_key: Identifies the Confluence space.

        :returns: Confluence page ID of a matching page (if found), or `None`.
        """

        path = "/content"
        query = {"title": title, "spaceKey": space_key}

        LOGGER.info("Checking if page exists with title: %s", title)

        url = self._build_url(path)
        response = self.session.get(
            url,
            params=query,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        response.raise_for_status()
        data = typing.cast(dict[str, JsonType], response.json())
        results = _json_to_object(list[ConfluencePageProperties], data["results"])

        if len(results) == 1:
            return results[0].id
        else:
            return None

    def get_or_create_page(self, title: str, parent_id: str) -> ConfluencePage:
        """
        Finds a page with the given title, or creates a new page if no such page exists.

        :param title: Page title. Pages in the same Confluence space must have a unique title.
        :param parent_id: Identifies the parent page for a new child page.
        """

        parent_page = self.get_page_properties(parent_id)
        page_id = self.page_exists(title, space_key=parent_page.space.key)

        if page_id is not None:
            LOGGER.debug("Retrieving existing page: %s", page_id)
            return self.get_page(page_id)
        else:
            LOGGER.debug("Creating new page with title: %s", title)
            return self.create_page(parent_id, title, "")

    def get_labels(self, page_id: str) -> list[ConfluenceIdentifiedLabel]:
        """
        Retrieves labels for a Confluence page.

        :param page_id: The Confluence page ID.
        :returns: A list of page labels.
        """

        path = f"/content/{page_id}/label"
        results = self._fetch(path)
        return _json_to_object(list[ConfluenceIdentifiedLabel], results)

    def add_labels(self, page_id: str, labels: list[ConfluenceLabel]) -> None:
        """
        Adds labels to a Confluence page.

        :param page_id: The Confluence page ID.
        :param labels: A list of page labels to add.
        """

        path = f"/content/{page_id}/label"

        url = self._build_url(path)
        response = self.session.post(
            url,
            data=json_dump_string(object_to_json(labels)),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        if response.text:
            LOGGER.debug("Received HTTP payload:\n%s", response.text)
        response.raise_for_status()

    def remove_labels(self, page_id: str, labels: list[ConfluenceLabel]) -> None:
        """
        Removes labels from a Confluence page.

        :param page_id: The Confluence page ID.
        :param labels: A list of page labels to remove.
        """

        path = f"/content/{page_id}/label"
        for label in labels:
            query = {"name": label.name}

            url = self._build_url(path, query)
            response = self.session.delete(url)
            if response.text:
                LOGGER.debug("Received HTTP payload:\n%s", response.text)
            response.raise_for_status()

    def update_labels(self, page_id: str, labels: list[ConfluenceLabel]) -> None:
        """
        Assigns the specified labels to a Confluence page. Existing labels are removed.

        :param page_id: The Confluence page ID.
        :param labels: A list of page labels to assign.
        """

        new_labels = set(labels)
        old_labels = set(ConfluenceLabel(name=label.name, prefix=label.prefix) for label in self.get_labels(page_id))

        add_labels = list(new_labels - old_labels)
        remove_labels = list(old_labels - new_labels)

        if add_labels:
            add_labels.sort()
            self.add_labels(page_id, add_labels)
        if remove_labels:
            remove_labels.sort()
            self.remove_labels(page_id, remove_labels)
