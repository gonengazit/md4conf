"""
Publish Markdown files to Confluence wiki.

Copyright 2022-2025, Levente Hunyadi

:see: https://github.com/hunyadi/md2conf
"""

# mypy: disable-error-code="dict-item"

import hashlib
import importlib.resources as resources
import logging
import os.path
import re
import uuid
import xml.etree.ElementTree
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional, Union
from urllib.parse import ParseResult, quote_plus, urlparse, urlunparse

import lxml.etree as ET
import markdown
from lxml.builder import ElementMaker
from bs4 import BeautifulSoup, CData, Tag
from bs4.element import NavigableString

from .collection import ConfluencePageCollection
from .mermaid import render_diagram
from .metadata import ConfluenceSiteMetadata
from .properties import PageError
from .scanner import ScannedDocument, Scanner

namespaces = {
    "ac": "http://atlassian.com/content",
    "ri": "http://atlassian.com/resource/identifier",
}
for key, value in namespaces.items():
    ET.register_namespace(key, value)

HTML = ElementMaker()
AC = ElementMaker(namespace=namespaces["ac"])
RI = ElementMaker(namespace=namespaces["ri"])

LOGGER = logging.getLogger(__name__)


class ParseError(RuntimeError):
    pass


def starts_with_any(text: str, prefixes: list[str]) -> bool:
    "True if text starts with any of the listed prefixes."

    for prefix in prefixes:
        if text.startswith(prefix):
            return True
    return False


def is_absolute_url(url: str) -> bool:
    urlparts = urlparse(url)
    return bool(urlparts.scheme) or bool(urlparts.netloc)


def is_relative_url(url: str) -> bool:
    urlparts = urlparse(url)
    return not bool(urlparts.scheme) and not bool(urlparts.netloc)


def encode_title(text: str) -> str:
    "Converts a title string such that it is safe to embed into a Confluence URL."

    # replace unsafe characters with space
    text = re.sub(r"[^A-Za-z0-9._~()'!*:@,;+?-]+", " ", text)

    # replace multiple consecutive spaces with single space
    text = re.sub(r"\s\s+", " ", text)

    # URL-encode
    return quote_plus(text.strip())


def emoji_generator(
    index: str,
    shortname: str,
    alias: Optional[str],
    uc: Optional[str],
    alt: str,
    title: Optional[str],
    category: Optional[str],
    options: dict[str, Any],
    md: markdown.Markdown,
) -> xml.etree.ElementTree.Element:
    name = (alias or shortname).strip(":")
    span = xml.etree.ElementTree.Element("span", {"data-emoji-shortname": name})
    if uc is not None:
        span.attrib["data-emoji-unicode"] = uc

        # convert series of Unicode code point hexadecimal values into characters
        span.text = "".join(chr(int(item, base=16)) for item in uc.split("-"))
    else:
        span.text = alt
    return span


def markdown_to_html(content: str) -> str:
    return markdown.markdown(
        content,
        extensions=[
            "admonition",
            "markdown.extensions.tables",
            # "markdown.extensions.fenced_code",
            "pymdownx.emoji",
            "pymdownx.highlight",  # required by `pymdownx.superfences`
            "pymdownx.magiclink",
            "pymdownx.superfences",
            "pymdownx.tilde",
            "sane_lists",
            "md_in_html",
        ],
        extension_configs={
            "pymdownx.emoji": {
                "emoji_generator": emoji_generator,
            },
            "pymdownx.highlight": {
                "use_pygments": False,
            },
        },
    )


def _elements_from_strings(dtd_path: Path, items: list[str]) -> ET._Element:
    """
    Creates a fragment of several XML nodes from their string representation wrapped in a root element.

    :param dtd_path: Path to a DTD document that defines entities like &cent; or &copy;.
    :param items: Strings to parse into XML fragments.
    :returns: An XML document as an element tree.
    """

    parser = ET.XMLParser(
        remove_blank_text=True,
        remove_comments=True,
        strip_cdata=False,
        load_dtd=True,
    )

    ns_attr_list = "".join(
        f' xmlns:{key}="{value}"' for key, value in namespaces.items()
    )

    data = [
        '<?xml version="1.0"?>',
        f'<!DOCTYPE ac:confluence PUBLIC "-//Atlassian//Confluence 4 Page//EN" "{dtd_path.as_posix()}">'
        f"<root{ns_attr_list}>",
    ]
    data.extend(items)
    data.append("</root>")

    try:
        return ET.fromstringlist(data, parser=parser)
    except ET.XMLSyntaxError as ex:
        raise ParseError() from ex


def elements_from_strings(items: list[str]) -> ET._Element:
    "Creates a fragment of several XML nodes from their string representation wrapped in a root element."

    resource_path = resources.files(__package__).joinpath("entities.dtd")
    with resources.as_file(resource_path) as dtd_path:
        return _elements_from_strings(dtd_path, items)


def elements_from_string(content: str) -> ET._Element:
    return elements_from_strings([content])


_languages = [
    "abap",
    "actionscript3",
    "ada",
    "applescript",
    "arduino",
    "autoit",
    "bash",
    "c",
    "clojure",
    "coffeescript",
    "coldfusion",
    "cpp",
    "csharp",
    "css",
    "cuda",
    "d",
    "dart",
    "delphi",
    "diff",
    "elixir",
    "erlang",
    "fortran",
    "foxpro",
    "go",
    "graphql",
    "groovy",
    "haskell",
    "haxe",
    "html",
    "java",
    "javafx",
    "javascript",
    "json",
    "jsx",
    "julia",
    "kotlin",
    "livescript",
    "lua",
    "mermaid",
    "mathematica",
    "matlab",
    "objectivec",
    "objectivej",
    "ocaml",
    "octave",
    "pascal",
    "perl",
    "php",
    "powershell",
    "prolog",
    "puppet",
    "python",
    "qml",
    "r",
    "racket",
    "rst",
    "ruby",
    "rust",
    "sass",
    "scala",
    "scheme",
    "shell",
    "smalltalk",
    "splunk",
    "sql",
    "standardml",
    "swift",
    "tcl",
    "tex",
    "tsx",
    "typescript",
    "vala",
    "vb",
    "verilog",
    "vhdl",
    "xml",
    "xquery",
    "yaml",
]


class NodeVisitor:
    def visit(self, node: ET._Element) -> None:
        "Recursively visits all descendants of this node."

        if len(node) < 1:
            return

        for index in range(len(node)):
            source = node[index]
            target = self.transform(source)
            if target is not None:
                node[index] = target
            else:
                self.visit(source)

    def transform(self, child: ET._Element) -> Optional[ET._Element]:
        pass


def title_to_identifier(title: str) -> str:
    "Converts a section heading title to a GitHub-style Markdown same-page anchor."

    s = title.strip().lower()
    s = re.sub("[^ A-Za-z0-9]", "", s)
    s = s.replace(" ", "-")
    return s


def element_to_text(element: Tag) -> str:
    """Gets all text from a BeautifulSoup tag."""
    return element.get_text(strip=True)


@dataclass
class TableOfContentsEntry:
    level: int
    text: str


class TableOfContents:
    "Builds a table of contents from Markdown headings."

    headings: list[TableOfContentsEntry]

    def __init__(self) -> None:
        self.headings = []

    def add(self, level: int, text: str) -> None:
        """
        Adds a heading to the table of contents.

        :param level: Markdown heading level (e.g. `1` for first-level heading).
        :param text: Markdown heading text.
        """

        self.headings.append(TableOfContentsEntry(level, text))

    def get_title(self) -> Optional[str]:
        """
        Returns a proposed document title (if unique).

        :returns: Title text, or `None` if no unique title can be inferred.
        """

        for level in range(1, 7):
            try:
                (title,) = (item.text for item in self.headings if item.level == level)
                return title
            except ValueError:
                pass

        return None


@dataclass
class ConfluenceConverterOptions:
    """
    Options for converting an HTML tree into Confluence storage format.

    :param ignore_invalid_url: When true, ignore invalid URLs in input, emit a warning and replace the anchor with
        plain text; when false, raise an exception.
    :param heading_anchors: When true, emit a structured macro *anchor* for each section heading using GitHub
        conversion rules for the identifier.
    :param render_mermaid: Whether to pre-render Mermaid diagrams into PNG/SVG images.
    :param diagram_output_format: Target image format for diagrams.
    :param webui_links: When true, convert relative URLs to Confluence Web UI links.
    """

    ignore_invalid_url: bool = False
    heading_anchors: bool = False
    render_mermaid: bool = False
    diagram_output_format: Literal["png", "svg"] = "png"
    webui_links: bool = False


class ConfluenceStorageFormatConverter:
    """
    Transforms a plain HTML string into the Confluence storage format
    using BeautifulSoup.
    """

    def __init__(
        self,
        options: ConfluenceConverterOptions,
        path: Path,
        root_dir: Path,
        site_metadata: ConfluenceSiteMetadata,
        page_metadata: ConfluencePageCollection,
    ):
        self.options = options
        self.path = path
        self.base_dir = path.parent
        self.root_dir = root_dir
        self.site_metadata = site_metadata
        self.page_metadata = page_metadata

        # Conversion artifacts
        self.toc = TableOfContents()
        self.links: list[str] = []
        self.images: list[Path] = []
        self.embedded_images: dict[str, bytes] = {}

    def convert(self, html_content: str) -> str:
        """
        Main conversion function. Parses HTML and applies all transformations.

        Args:
            html_content: A string containing the HTML to convert.

        Returns:
            A string with the converted Confluence Storage Format XHTML.
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # The order of transformations can be important.
        self._transform_paragraph_images(soup)
        self._transform_headings(soup)
        self._transform_links(soup)
        self._transform_images(soup)
        self._transform_code_blocks(soup)
        self._transform_toc(soup)
        self._transform_admonitions(soup)
        self._transform_alerts(soup)
        self._transform_sections(soup)
        self._transform_emojis(soup)
        self._transform_paragraphs(soup)

        return str(soup)

    def _create_macro(self, soup: BeautifulSoup, name: str, params: Optional[dict[str, str]] = None, plain_text_body: Optional[str] = None) -> Tag:
        """Helper to create a generic Confluence <ac:structured-macro>."""
        macro = soup.new_tag("ac:structured-macro")
        macro['ac:name'] = name
        macro['ac:schema-version'] = '1'

        if params:
            for key, value in params.items():
                param_tag = soup.new_tag("ac:parameter")
                param_tag['ac:name'] = key
                param_tag.string = value
                macro.append(param_tag)

        if plain_text_body is not None:
            body_tag = soup.new_tag(f"ac:plain-text-body")
            body_tag.append(CData(plain_text_body))
            macro.append(body_tag)

        return macro

    def _transform_headings(self, soup: BeautifulSoup):
        """Adds heading anchors and populates the Table of Contents."""
        heading_tags = soup.find_all(re.compile(r"^h[1-6]$", re.IGNORECASE))
        for heading in heading_tags:
            level = int(heading.name[1])
            title = element_to_text(heading)
            self.toc.add(level, title)

            if self.options.heading_anchors:
                anchor_id = title_to_identifier(title)
                anchor_macro = self._create_macro(soup, "anchor", {"": anchor_id})
                heading.insert(0, anchor_macro)

    def _transform_links(self, soup: BeautifulSoup):
        """Converts relative page links to Confluence web links."""
        for anchor in soup.find_all("a", href=True):
            url = anchor['href']
            if is_absolute_url(url):
                continue

            LOGGER.debug("Found link %s relative to %s", url, self.path)
            relative_url = urlparse(url)

            # Handle local anchor links (e.g., href="#some-id")
            if (
                not relative_url.scheme
                and not relative_url.netloc
                and not relative_url.path
                and not relative_url.params
                and not relative_url.query
            ):
                if self.options.heading_anchors:
                    target = relative_url.fragment.lstrip("#")
                    link_wrapper = soup.new_tag("ac:link", attrs={'ac:anchor': target})
                    link_body = soup.new_tag("ac:link-body")
                    link_body.extend(anchor.contents) # Move content
                    link_wrapper.append(link_body)
                    anchor.replace_with(link_wrapper)
                continue

            # Handle links to other pages
            try:
                absolute_path = (self.base_dir / relative_url.path).resolve(strict=True)
                if not str(absolute_path).startswith(str(self.root_dir)):
                    raise DocumentError(f"Link {url} points outside project root.")

                link_metadata = self.page_metadata.get(absolute_path)
                if not link_metadata:
                    raise DocumentError(f"No page metadata found for link: {url}")

                relative_path = os.path.relpath(absolute_path, self.base_dir)
                LOGGER.debug("found link to page %s with metadata: %s", relative_path, link_metadata)

                self.links.append(url)


                if self.options.webui_links:
                    page_url = f"{self.site_metadata.base_path}pages/viewpage.action?pageId={link_metadata.page_id}"
                else:
                    space_key = link_metadata.space_key or self.site_metadata.space_key
                    if not space_key:
                        raise DocumentError("Space key is required to build page links.")
                    page_url = f"{self.site_metadata.base_path}spaces/{space_key}/pages/{link_metadata.page_id}/{encode_title(link_metadata.title)}"

                components = urlparse(page_url)
                transformed_url = urlunparse(relative_url._replace(scheme=components.scheme or 'https', netloc=components.netloc or self.site_metadata.domain, path=components.path))
                anchor['href'] = transformed_url

            except (DocumentError, FileNotFoundError) as e:
                print(f"Warning: {e}")
                if self.options.ignore_invalid_url:
                    del anchor['href'] # Make it a dead link
                else:
                    raise

    def _create_ac_image_tag(self, soup: BeautifulSoup, img_tag: Tag) -> Tag:
        """Helper to create a Confluence <ac:image> tag from an HTML <img> tag."""
        src = img_tag.get('src')
        if not src:
            raise DocumentError("Image lacks 'src' attribute.")

        ac_image = soup.new_tag("ac:image")
        for attr in ['width', 'height']:
            if img_tag.has_attr(attr):
                ac_image[f'ac:{attr}'] = img_tag[attr]

        # Add attachment or URL element
        if is_absolute_url(src):
            ri_child = soup.new_tag("ri:url", attrs={'ri:value': src})
        else:
            path = Path(src)
            # Logic to prefer PNG over SVG
            if path.suffix == ".svg":
                png_file = path.with_suffix(".png")
                if (self.base_dir / png_file).exists():
                    path = png_file

            self.images.append(path)
            image_name = attachment_name(path)
            ri_child = soup.new_tag("ri:attachment", attrs={'ri:filename': image_name})

        ac_image.append(ri_child)

        # Add caption if alt text exists
        if img_tag.has_attr('alt'):
            caption_tag = soup.new_tag("ac:caption")
            p_tag = soup.new_tag("p")
            p_tag.string = img_tag['alt']
            caption_tag.append(p_tag)
            ac_image.append(caption_tag)

        return ac_image

    def _transform_paragraph_images(self, soup: BeautifulSoup):
        """Transforms <p><img></p> into a single <ac:image> block."""
        for p_tag in soup.find_all("p"):
            # Find paragraphs containing only an image tag and whitespace
            meaningful_children = [c for c in p_tag.children if isinstance(c, Tag)]
            if len(meaningful_children) == 1 and meaningful_children[0].name == 'img':
                ac_image = self._create_ac_image_tag(soup, meaningful_children[0])
                p_tag.replace_with(ac_image)

    def _transform_images(self, soup: BeautifulSoup):
        """Transforms standalone <img> tags."""
        for img_tag in soup.find_all("img"):
            ac_image = self._create_ac_image_tag(soup, img_tag)
            img_tag.replace_with(ac_image)

    def _transform_code_blocks(self, soup: BeautifulSoup):
        """Converts <pre><code> blocks into Confluence code macros."""
        for pre_tag in soup.find_all("pre"):
            code_tag = pre_tag.find("code")
            if not code_tag:
                continue

            language = pre_tag.get("class", [""])[0]
            content = code_tag.get_text().rstrip()

            if language.lower() == "mermaid":
                # Handle mermaid diagrams
                if self.options.render_mermaid:
                    image_data = render_diagram(content, self.options.diagram_output_format)
                    image_hash = hashlib.md5(image_data).hexdigest()
                    image_filename = f"embedded_{image_hash}.{self.options.diagram_output_format}"
                    self.embedded_images[image_filename] = image_data

                    new_tag = soup.new_tag("ac:image")
                    ri_attachment = soup.new_tag("ri:attachment", attrs={'ri:filename': image_filename})
                    new_tag.append(ri_attachment)
                else:
                    # Diagram macro for live rendering in Confluence
                    new_tag = self._create_macro(soup, "macro-diagram", {"syntax": "Mermaid"}, content)
            else:
                # Standard code block macro
                params = {'language': language, 'theme': 'Default'}
                new_tag = self._create_macro(soup, "code", params, content)

            pre_tag.replace_with(new_tag)

    def _transform_toc(self, soup: BeautifulSoup):
        """Transforms a [TOC] placeholder into a Confluence TOC macro."""
        for p_tag in soup.find_all("p"):
            if p_tag.get_text(strip=True) in ["[TOC]", "[[TOC]]"]:
                toc_macro = self._create_macro(soup, "toc", {"style": "default"})
                p_tag.replace_with(toc_macro)

    def _transform_admonitions(self, soup: BeautifulSoup):
        """Transforms admonition divs into info/note/warning macros."""
        for div in soup.find_all("div", class_="admonition"):
            class_list = div.get('class', [])
            admonition_type = next((c for c in class_list if c in ["info", "tip", "note", "warning"]), None)
            if not admonition_type:
                continue

            title_p = div.find("p", class_="admonition-title")
            params = {}
            if title_p:
                params['title'] = title_p.get_text(strip=True)
                title_p.decompose() # Remove the title paragraph

            macro = self._create_macro(soup, admonition_type, params)
            # Move remaining content into the rich text body
            macro_body = soup.new_tag("ac:rich-text-body")
            macro_body.extend(div.contents)
            macro.append(macro_body)
            div.replace_with(macro)

    def _transform_alerts(self, soup: BeautifulSoup):
        """Transforms GitHub/GitLab style blockquote alerts."""
        for bq in soup.find_all("blockquote"):
            p_tag = bq.find("p")
            if not p_tag or not p_tag.text:
                continue

            text = p_tag.get_text(strip=True)
            class_name, skip = None, 0

            # GitHub: [!NOTE]
            gh_match = re.match(r"^\[!([A-Z]+)\]\s*", p_tag.text.lstrip())
            if gh_match:
                alert_map = {"NOTE": "note", "TIP": "tip", "IMPORTANT": "info", "WARNING": "warning", "CAUTION": "warning"}
                class_name = alert_map.get(gh_match.group(1))
                skip = len(gh_match.group(0))

            # GitLab: NOTE:
            gl_match = re.match(r"^(FLAG|NOTE|WARNING|DISCLAIMER):\s*", text)
            if not class_name and gl_match:
                alert_map = {"FLAG": "tip", "NOTE": "note", "WARNING": "warning", "DISCLAIMER": "info"}
                class_name = alert_map.get(gl_match.group(1))
                skip = len(gl_match.group(0))

            if class_name:
                # Remove the alert prefix from the text node
                first_text_node = p_tag.find(string=True)
                if first_text_node:
                    first_text_node.replace_with(first_text_node.string.lstrip()[skip:])

                macro = self._create_macro(soup, class_name)
                print(p_tag)
                print(bq.contents)
                # Move remaining content into the rich text body
                macro_body = soup.new_tag("ac:rich-text-body")
                macro_body.extend(bq.contents)
                macro.append(macro_body)

                bq.replace_with(macro)

    def _transform_sections(self, soup: BeautifulSoup):
        """Transforms <details><summary> sections into expand macros."""
        for details in soup.find_all("details"):
            summary = details.find("summary")
            if not summary:
                continue

            title = summary.get_text(strip=True)
            summary.decompose() # Remove summary from content

            macro = self._create_macro(soup, "expand", {"title": title})
            macro.find("ac:rich-text-body").extend(details.contents)
            details.replace_with(macro)

    def _transform_emojis(self, soup: BeautifulSoup):
        """Transforms emoji spans into emoticons."""
        for span in soup.find_all("span", attrs={'data-emoji-shortname': True}):
            shortname = span['data-emoji-shortname']
            emoticon = soup.new_tag("ac:emoticon", attrs={
                'ac:name': shortname,
                'ac:emoji-shortname': f":{shortname}:",
                'ac:emoji-id': span.get('data-emoji-unicode', ''),
                'ac:emoji-fallback': span.get_text(strip=True)
            })
            span.replace_with(emoticon)

    def _transform_paragraphs(self, soup: BeautifulSoup):
        """Transforms paragraphs to remove newline characters"""
        for p in soup.find_all("p"):
            for child in p.children:
                if isinstance(child, NavigableString):
                    new_text = child.replace("\n", "")
                    child.replace_with(new_text)




class ConfluenceStorageFormatCleaner(NodeVisitor):
    "Removes volatile attributes from a Confluence storage format XHTML document."

    def transform(self, child: ET._Element) -> Optional[ET._Element]:
        child.attrib.pop(ET.QName(namespaces["ac"], "macro-id"), None)
        child.attrib.pop(ET.QName(namespaces["ri"], "version-at-save"), None)
        return None


class DocumentError(RuntimeError):
    "Raised when a converted Markdown document has an unexpected element or attribute."


@dataclass
class ConfluencePageID:
    page_id: str


@dataclass
class ConfluenceQualifiedID:
    page_id: str
    space_key: str


@dataclass
class ConfluenceDocumentOptions:
    """
    Options that control the generated page content.

    :param ignore_invalid_url: When true, ignore invalid URLs in input, emit a warning and replace the anchor with
        plain text; when false, raise an exception.
    :param heading_anchors: When true, emit a structured macro *anchor* for each section heading using GitHub
        conversion rules for the identifier.
    :param generated_by: Text to use as the generated-by prompt (or `None` to omit a prompt).
    :param root_page_id: Confluence page to assume root page role for publishing a directory of Markdown files.
    :param keep_hierarchy: Whether to maintain source directory structure when exporting to Confluence.
    :param render_mermaid: Whether to pre-render Mermaid diagrams into PNG/SVG images.
    :param diagram_output_format: Target image format for diagrams.
    :param webui_links: When true, convert relative URLs to Confluence Web UI links.
    """

    ignore_invalid_url: bool = False
    heading_anchors: bool = False
    generated_by: Optional[str] = "This page has been generated with a tool."
    root_page_id: Optional[ConfluencePageID] = None
    keep_hierarchy: bool = False
    render_mermaid: bool = False
    diagram_output_format: Literal["png", "svg"] = "png"
    webui_links: bool = False


class ConversionError(RuntimeError):
    "Raised when a Markdown document cannot be converted to Confluence Storage Format."


class ConfluenceDocument:
    title: Optional[str]
    labels: Optional[list[str]]
    links: list[str]
    images: list[Path]

    options: ConfluenceDocumentOptions
    root: ET._Element

    @classmethod
    def create(
        cls,
        path: Path,
        options: ConfluenceDocumentOptions,
        root_dir: Path,
        site_metadata: ConfluenceSiteMetadata,
        page_metadata: ConfluencePageCollection,
    ) -> tuple[ConfluencePageID, "ConfluenceDocument"]:
        path = path.resolve(True)

        document = Scanner().read(path)

        if document.page_id is not None:
            page_id = ConfluencePageID(document.page_id)
        else:
            # look up Confluence page ID in metadata
            metadata = page_metadata.get(path)
            if metadata is not None:
                page_id = ConfluencePageID(metadata.page_id)
            else:
                raise PageError("missing Confluence page ID")

        return page_id, ConfluenceDocument(
            path, document, options, root_dir, site_metadata, page_metadata
        )

    def __init__(
        self,
        path: Path,
        document: ScannedDocument,
        options: ConfluenceDocumentOptions,
        root_dir: Path,
        site_metadata: ConfluenceSiteMetadata,
        page_metadata: ConfluencePageCollection,
    ) -> None:
        self.options = options

        # convert to HTML
        html = markdown_to_html(document.text)

        # parse Markdown document
        if self.options.generated_by is not None:
            generated_by = document.generated_by or self.options.generated_by
        else:
            generated_by = None

        if generated_by is not None:
            generated_by_html = markdown_to_html(generated_by)

            content = [
                '<ac:structured-macro ac:name="info" ac:schema-version="1">',
                f"<ac:rich-text-body>{generated_by_html}</ac:rich-text-body>",
                "</ac:structured-macro>",
                html,
            ]
        else:
            content = [html]

        try:
            self.root = elements_from_strings(content)
        except ParseError as ex:
            raise ConversionError(path) from ex

        converter = ConfluenceStorageFormatConverter(
            ConfluenceConverterOptions(
                ignore_invalid_url=self.options.ignore_invalid_url,
                heading_anchors=self.options.heading_anchors,
                render_mermaid=self.options.render_mermaid,
                diagram_output_format=self.options.diagram_output_format,
                webui_links=self.options.webui_links,
            ),
            path,
            root_dir,
            site_metadata,
            page_metadata,
        )
        converter.visit(self.root)
        self.links = converter.links
        self.images = converter.images
        self.embedded_images = converter.embedded_images

        self.title = document.title or converter.toc.get_title()
        self.labels = document.tags

    def xhtml(self) -> str:
        return elements_to_string(self.root)


def attachment_name(name: Union[Path, str]) -> str:
    """
    Safe name for use with attachment uploads.

    Allowed characters:
    * Alphanumeric characters: 0-9, a-z, A-Z
    * Special characters: hyphen (-), underscore (_), period (.)
    """

    return re.sub(r"[^\-0-9A-Za-z_.]", "_", str(name))


def sanitize_confluence(html: str) -> str:
    "Generates a sanitized version of a Confluence storage format XHTML document with no volatile attributes."

    if not html:
        return ""

    root = elements_from_strings([html])
    ConfluenceStorageFormatCleaner().visit(root)
    return elements_to_string(root)


def elements_to_string(root: ET._Element) -> str:
    xml = ET.tostring(root, encoding="utf8", method="xml").decode("utf8")
    m = re.match(r"^<root\s+[^>]*>(.*)</root>\s*$", xml, re.DOTALL)
    if m:
        return m.group(1)
    else:
        raise ValueError("expected: Confluence content")


def _content_to_string(dtd_path: Path, content: str) -> str:
    parser = ET.XMLParser(
        remove_blank_text=True,
        remove_comments=True,
        strip_cdata=False,
        load_dtd=True,
    )

    ns_attr_list = "".join(
        f' xmlns:{key}="{value}"' for key, value in namespaces.items()
    )

    data = [
        '<?xml version="1.0"?>',
        f'<!DOCTYPE ac:confluence PUBLIC "-//Atlassian//Confluence 4 Page//EN" "{dtd_path.as_posix()}">'
        f"<root{ns_attr_list}>",
    ]
    data.append(content)
    data.append("</root>")

    tree = ET.fromstringlist(data, parser=parser)
    return ET.tostring(tree, pretty_print=True).decode("utf-8")


def content_to_string(content: str) -> str:
    "Converts a Confluence Storage Format document returned by the API into a readable XML document."

    resource_path = resources.files(__package__).joinpath("entities.dtd")
    with resources.as_file(resource_path) as dtd_path:
        return _content_to_string(dtd_path, content)
