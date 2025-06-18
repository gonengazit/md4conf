"""
Publish Markdown files to Confluence wiki.

Copyright 2022-2025, Levente Hunyadi

:see: https://github.com/hunyadi/md2conf
"""

import hashlib
import logging
import os.path
import re
import shutil
import string
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Optional, Sequence, Union, overload
from urllib.parse import quote_plus, urlparse, urlunparse

from bs4 import BeautifulSoup, CData, Tag
from bs4._typing import _AtMostOneElement, _QueryResults
from bs4.element import AttributeValueList, NavigableString, PageElement

from .collection import ConfluencePageCollection
from .mermaid import render_diagram
from .metadata import ConfluenceSiteMetadata, ConfluencePageMetadata
from .properties import PageError
from .scanner import ScannedDocument, Scanner

LOGGER = logging.getLogger(__name__)


@overload
def bs4_tagonly[**P](f: Callable[P, _QueryResults]) -> Callable[P, Sequence[Tag]]: ...
@overload
def bs4_tagonly[**P](
    f: Callable[P, _AtMostOneElement],
) -> Callable[P, Optional[Tag]]: ...


def bs4_tagonly[**P](
    f: Callable[P, _QueryResults] | Callable[P, _AtMostOneElement],
) -> Callable[P, Sequence[Tag] | Optional[Tag]]:
    def _inner(*args: P.args, **kwargs: P.kwargs) -> Sequence[Tag] | Optional[Tag]:
        if "name" in kwargs:
            assert kwargs["name"] is not None
        else:
            # (self, name, ...)
            assert len(args) >= 2, args

        # Safety: From bs4 code: "NavigableString can only match [...] does not define any name or attribute rules"
        #         we require `name` to not be None
        return f(*args, **kwargs)  # type: ignore[return-value]

    return _inner


bs4_find = bs4_tagonly(Tag.find)
bs4_find_all = bs4_tagonly(Tag.find_all)


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


def markdown_to_html(content: str) -> str:
    if shutil.which("pandoc") is None:
        raise ConversionError("Couldn't find pandoc")

    pandoc_filters_dir = Path(__file__).parent / Path("../pandoc_filters")
    cmd = [
        "pandoc",
        "--from=gfm+hard_line_breaks+wikilinks_title_after_pipe",
        "--to=html",
        "--mathjax",
        "--no-highlight",
        "--wrap=none",
        f"--lua-filter={pandoc_filters_dir / 'highlight.lua'}",
        f"--lua-filter={pandoc_filters_dir / 'wikilink-image-filter.lua'}",
    ]
    pandoc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    stdout, stderr = pandoc.communicate(content.encode("utf-8"))
    if pandoc.returncode != 0:
        raise ConversionError(f"pandoc error: {stderr!r}")
    else:
        return stdout.decode("utf-8")


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


def title_to_identifier(title: str) -> str:
    "Converts a section heading title to a GitHub-style Markdown same-page anchor."

    s = title.strip().lower()
    s = re.sub("[^ A-Za-z0-9]", "", s)
    s = s.replace(" ", "-")
    return s


def element_to_text(element: Tag) -> str:
    """Gets all text from a BeautifulSoup tag."""
    return element.get_text(strip=True)


def is_rtl(text: Optional[str]) -> Optional[bool]:
    """ "returns whether a text is rtl. returns True for rtl, False for ltr, and None for unspecified"""
    if text is None:
        return None
    for c in text:
        if c in string.ascii_letters:
            return False
        if c in "אבגדהוזחטיכךלמםנןסעפףצץקרשת":
            return True
    return None


def set_direction_style(tag: Tag, rtl: Optional[bool]) -> None:
    if rtl:
        tag["style"] = "direction: rtl; text-align: right;"
    else:
        # if rtl is None: default is ltr
        tag["style"] = "direction: ltr; text-align: left;"


INLINE_ELEMENTS = {
    "a",
    "abbr",
    "acronym",
    "b",
    "bdo",
    "big",
    "button",
    "cite",
    "code",
    "dfn",
    "em",
    "i",
    "input",
    "kbd",
    "label",
    "q",
    "samp",
    "select",
    "small",
    "strong",
    "sub",
    "sup",
    "time",
    "var",
}


def is_inline(element: PageElement) -> bool:
    if not isinstance(element, Tag):
        assert isinstance(element, NavigableString)
        return True
    if element.name in INLINE_ELEMENTS:
        return True
    if element.name == "span":
        if not element.has_attr("class") or list(element["class"]) != [
            "math",
            "display",
        ]:
            return True
    return False


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

    def convert(self, html_content: str) -> None:
        """
        Main conversion function. Parses HTML and applies all transformations.

        Args:
            html_content: A string containing the HTML to convert.

        Returns:
            A string with the converted Confluence Storage Format XHTML.
        """
        self.soup = BeautifulSoup(html_content, "html.parser")

        # The order of transformations can be important.
        self._transform_paragraph_images()
        self._transform_headings()
        self._transform_links()
        self._transform_images()
        self._transform_code_blocks()
        self._transform_toc()
        self._transform_admonitions()
        self._transform_alerts()
        self._transform_sections()
        self._transform_emojis()
        self._transform_paragraphs()
        self._transform_math()

    def _create_macro(
        self,
        name: str,
        params: Optional[dict[str, str]] = None,
        plain_text_body: Optional[str] = None,
        rich_text_body: Optional[list[PageElement]] = None,
    ) -> Tag:
        """Helper to create a generic Confluence <ac:structured-macro>."""
        macro = self.soup.new_tag("ac:structured-macro")
        macro["ac:name"] = name
        macro["ac:schema-version"] = "1"

        if params:
            for key, value in params.items():
                param_tag = self.soup.new_tag("ac:parameter")
                param_tag["ac:name"] = key
                param_tag.string = value
                macro.append(param_tag)

        if plain_text_body is not None:
            body_tag = self.soup.new_tag("ac:plain-text-body")
            body_tag.append(CData(plain_text_body))
            macro.append(body_tag)

        if rich_text_body is not None:
            rich_body_tag = self.soup.new_tag("ac:rich-text-body")
            rich_body_tag.extend(rich_text_body)
            macro.append(rich_body_tag)

        return macro

    def _transform_headings(self) -> None:
        """Adds heading anchors and populates the Table of Contents."""

        heading_tags = bs4_find_all(self.soup, re.compile(r"^h[1-6]$", re.IGNORECASE))
        for heading in heading_tags:
            level = int(heading.name[1])
            title = element_to_text(heading)
            self.toc.add(level, title)

            if self.options.heading_anchors:
                anchor_id = title_to_identifier(title)
                anchor_macro = self._create_macro("anchor", {"": anchor_id})
                heading.insert(0, anchor_macro)

            set_direction_style(heading, is_rtl(title))

    def _resolve_wikilink(self, link: str) -> Optional[ConfluencePageMetadata]:
        link_path = Path(link)
        if link_path.suffix != ".md":
            link_path = link_path.with_suffix(".md")

        link_parts = link_path.parts

        for path, metadata in self.page_metadata.items():
            if path.parts[-len(link_parts) :] == link_parts:
                return metadata
        return None

    def _find_wikilink_attachment(self, src: str) -> Optional[Path]:
        """find an image matching the path in the base directory"""
        src_path = Path(src)
        if (self.base_dir / src_path).exists():
            return src_path

        LOGGER.debug(f"looking for wikilink attachment {src_path}")

        for dirpath, _, filenames in os.walk(self.base_dir):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                if src_path.parts == file_path.parts[-len(src_path.parts) :]:
                    logging.debug(f"found {filename} in directory {dirpath}")
                    return Path(dirpath) / filename

        LOGGER.error(f"Couldn't find attachment {src_path}")
        return None

    def _transform_links(self) -> None:
        """Converts relative page links to Confluence web links."""

        for anchor in bs4_find_all(self.soup, "a", href=True):
            url = anchor["href"]
            assert isinstance(url, str)

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
                    target = title_to_identifier(relative_url.fragment.lstrip("#"))
                    link_wrapper = self.soup.new_tag(
                        "ac:link", attrs={"ac:anchor": target}
                    )
                    link_body = self.soup.new_tag("ac:link-body")
                    link_body.extend(anchor.contents)  # Move content
                    link_wrapper.append(link_body)
                    anchor.replace_with(link_wrapper)
                continue

            # Handle links to other pages
            try:
                absolute_path = (self.base_dir / relative_url.path).resolve()
                if not str(absolute_path).startswith(str(self.root_dir)):
                    raise DocumentError(f"Link {url} points outside project root.")

                link_metadata = self.page_metadata.get(absolute_path)
                if not link_metadata and anchor["title"] == "wikilink":
                    link_metadata = self._resolve_wikilink(relative_url.path)
                if not link_metadata:
                    raise DocumentError(f"No page metadata found for link: {url}")

                relative_path = os.path.relpath(absolute_path, self.base_dir)
                LOGGER.debug(
                    "found link to page %s with metadata: %s",
                    relative_path,
                    link_metadata,
                )

                self.links.append(url)

                if self.options.webui_links:
                    page_url = f"{self.site_metadata.base_path}pages/viewpage.action?pageId={link_metadata.page_id}"
                else:
                    space_key = link_metadata.space_key or self.site_metadata.space_key
                    if not space_key:
                        raise DocumentError(
                            "Space key is required to build page links."
                        )
                    page_url = f"{self.site_metadata.base_path}spaces/{space_key}/pages/{link_metadata.page_id}/{encode_title(link_metadata.title)}"

                components = urlparse(page_url)
                transformed_url = urlunparse(
                    relative_url._replace(
                        scheme=components.scheme or "https",
                        netloc=components.netloc or self.site_metadata.domain,
                        path=components.path,
                    )
                )
                anchor["href"] = transformed_url

            except (DocumentError, FileNotFoundError) as e:
                print(f"Warning: {e}")
                if self.options.ignore_invalid_url:
                    del anchor["href"]  # Make it a dead link
                else:
                    raise

    def _create_ac_image_tag(self, img_tag: Tag) -> Tag:
        """Helper to create a Confluence <ac:image> tag from an HTML <img> tag.

        If the image path doesn't exist but ignore_invalid_url is True - will just return a <br/> tag
        """
        src = img_tag.get("src")
        if not src or not isinstance(src, str):
            raise DocumentError("Image lacks 'src' attribute.")

        ac_image = self.soup.new_tag("ac:image")
        for attr in ["width", "height"]:
            if img_tag.has_attr(attr):
                ac_image[f"ac:{attr}"] = img_tag[attr]

        # Add attachment or URL element
        if is_absolute_url(src):
            ri_child = self.soup.new_tag("ri:url", attrs={"ri:value": src})
        else:
            if img_tag["title"] == "wikilink":
                path = self._find_wikilink_attachment(src)
            else:
                path = Path(src)

            if path is None or not (self.base_dir / path).exists():
                if self.options.ignore_invalid_url:
                    logging.error(f"couldn't upload image with path {src}")
                    return self.soup.new_tag("br")
                else:
                    raise DocumentError(f"couldn't upload image with path {src}")

            # Logic to prefer PNG over SVG
            if path.suffix == ".svg":
                png_file = path.with_suffix(".png")
                if (self.base_dir / png_file).exists():
                    path = png_file

            self.images.append(path)
            image_name = attachment_name(path)
            ri_child = self.soup.new_tag(
                "ri:attachment", attrs={"ri:filename": image_name}
            )

        ac_image.append(ri_child)

        # Add caption if alt text exists
        if img_tag.has_attr("alt"):
            assert isinstance(img_tag["alt"], str)
            caption_tag = self.soup.new_tag("ac:caption")
            p_tag = self.soup.new_tag("p")
            p_tag.string = img_tag["alt"]
            caption_tag.append(p_tag)
            ac_image.append(caption_tag)

        return ac_image

    def _transform_paragraph_images(self) -> None:
        """Transforms <p><img></p> into a single <ac:image> block."""

        for p_tag in bs4_find_all(self.soup, "p"):
            # Find paragraphs containing only an image tag and whitespace
            meaningful_children = [c for c in p_tag.children if isinstance(c, Tag)]
            if len(meaningful_children) == 1 and meaningful_children[0].name == "img":
                ac_image = self._create_ac_image_tag(meaningful_children[0])
                p_tag.replace_with(ac_image)

    def _transform_images(self) -> None:
        """Transforms standalone <img> tags."""
        for img_tag in bs4_find_all(self.soup, "img"):
            ac_image = self._create_ac_image_tag(img_tag)
            img_tag.replace_with(ac_image)

    def _transform_code_blocks(self) -> None:
        """Converts <pre><code> blocks into Confluence code macros."""
        for pre_tag in bs4_find_all(self.soup, "pre"):
            code_tag = pre_tag.find("code")
            if not code_tag:
                continue

            pre_classes = pre_tag.get("class")
            if isinstance(pre_classes, list):
                language = pre_classes[0]
            else:
                language = ""
            content = code_tag.get_text().rstrip()

            if language.lower() == "mermaid":
                # Handle mermaid diagrams
                if self.options.render_mermaid:
                    image_data = render_diagram(
                        content, self.options.diagram_output_format
                    )
                    image_hash = hashlib.md5(image_data).hexdigest()
                    image_filename = (
                        f"embedded_{image_hash}.{self.options.diagram_output_format}"
                    )
                    self.embedded_images[image_filename] = image_data

                    new_tag = self.soup.new_tag("ac:image")
                    ri_attachment = self.soup.new_tag(
                        "ri:attachment", attrs={"ri:filename": image_filename}
                    )
                    new_tag.append(ri_attachment)
                else:
                    # Diagram macro for live rendering in Confluence
                    new_tag = self._create_macro(
                        "macro-diagram", {"syntax": "Mermaid"}, content
                    )
            else:
                # Standard code block macro
                params = {"language": language, "theme": "Default"}
                new_tag = self._create_macro("code", params, content)

            pre_tag.replace_with(new_tag)

    def _transform_toc(self) -> None:
        """Transforms a [TOC] placeholder into a Confluence TOC macro."""
        for p_tag in bs4_find_all(self.soup, "p"):
            if p_tag.get_text(strip=True) in ["[TOC]", "[[TOC]]"]:
                toc_macro = self._create_macro("toc", {"style": "default"})
                p_tag.replace_with(toc_macro)

    def _transform_admonitions(self) -> None:
        """Transforms admonition divs into info/note/warning macros."""
        for div in bs4_find_all(self.soup, "div", class_="admonition"):
            class_list_ = div.get("class")
            if isinstance(class_list_, AttributeValueList):
                class_list: Sequence[str] = class_list_
            else:
                class_list = []

            admonition_type = next(
                (c for c in class_list if c in ["info", "tip", "note", "warning"]), None
            )
            if not admonition_type:
                continue

            title_p = div.find("p", class_="admonition-title")
            params = {}
            if title_p:
                params["title"] = title_p.get_text(strip=True)
                title_p.decompose()  # Remove the title paragraph

            macro = self._create_macro(
                admonition_type, params, rich_text_body=div.contents
            )
            div.replace_with(macro)

    def _transform_alerts(self) -> None:
        """Transforms GitHub/GitLab style blockquote alerts."""
        for bq in bs4_find_all(self.soup, "blockquote"):
            p_tag = bs4_find(bq, "p")
            if not p_tag or not p_tag.text:
                continue

            text = p_tag.get_text(strip=True)
            class_name, skip = None, 0

            # GitHub: [!NOTE]
            gh_match = re.match(r"^\[!([A-Za-z]+)\][\+\-]?\s*", p_tag.text.lstrip())
            if gh_match:
                alert_map = {
                    "NOTE": "info",
                    "ABSTRACT": "info",
                    "SUMMARY": "info",
                    "TLDR": "info",
                    "INFO": "info",
                    "TODO": "info",
                    "EXAMPLE": "info",
                    "TIP": "tip",
                    "HINT": "tip",
                    "IMPORTANT": "tip",
                    "SUCCESS": "tip",
                    "CHECK": "tip",
                    "DONE": "tip",
                    "WARNING": "note",
                    "CAUTION": "note",
                    "ATTENTION": "note",
                    "FAILURE": "warning",
                    "FAIL": "warning",
                    "MISSING": "warning",
                    "DANGER": "warning",
                    "ERROR": "warning",
                    "BUG": "warning",
                }
                class_name = alert_map.get(gh_match.group(1).upper(), "info")
                skip = len(gh_match.group(0))

            # GitLab: NOTE:
            gl_match = re.match(r"^(FLAG|NOTE|WARNING|DISCLAIMER):\s*", text)
            if not class_name and gl_match:
                alert_map = {
                    "FLAG": "tip",
                    "NOTE": "note",
                    "WARNING": "warning",
                    "DISCLAIMER": "info",
                }
                class_name = alert_map.get(gl_match.group(1))
                skip = len(gl_match.group(0))

            if class_name:
                # Remove the alert prefix from the text node
                first_text_node = p_tag.find(string=True)
                if first_text_node:
                    # Gal: Unclear how this cannot be true, but that's what the typing says
                    assert isinstance(first_text_node, (Tag, NavigableString))

                    string = first_text_node.string
                    # Gal: Should always be true due to the find filter...
                    assert string is not None

                    title_stripped = self.soup.new_tag(
                        "strong", string=string.lstrip()[skip:]
                    )

                    first_text_node.replace_with(title_stripped)

                macro = self._create_macro(class_name, rich_text_body=bq.contents)

                bq.replace_with(macro)

    def _transform_sections(self) -> None:
        """Transforms <details><summary> sections into expand macros."""
        for details in bs4_find_all(self.soup, "details"):
            summary = details.find("summary")
            if not summary:
                continue

            title = summary.get_text(strip=True)
            summary.decompose()  # Remove summary from content

            macro = self._create_macro(
                "expand", {"title": title}, rich_text_body=details.contents
            )
            details.replace_with(macro)

    def _transform_emojis(self) -> None:
        """Transforms emoji spans into emoticons."""
        for span in bs4_find_all(
            self.soup, "span", attrs={"data-emoji-shortname": True}
        ):
            shortname = span["data-emoji-shortname"]
            assert isinstance(shortname, str)
            emoji_id = span.get("data-emoji-unicode", "")
            assert isinstance(emoji_id, str)
            emoticon = self.soup.new_tag(
                "ac:emoticon",
                attrs={
                    "ac:name": shortname,
                    "ac:emoji-shortname": f":{shortname}:",
                    "ac:emoji-id": emoji_id,
                    "ac:emoji-fallback": span.get_text(strip=True),
                },
            )
            span.replace_with(emoticon)

    def _transform_paragraphs(self) -> None:
        """Transforms paragraphs to remove newline characters"""
        for p in bs4_find_all(self.soup, "p"):
            for child in p.children:
                if isinstance(child, NavigableString):
                    new_text = child.replace("\n", "")

                    # TODO: Get rid of the `NavigableString` cast when https://bugs.launchpad.net/beautifulsoup/+bug/2114746 is resolved
                    child.replace_with(NavigableString(new_text))

            # split children into lines
            lines: list[list[Tag | NavigableString]] = [[]]
            for child in list(p.children):
                assert isinstance(child, (Tag, NavigableString))
                lines[-1].append(child)
                if not is_inline(child):
                    lines.append([])
            if not lines[-1]:
                lines.pop()

            new_ps = [self.soup.new_tag("p")]
            last_p_rtl = None
            for line in lines:
                line_rtl = None
                for child in line:
                    child_rtl = is_rtl(child.string)
                    if child_rtl is not None:
                        line_rtl = child_rtl
                        break

                if line_rtl is not None:
                    if last_p_rtl is not None and last_p_rtl != line_rtl:
                        last_element = new_ps[-1].contents[-1]
                        if isinstance(last_element, Tag) and last_element.name == "br":
                            new_ps[-1].contents.pop()
                        new_ps.append(self.soup.new_tag("p"))
                    last_p_rtl = line_rtl
                new_ps[-1].extend(line)

                set_direction_style(new_ps[-1], last_p_rtl)

            p.replace_with(*new_ps)

    def _transform_math(self) -> None:
        for math_inline in bs4_find_all(
            self.soup, "span", attrs={"class": "math inline"}
        ):
            math_string = math_inline.string
            assert math_string is not None
            math_macro = self._create_macro(
                "eazy-math-inline",
                {"body": math_string.lstrip(" (\\").rstrip("\\) ")},
            )
            math_inline.replace_with(math_macro)

        for math_block in bs4_find_all(
            self.soup, "span", attrs={"class": "math display"}
        ):
            math_string = math_block.string
            assert math_string is not None
            math_macro = self._create_macro(
                "easy-math-block",
                {
                    "body": math_string.lstrip("\\[ ").rstrip("]\\ "),
                    "align": "center",
                },
            )
            math_macro["data-layout"] = "default"
            math_block.replace_with(math_macro)


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

            html = (
                f'<ac:structured-macro ac:name="info" ac:schema-version="1"><ac:rich-text-body>{generated_by_html}</ac:rich-text-body></ac:structured-macro>'
                + html
            )

        self.converter = ConfluenceStorageFormatConverter(
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
        self.converter.convert(html)
        self.images = self.converter.images
        self.links = self.converter.links
        self.embedded_images = self.converter.embedded_images

        self.title = document.title
        self.labels = document.tags

    def xhtml(self) -> str:
        return str(self.converter.soup)


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
    soup = BeautifulSoup(html, "html.parser")
    VOLATILE_ATTRS = ["ac:macro-id", "ri:version-at-save"]
    for tag in bs4_find_all(soup, True):
        for attr in VOLATILE_ATTRS:
            if attr in tag.attrs:
                del tag[attr]
    return str(soup)


def content_to_string(content: str) -> str:
    "Converts a Confluence Storage Format document returned by the API into a readable XML document."
    return content
