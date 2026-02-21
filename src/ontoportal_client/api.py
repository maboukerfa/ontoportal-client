"""Utilities for interacting with OntoPortal."""

import xml.etree.ElementTree as ET
from collections.abc import Iterable
from typing import Any, ClassVar, Literal, cast
from urllib.parse import quote

import pystow
import requests
from tqdm import tqdm

from .constants import NAMES, URLS

__all__ = [
    # Concrete clients
    "AgroPortalClient",
    "BioDivPortal",
    "BioPortalClient",
    "EarthPortal",
    "EcoPortalClient",
    "IndustryPortalClient",
    "LovPortal",
    "MatPortalClient",
    "MedPortalClient",
    # Base clients
    "OntoPortalClient",
    "OntoportalAstroClient",
    "PreconfiguredOntoPortalClient",
    "SIFRBioPortalClient",
    "SocioPortal",
    "TechnoPortal",
]

DEFAULT_TIMEOUT = 5


class OntoPortalClient:
    """A client for an OntoPortal site, like BioPortal."""

    def __init__(self, api_key: str, base_url: str):
        """Instantiate the OntoPortal client.

        :param api_key: The API key for the OntoPortal instance
        :param base_url: The base URL for the OntoPortal instance, e.g.,
            ``https://data.bioontology.org`` for BioPortal.
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Get the response JSON."""
        return self.get_response(path=path, params=params, **kwargs).json()

    def get_response(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        raise_for_status: bool = True,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Send a GET request the given endpoint on the OntoPortal site.

        :param path: The path to query following the base URL, e.g., ``/ontologies``. If
            this starts with the base URL, it gets stripped.
        :param params: Parameters to pass through to :func:`requests.get`
        :param raise_for_status: If true and the status code isn't 200, raise an
            exception
        :param timeout: A configurable timeout for sending the request
        :param kwargs: Keyword arguments to pass through to :func:`requests.get`

        :returns: The response from :func:`requests.get`

        The rate limit is 15 queries per second. See:
        https://www.bioontology.org/wiki/Annotator_Optimizing_and_Troublehooting
        """
        if not params:
            params = {}
        params.setdefault("apikey", self.api_key)
        if path.startswith(self.base_url):
            path = path[len(self.base_url) :]
        res = requests.get(
            self.base_url + "/" + path.lstrip("/"),
            params=params,
            timeout=timeout or DEFAULT_TIMEOUT,
            **kwargs,
        )
        if raise_for_status:
            res.raise_for_status()
        return res

    def get_ontologies(self) -> list[dict[str, Any]]:
        """Get ontologies."""
        return self.get_json("ontologies")  # type:ignore

    def get_ontology_versions(self, ontology: str) -> list[dict[str, Any]]:
        """Get all submissions for the given ontology (all metadata for each version)."""
        return self.get_json(f"/ontologies/{ontology.upper()}/submissions")

    def get_latest_changelog(self, ontology: str, version_id: int) -> dict[str, Any]:
        """Get the changelog between this version and the previous one."""
        res = self.get_response(f"/ontologies/{ontology.upper()}/submissions/{version_id}/download_diff")
        return _parse_diff(res.text)

    def annotate(
        self, text: str, ontology: str | None = None, require_exact_match: bool = True
    ) -> list[dict[str, Any]]:
        """Annotate the given text."""
        # possible fields include 'prefLabel', 'synonym', 'definition', 'semanticType', 'cui'
        include = ["prefLabel", "semanticType", "cui"]
        params = {
            "include": ",".join(include),
            "require_exact_match": require_exact_match,
            "text": text,
        }
        if ontology:
            params["ontologies"] = ontology
        return self.get_json("/annotator", params=params)  # type:ignore

    def search(
        self,
        text: str,
        ontology: str | None = None,
        all_results: bool = False,
        page_size: int = 20,
        lang: str | None = None,
        portals: list[str] | None = None,
        also_search_properties: bool | None = None,
        also_search_obsolete: bool | None = None,
        also_search_views: bool | None = None,
        require_exact_match: bool | None = None,
        require_definition: bool | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Search the given text and unroll the paginated results.

        :param text: The text to search for
        :param ontology: Restrict search to a specific ontology
        :param all_results: If True, return all results (all pages). If False, return only the first page.
        :param page_size: Number of results per page (default 20)
        :param lang: The language to search in
        :param portals: Restrict search to specific portals
        :param also_search_properties: If True, search over properties as well
        :param also_search_obsolete: If True, search over obsolete classes
        :param also_search_views: If True, search over ontology views
        :param require_exact_match: If True, require an exact match
        :param require_definition: If True, require classes to have a definition
        """
        for page in self.search_paginated(
            text=text,
            ontology=ontology,
            all_results=all_results,
            page_size=page_size,
            lang=lang,
            portals=portals,
            also_search_properties=also_search_properties,
            also_search_obsolete=also_search_obsolete,
            also_search_views=also_search_views,
            require_exact_match=require_exact_match,
            require_definition=require_definition,
        ):
            yield from page.get("collection", [])

    def search_paginated(
        self,
        text: str,
        ontology: str | None = None,
        start: str = "1",
        all_results: bool = False,
        page_size: int = 20,
        lang: str | None = None,
        portals: list[str] | None = None,
        also_search_properties: bool | None = None,
        also_search_obsolete: bool | None = None,
        also_search_views: bool | None = None,
        require_exact_match: bool | None = None,
        require_definition: bool | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Search the given text.

        :param text: The text to search for
        :param ontology: Restrict search to a specific ontology
        :param start: The page to start from (default "1")
        :param all_results: If True, yield all pages. If False, yield only the first page.
        :param page_size: Number of results per page (default 20)
        :param lang: The language to search in
        :param portals: Restrict search to specific portals
        :param also_search_properties: If True, search over properties as well
        :param also_search_obsolete: If True, search over obsolete classes
        :param also_search_views: If True, search over ontology views
        :param require_exact_match: If True, require an exact match
        :param require_definition: If True, require classes to have a definition
        """
        params: dict[str, Any] = {"q": text, "include": ["prefLabel"], "page": start, "pagesize": page_size}
        if ontology:
            params["ontologies"] = ontology
        if lang:
            params["lang"] = lang
        if portals:
            params["portals"] = portals
        if also_search_properties is not None:
            params["also_search_properties"] = str(also_search_properties).lower()
        if also_search_obsolete is not None:
            params["also_search_obsolete"] = str(also_search_obsolete).lower()
        if also_search_views is not None:
            params["also_search_views"] = str(also_search_views).lower()
        if require_exact_match is not None:
            params["require_exact_match"] = str(require_exact_match).lower()
        if require_definition is not None:
            params["require_definition"] = str(require_definition).lower()
        
        first = True
        while params["page"]:
            result = self.get_json("/search", params)
            yield result
            if not all_results:
                break
            # `result["nextPage"]` is always present but will be null on the last page
            params["page"] = result["nextPage"]

    def get_ancestors(self, ontology: str, uri: str) -> list[dict[str, Any]]:
        """Get the ancestors of the given class."""
        quoted_uri = quote(uri, safe="")
        return cast(
            list[dict[str, Any]],
            self.get_json(
                f"/ontologies/{ontology}/classes/{quoted_uri}/ancestors",
                params={"display_context": "false"},
            ),
        )

    def get_mappings(
        self,
        ontology_1: str,
        ontology_2: str,
        *,
        progress: bool = False,
        timeout: int | None = None,
        display_links: bool = False,
        display_context: bool = False,
    ) -> Iterable[dict[str, Any]]:
        """Get mappings between two ontologies."""
        res_json = self.get_json(
            "/mappings",
            params={
                "ontologies": f"{ontology_1},{ontology_2}",
                "display_links": _bool(display_links),
                "display_context": _bool(display_context),
            },
            timeout=timeout,
        )
        page_count = res_json["pageCount"]
        if not page_count:
            tqdm.write(f"no pages returned from {ontology_1}->{ontology_2}")
            return
        yield from res_json["collection"]
        with tqdm(
            total=page_count,
            disable=page_count == 1 or not progress,
            desc=f"Get mappings {ontology_1}->{ontology_2}",
            unit="page",
        ) as pbar:
            pbar.update(1)  # already did first page
            while next_page := res_json["links"]["nextPage"]:
                pbar.update(1)
                res = requests.get(next_page, timeout=timeout or DEFAULT_TIMEOUT)
                res.raise_for_status()
                res_json = res.json()
                yield from res_json["collection"]


def _bool(x: bool) -> Literal["true", "false"]:
    return "true" if x else "false"


class PreconfiguredOntoPortalClient(OntoPortalClient):
    """A client for an OntoPortal site, like BioPortal."""

    #: The name of the instance
    name: ClassVar[str]

    def __init__(self, api_key: str | None = None, value_key: str = "api_key"):
        """Instantiate the OntoPortal Client.

        :param api_key: The API key for the instance. If not given, use :mod:`pystow` to
            read the configuration in one of the following ways. Using BioPortal as an
            example, where the subclass of :class:`PreconfiguredOntoPortalClient` sets
            the class variable ``name = "bioportal"``, the configuration can be set in
            the following ways:

            1. From `BIOPORTAL_API_KEY` in the environment, where the `name` is
               uppercased before `_API_KEY`
            2. From a configuration file at `~/.config/bioportal.ini` and set the
               `[bioportal]` section in it with the given key
        :param value_key: The name of the key to use. By default, uses ``api_key``
        """
        base_url = URLS[cast(NAMES, self.name)]
        if api_key is None:
            api_key = pystow.get_config(self.name, value_key, raise_on_missing=True)
        super().__init__(api_key=api_key, base_url=base_url)


class BioPortalClient(PreconfiguredOntoPortalClient):
    """A client for BioPortal.

    To get an API key, follow the sign-up process at
    https://bioportal.bioontology.org/account.

    See API documentation at https://data.bioontology.org/documentation.
    """

    name = "bioportal"


class AgroPortalClient(PreconfiguredOntoPortalClient):
    """A client for AgroPortal."""

    name = "agroportal"


class EcoPortalClient(PreconfiguredOntoPortalClient):
    """A client for EcoPortal."""

    name = "ecoportal"


class MatPortalClient(PreconfiguredOntoPortalClient):
    """A client for materials science ontologies in `MatPortal <https://matportal.org>`_.

    Create an account and get an API key by starting at
    https://matportal.org/accounts/new.
    """

    name = "matportal"


class SIFRBioPortalClient(PreconfiguredOntoPortalClient):
    """A client for French biomedical ontologies in `SIFR BioPortal <http://bioportal.lirmm.fr>`_.

    Create an account and get an API key by starting at
    http://bioportal.lirmm.fr/accounts/new.
    """

    name = "sifr_bioportal"


class MedPortalClient(PreconfiguredOntoPortalClient):
    """A client for medical ontologies in `MedPortal <https://medportal.bmicc.cn>`_.

    Create an account and get an API key by starting at
    https://medportal.bmicc.cn/accounts/new.
    """

    name = "medportal"


class IndustryPortalClient(PreconfiguredOntoPortalClient):
    """A client for industrial ontologies in `IndustryPortal <https://industryportal.enit.fr>`_.

    Create an account and get an API key by starting at
    https://industryportal.enit.fr/accounts/new.
    """

    name = "industryportal"


class OntoportalAstroClient(PreconfiguredOntoPortalClient):
    """A client for astrophysics ontologies in `OntoPortal-Astro <https://ontoportal-astro.eu/>`_.

    Create an account and get an API key by starting at
    https://ontoportal-astro.eu/accounts/new.
    """

    name = "ontoportal-astro"


class BioDivPortal(PreconfiguredOntoPortalClient):
    """A client for biodiversity ontologies in `BioDivPortal <https://biodivportal.gfbio.org/>`_.

    Create an account and get an API key by starting at
    https://biodivportal.gfbio.org/accounts/new.
    """

    name = "biodivportal"


class EarthPortal(PreconfiguredOntoPortalClient):
    """A client for biodiversity ontologies in `EarthPortal <https://earthportal.eu/>`_.

    Create an account and get an API key by starting at
    https://earthportal.eu/accounts/new.

    .. warning:: This resource is dead
    """

    name = "earthportal"


class SocioPortal(PreconfiguredOntoPortalClient):
    """A client for sociology ontologies in `SocioPortal <https://socioportal.org/>`_.

    Create an account and get an API key by starting at
    https://socioportal.org/accounts/new.
    """

    name = "socioportal"


class TechnoPortal(PreconfiguredOntoPortalClient):
    """A client for engineering and technology ontologies in `TechnoPortal <https://technoportal.hevs.ch/>`_.

    Create an account and get an API key by starting at
    https://technoportal.hevs.ch/accounts/new.
    """

    name = "technoportal"


class LovPortal(PreconfiguredOntoPortalClient):
    """A client for semantic web ontologies in `LovPortal <https://lovportal.lirmm.fr/>`_.

    Create an account and get an API key by starting at
    https://lovportal.lirmm.fr/accounts/new.
    """

    name = "lovportal"


def _parse_diff(text: str) -> dict[str, Any]:
    """Parse an XML diff report from OntoPortal."""
    root = ET.fromstring(text)
    res: dict[str, Any] = {}

    summary_node = root.find("diffSummary")
    if summary_node is not None:
        res["summary"] = {
            node.tag: int(node.text.strip()) if node.text and node.text.strip() else 0
            for node in summary_node
        }

    for section in ["changedClasses", "newClasses", "deletedClasses"]:
        section_node = root.find(section)
        if section_node is not None:
            items = []
            item_tag = section[:-2] if section.endswith("Classes") else section
            for item_node in section_node.findall(item_tag):
                item: dict[str, Any] = {}
                for child in item_node:
                    tag = child.tag
                    value = child.text.strip() if child.text else ""
                    if tag in {"newAxiom", "deletedAxiom"}:
                        item.setdefault(tag + "s", []).append(value)
                    else:
                        item[tag] = value
                items.append(item)
            res[section] = items
    return res
