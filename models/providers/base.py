"""
Base classes and data types for metadata providers.

This module defines the abstract base class that all metadata providers must implement,
along with unified data classes for search results and issue data.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from enum import Enum


class ProviderType(Enum):
    """Enumeration of supported metadata providers."""
    METRON = "metron"
    COMICVINE = "comicvine"
    GCD = "gcd"
    ANILIST = "anilist"
    BEDETHEQUE = "bedetheque"
    MANGADEX = "mangadex"


@dataclass
class SearchResult:
    """Unified search result across all providers."""
    provider: ProviderType
    id: str
    title: str
    year: Optional[int] = None
    publisher: Optional[str] = None
    issue_count: Optional[int] = None
    cover_url: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "provider": self.provider.value,
            "id": self.id,
            "title": self.title,
            "year": self.year,
            "publisher": self.publisher,
            "issue_count": self.issue_count,
            "cover_url": self.cover_url,
            "description": self.description
        }


@dataclass
class IssueResult:
    """Unified issue data across all providers."""
    provider: ProviderType
    id: str
    series_id: str
    issue_number: str
    title: Optional[str] = None
    cover_date: Optional[str] = None
    store_date: Optional[str] = None
    cover_url: Optional[str] = None
    summary: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "provider": self.provider.value,
            "id": self.id,
            "series_id": self.series_id,
            "issue_number": self.issue_number,
            "title": self.title,
            "cover_date": self.cover_date,
            "store_date": self.store_date,
            "cover_url": self.cover_url,
            "summary": self.summary
        }


@dataclass
class ProviderCredentials:
    """Credentials for a metadata provider."""
    api_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in {
            "api_key": self.api_key,
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.database
        }.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProviderCredentials":
        """Create from dictionary."""
        return cls(
            api_key=data.get("api_key"),
            username=data.get("username"),
            password=data.get("password"),
            host=data.get("host"),
            port=data.get("port"),
            database=data.get("database")
        )


class BaseProvider(ABC):
    """
    Abstract base class for all metadata providers.

    All metadata providers must inherit from this class and implement
    the required abstract methods to provide a consistent interface.
    """

    # Class attributes to be overridden by subclasses
    provider_type: ProviderType
    display_name: str
    requires_auth: bool = True
    auth_fields: List[str] = []  # e.g., ["api_key"] or ["username", "password"]

    # Default rate limit (requests per minute)
    rate_limit: int = 30

    def __init__(self, credentials: Optional[ProviderCredentials] = None):
        """
        Initialize the provider with optional credentials.

        Args:
            credentials: Provider credentials for authentication
        """
        self.credentials = credentials
        self._client = None

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Verify credentials and connectivity to the provider.

        Returns:
            True if connection is successful, False otherwise
        """
        pass

    @abstractmethod
    def search_series(self, query: str, year: Optional[int] = None) -> List[SearchResult]:
        """
        Search for series/volumes matching the query.

        Args:
            query: Search string (series name)
            year: Optional year to filter results

        Returns:
            List of matching SearchResult objects
        """
        pass

    @abstractmethod
    def get_series(self, series_id: str) -> Optional[SearchResult]:
        """
        Get series details by provider-specific ID.

        Args:
            series_id: The provider's series/volume ID

        Returns:
            SearchResult with series details, or None if not found
        """
        pass

    @abstractmethod
    def get_issues(self, series_id: str) -> List[IssueResult]:
        """
        Get all issues for a series.

        Args:
            series_id: The provider's series/volume ID

        Returns:
            List of IssueResult objects for all issues in the series
        """
        pass

    @abstractmethod
    def get_issue(self, issue_id: str) -> Optional[IssueResult]:
        """
        Get issue details by provider-specific ID.

        Args:
            issue_id: The provider's issue ID

        Returns:
            IssueResult with issue details, or None if not found
        """
        pass

    @abstractmethod
    def to_comicinfo(self, issue: IssueResult, series: Optional[SearchResult] = None) -> Dict[str, Any]:
        """
        Convert provider data to ComicInfo.xml field mapping.

        Args:
            issue: The issue data to convert
            series: Optional series data for additional fields

        Returns:
            Dictionary mapping ComicInfo.xml field names to values
        """
        pass

    def get_provider_info(self) -> Dict[str, Any]:
        """Get provider metadata for API responses."""
        return {
            "type": self.provider_type.value,
            "name": self.display_name,
            "requires_auth": self.requires_auth,
            "auth_fields": self.auth_fields,
            "rate_limit": self.rate_limit
        }
