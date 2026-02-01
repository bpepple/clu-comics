"""
MangaDex Provider Adapter.

Uses the mangadex Python wrapper for MangaDex API V5.
API Documentation: https://api.mangadex.org/docs/
Library: https://github.com/EMACC99/mangadex
"""
from typing import Optional, List, Dict, Any

from app_logging import app_logger
from .base import BaseProvider, ProviderType, ProviderCredentials, SearchResult, IssueResult
from . import register_provider


@register_provider
class MangaDexProvider(BaseProvider):
    """MangaDex metadata provider.

    MangaDex API is public and does not require authentication for basic searches.
    Uses the 'mangadex' Python wrapper library.
    """

    provider_type = ProviderType.MANGADEX
    display_name = "MangaDex"
    requires_auth = False
    auth_fields = []
    rate_limit = 60  # Conservative rate limit for public API

    def __init__(self, credentials: Optional[ProviderCredentials] = None):
        super().__init__(credentials)
        self._manga_api = None
        self._chapter_api = None

    def _get_manga_api(self):
        """Lazy initialization of Manga API."""
        if self._manga_api is None:
            try:
                from mangadex import Manga
                self._manga_api = Manga()
            except ImportError:
                app_logger.error("mangadex library not installed. Run: pip install mangadex")
                return None
        return self._manga_api

    def _get_chapter_api(self):
        """Lazy initialization of Chapter API."""
        if self._chapter_api is None:
            try:
                from mangadex import Chapter
                self._chapter_api = Chapter()
            except ImportError:
                app_logger.error("mangadex library not installed. Run: pip install mangadex")
                return None
        return self._chapter_api

    def _get_localized_value(self, attr_dict: Dict, preferred_lang: str = 'en') -> Optional[str]:
        """
        Extract value from localized attribute dictionary.
        MangaDex stores titles/descriptions as {'en': 'Title', 'ja': 'タイトル', ...}
        """
        if not attr_dict:
            return None
        if isinstance(attr_dict, str):
            return attr_dict
        # Prefer English, fallback to first available
        if preferred_lang in attr_dict:
            return attr_dict[preferred_lang]
        # Try common fallbacks
        for lang in ['en', 'en-us', 'ja-ro', 'ja']:
            if lang in attr_dict:
                return attr_dict[lang]
        # Return first available
        if attr_dict:
            return next(iter(attr_dict.values()), None)
        return None

    def _get_cover_url(self, manga_id: str, cover_filename: str) -> str:
        """Construct cover image URL from manga ID and cover filename."""
        if not cover_filename:
            return None
        return f"https://uploads.mangadex.org/covers/{manga_id}/{cover_filename}"

    def test_connection(self) -> bool:
        """Test connection by fetching a random manga."""
        try:
            manga_api = self._get_manga_api()
            if not manga_api:
                return False
            result = manga_api.random_manga()
            return result is not None
        except Exception as e:
            app_logger.error(f"MangaDex connection test failed: {e}")
            return False

    def search_series(self, query: str, year: Optional[int] = None) -> List[SearchResult]:
        """Search for manga on MangaDex."""
        try:
            manga_api = self._get_manga_api()
            if not manga_api:
                return []

            # Search with title query
            results = manga_api.get_manga_list(title=query, limit=20)

            if not results:
                return []

            search_results = []
            for manga in results:
                try:
                    manga_id = manga.manga_id if hasattr(manga, 'manga_id') else str(manga.id) if hasattr(manga, 'id') else None
                    if not manga_id:
                        continue

                    # Get title (localized)
                    title = None
                    if hasattr(manga, 'title'):
                        title = self._get_localized_value(manga.title)

                    # Get year
                    manga_year = getattr(manga, 'year', None)

                    # Filter by year if specified
                    if year and manga_year and manga_year != year:
                        continue

                    # Get description
                    description = None
                    if hasattr(manga, 'description'):
                        description = self._get_localized_value(manga.description)
                        if description and len(description) > 500:
                            description = description[:500] + "..."

                    # Get cover URL
                    cover_url = None
                    if hasattr(manga, 'cover_id') and manga.cover_id:
                        # Need to fetch cover details
                        try:
                            from mangadex import Cover
                            cover_api = Cover()
                            cover = cover_api.get_cover(manga.cover_id)
                            if cover and hasattr(cover, 'file_name'):
                                cover_url = self._get_cover_url(manga_id, cover.file_name)
                        except Exception:
                            pass

                    # Get chapter count
                    issue_count = None
                    if hasattr(manga, 'last_chapter') and manga.last_chapter:
                        try:
                            issue_count = int(float(manga.last_chapter))
                        except (ValueError, TypeError):
                            pass

                    search_results.append(SearchResult(
                        provider=self.provider_type,
                        id=manga_id,
                        title=title or "Unknown Title",
                        year=manga_year,
                        publisher=None,  # MangaDex doesn't have publisher info
                        issue_count=issue_count,
                        cover_url=cover_url,
                        description=description
                    ))
                except Exception as e:
                    app_logger.warning(f"Error parsing manga result: {e}")
                    continue

            return search_results
        except Exception as e:
            app_logger.error(f"MangaDex search failed: {e}")
            return []

    def get_series(self, series_id: str) -> Optional[SearchResult]:
        """Get manga details by MangaDex ID."""
        try:
            manga_api = self._get_manga_api()
            if not manga_api:
                return None

            manga = manga_api.view_manga_by_id(series_id)
            if not manga:
                return None

            # Get title
            title = None
            if hasattr(manga, 'title'):
                title = self._get_localized_value(manga.title)

            # Get year
            manga_year = getattr(manga, 'year', None)

            # Get description
            description = None
            if hasattr(manga, 'description'):
                description = self._get_localized_value(manga.description)

            # Get cover URL
            cover_url = None
            if hasattr(manga, 'cover_id') and manga.cover_id:
                try:
                    from mangadex import Cover
                    cover_api = Cover()
                    cover = cover_api.get_cover(manga.cover_id)
                    if cover and hasattr(cover, 'file_name'):
                        cover_url = self._get_cover_url(series_id, cover.file_name)
                except Exception:
                    pass

            # Get chapter count
            issue_count = None
            if hasattr(manga, 'last_chapter') and manga.last_chapter:
                try:
                    issue_count = int(float(manga.last_chapter))
                except (ValueError, TypeError):
                    pass

            return SearchResult(
                provider=self.provider_type,
                id=series_id,
                title=title or "Unknown Title",
                year=manga_year,
                publisher=None,
                issue_count=issue_count,
                cover_url=cover_url,
                description=description
            )
        except Exception as e:
            app_logger.error(f"MangaDex get_series failed: {e}")
            return None

    def get_issues(self, series_id: str) -> List[IssueResult]:
        """Get chapters for a manga."""
        try:
            manga_api = self._get_manga_api()
            if not manga_api:
                return []

            # Get volumes and chapters for the manga
            volumes_chapters = manga_api.get_manga_volumes_and_chapters(series_id, translatedLanguage=['en'])

            if not volumes_chapters:
                return []

            results = []
            seen_chapters = set()

            # volumes_chapters is a dict: {'volumes': {'1': {'chapters': {...}}, ...}}
            if isinstance(volumes_chapters, dict) and 'volumes' in volumes_chapters:
                for vol_num, vol_data in volumes_chapters.get('volumes', {}).items():
                    if not isinstance(vol_data, dict):
                        continue
                    chapters = vol_data.get('chapters', {})
                    if not isinstance(chapters, dict):
                        continue

                    for ch_num, ch_data in chapters.items():
                        if ch_num in seen_chapters:
                            continue
                        seen_chapters.add(ch_num)

                        chapter_id = ch_data.get('id') if isinstance(ch_data, dict) else None
                        if not chapter_id:
                            continue

                        results.append(IssueResult(
                            provider=self.provider_type,
                            id=chapter_id,
                            series_id=series_id,
                            issue_number=str(ch_num),
                            title=None,
                            cover_date=None,
                            store_date=None,
                            cover_url=None,
                            summary=None
                        ))

            # Sort by chapter number
            try:
                results.sort(key=lambda x: float(x.issue_number) if x.issue_number else 0)
            except (ValueError, TypeError):
                pass

            return results
        except Exception as e:
            app_logger.error(f"MangaDex get_issues failed: {e}")
            return []

    def get_issue(self, issue_id: str) -> Optional[IssueResult]:
        """Get chapter details by ID."""
        try:
            chapter_api = self._get_chapter_api()
            if not chapter_api:
                return None

            chapter = chapter_api.get_chapter(issue_id)
            if not chapter:
                return None

            # Extract chapter data
            chapter_num = getattr(chapter, 'chapter', None) or '1'
            manga_id = getattr(chapter, 'manga_id', None)
            title = getattr(chapter, 'title', None)
            publish_at = getattr(chapter, 'publish_at', None)

            # Format date if available
            cover_date = None
            if publish_at:
                try:
                    if hasattr(publish_at, 'strftime'):
                        cover_date = publish_at.strftime('%Y-%m-%d')
                    else:
                        cover_date = str(publish_at)[:10]
                except Exception:
                    pass

            return IssueResult(
                provider=self.provider_type,
                id=issue_id,
                series_id=manga_id or "",
                issue_number=str(chapter_num),
                title=title,
                cover_date=cover_date,
                store_date=None,
                cover_url=None,
                summary=None
            )
        except Exception as e:
            app_logger.error(f"MangaDex get_issue failed: {e}")
            return None

    def get_issue_metadata(self, series_id: str, issue_number: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific chapter in a series."""
        try:
            series = self.get_series(series_id)
            if not series:
                return None

            # Prefix with 'v' for volume (manga convention)
            volume_number = f"v{issue_number}" if not issue_number.startswith('v') else issue_number

            return {
                "Series": series.title,
                "Number": volume_number,
                "Year": series.year,
                "Summary": series.description,
                "Web": f"https://mangadex.org/title/{series_id}",
                "Notes": f"Metadata from MangaDex. Manga ID: {series_id}",
                "Manga": "YesAndRightToLeft"
            }
        except Exception as e:
            app_logger.error(f"MangaDex get_issue_metadata failed: {e}")
            return None

    def to_comicinfo(self, issue: IssueResult, series: Optional[SearchResult] = None) -> Dict[str, Any]:
        """Convert MangaDex data to ComicInfo.xml fields."""
        try:
            if not series and issue.series_id:
                series = self.get_series(issue.series_id)

            # Prefix with 'v' for volume (manga convention)
            volume_number = issue.issue_number
            if volume_number and not volume_number.startswith('v'):
                volume_number = f"v{volume_number}"

            comicinfo = {
                "Number": volume_number,
                "Notes": f"Metadata from MangaDex. Manga ID: {issue.series_id}",
                "Web": f"https://mangadex.org/title/{issue.series_id}",
                "Manga": "YesAndRightToLeft"
            }

            if issue.title:
                comicinfo["Title"] = issue.title

            if issue.cover_date:
                comicinfo["Date"] = issue.cover_date

            if series:
                comicinfo["Series"] = series.title
                if series.year:
                    comicinfo["Year"] = series.year
                if series.description:
                    comicinfo["Summary"] = series.description

            return {k: v for k, v in comicinfo.items() if v is not None}
        except Exception as e:
            app_logger.error(f"MangaDex to_comicinfo failed: {e}")
            return {}
