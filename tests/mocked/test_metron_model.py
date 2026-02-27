"""Tests for models/metron.py -- mocked Mokkari API."""
import pytest
from unittest.mock import patch, MagicMock, mock_open
from tests.mocked.conftest import make_mock_series, make_mock_issue


class TestGetApi:

    @patch("models.metron.MokkariSession")
    def test_returns_client(self, mock_session_class):
        from models.metron import get_api, MetronClient

        mock_session_class.return_value = MagicMock()
        client = get_api("user", "pass")
        assert client is not None
        assert isinstance(client, MetronClient)
        mock_session_class.assert_called_once()

    def test_empty_credentials(self):
        from models.metron import get_api
        assert get_api("", "") is None
        assert get_api(None, None) is None


class TestIsConnectionError:

    def test_timeout_detected(self):
        from models.metron import is_connection_error
        from mokkari.exceptions import ApiError
        import requests.exceptions

        exc = ApiError("API error")
        exc.__cause__ = requests.exceptions.ReadTimeout()
        assert is_connection_error(exc) is True

    def test_normal_error_not_connection(self):
        from models.metron import is_connection_error
        assert is_connection_error(Exception("Invalid credentials")) is False

    def test_various_network_errors(self):
        from models.metron import is_connection_error
        from mokkari.exceptions import ApiError
        import requests.exceptions

        exc = ApiError("API error")
        exc.__cause__ = requests.exceptions.ConnectionError()
        assert is_connection_error(exc) is True


class TestParseCvinfo:

    def test_parse_metron_id(self, tmp_path):
        from models.metron import parse_cvinfo_for_metron_id

        cvinfo = tmp_path / "cvinfo"
        cvinfo.write_text("https://comicvine.gamespot.com/batman/4050-12345/\nseries_id: 100\n")

        assert parse_cvinfo_for_metron_id(str(cvinfo)) == 100

    def test_no_series_id(self, tmp_path):
        from models.metron import parse_cvinfo_for_metron_id

        cvinfo = tmp_path / "cvinfo"
        cvinfo.write_text("https://comicvine.gamespot.com/batman/4050-12345/\n")

        assert parse_cvinfo_for_metron_id(str(cvinfo)) is None

    def test_parse_comicvine_id(self, tmp_path):
        from models.metron import parse_cvinfo_for_comicvine_id

        cvinfo = tmp_path / "cvinfo"
        cvinfo.write_text("https://comicvine.gamespot.com/batman/4050-12345/\nseries_id: 100\n")

        assert parse_cvinfo_for_comicvine_id(str(cvinfo)) == 12345


class TestGetSeriesIdByComicvineId:

    def test_found(self, mock_client):
        client, mock_session = mock_client
        mock_series = make_mock_series(id=42)
        mock_session.series_list.return_value = [mock_series]

        assert client.get_series_id_by_comicvine_id(12345) == 42

    def test_not_found(self, mock_client):
        client, mock_session = mock_client
        mock_session.series_list.return_value = []

        assert client.get_series_id_by_comicvine_id(99999) is None


class TestSearchSeriesByName:

    def test_returns_best_match(self, mock_client):
        client, mock_session = mock_client
        s = make_mock_series(id=100, name="Batman", year_began=2016)
        mock_session.series_list.return_value = [s]

        result = client.search_series_by_name("Batman")
        assert result is not None
        assert result["id"] == 100
        assert result["name"] == "Batman"

    def test_year_ranking(self, mock_client):
        client, mock_session = mock_client
        s1 = make_mock_series(id=1, name="Batman", year_began=1940)
        s2 = make_mock_series(id=2, name="Batman", year_began=2016)
        mock_session.series_list.return_value = [s1, s2]

        result = client.search_series_by_name("Batman", year=2016)
        assert result["id"] == 2  # Closer to 2016

    def test_no_results(self, mock_client):
        client, mock_session = mock_client
        mock_session.series_list.return_value = []

        assert client.search_series_by_name("Nonexistent") is None

    def test_no_series_name(self, mock_client):
        client, mock_session = mock_client
        assert client.search_series_by_name("") is None


class TestGetSeriesDetails:

    def test_returns_details(self, mock_client):
        client, mock_session = mock_client
        mock_session.series.return_value = make_mock_series(id=100, cv_id=12345)

        result = client.get_series_details(100)
        assert result["id"] == 100
        assert result["cv_id"] == 12345

    def test_not_found(self, mock_client):
        client, mock_session = mock_client
        mock_session.series.return_value = None

        assert client.get_series_details(9999) is None


class TestGetIssueMetadata:

    def test_double_fetch_pattern(self, mock_client):
        client, mock_session = mock_client
        mock_session.issues_list.return_value = [MagicMock(id=500)]
        full_issue = make_mock_issue(id=500)
        mock_session.issue.return_value = full_issue

        result = client.get_issue_metadata(100, "1")
        assert result is not None
        mock_session.issues_list.assert_called_once()
        mock_session.issue.assert_called_once_with(500)

    def test_issue_not_found(self, mock_client):
        client, mock_session = mock_client
        mock_session.issues_list.return_value = []

        assert client.get_issue_metadata(100, "999") is None


class TestGetAllIssuesForSeries:

    def test_returns_issues(self, mock_client):
        client, mock_session = mock_client
        mock_session.issues_list.return_value = [MagicMock(id=1), MagicMock(id=2)]

        result = client.get_all_issues_for_series(100)
        assert len(result) == 2

    def test_empty_series(self, mock_client):
        client, mock_session = mock_client
        mock_session.issues_list.return_value = []

        assert client.get_all_issues_for_series(100) == []


class TestGetReleases:

    def test_fetches_releases(self, mock_client):
        client, mock_session = mock_client
        mock_session.issues_list.return_value = [MagicMock(), MagicMock()]

        result = client.get_releases("2024-01-01", "2024-01-07")
        assert len(result) == 2

    def test_no_date_before(self, mock_client):
        client, mock_session = mock_client
        mock_session.issues_list.return_value = []

        result = client.get_releases("2024-01-01")
        assert result == []


class TestSessionDelegation:
    """__getattr__ should forward unknown calls to the underlying session."""

    def test_direct_session_method(self, mock_client):
        client, mock_session = mock_client
        mock_session.publishers_list.return_value = [MagicMock()]

        result = client.publishers_list({"name": "Marvel"})
        mock_session.publishers_list.assert_called_once_with({"name": "Marvel"})
        assert len(result) == 1

    def test_issue_lookup(self, mock_client):
        client, mock_session = mock_client
        mock_issue = make_mock_issue(id=500)
        mock_session.issue.return_value = mock_issue

        result = client.issue(500)
        mock_session.issue.assert_called_once_with(500)
        assert result is mock_issue


class TestMapToComicinfo:

    def test_full_mapping(self):
        from models.metron import map_to_comicinfo

        issue_data = {
            "id": 500,
            "number": "1",
            "story_titles": ["The Beginning"],
            "cover_date": "2020-06-15",
            "series": {"name": "Batman", "year_began": 2016, "genres": [{"name": "Superhero"}]},
            "publisher": {"name": "DC Comics"},
            "credits": [
                {"creator": "Tom King", "role": [{"name": "Writer"}]},
                {"creator": "David Finch", "role": [{"name": "Penciller"}]},
            ],
            "characters": [{"name": "Batman"}, {"name": "Catwoman"}],
            "teams": [{"name": "Justice League"}],
            "rating": {"name": "Teen"},
            "desc": "Batman returns to Gotham",
            "resource_url": "https://metron.cloud/issue/500/",
            "modified": "2024-01-01",
            "page_count": 32,
        }

        result = map_to_comicinfo(issue_data)

        assert result["Series"] == "Batman"
        assert result["Number"] == "1"
        assert result["Title"] == "The Beginning"
        assert result["Year"] == 2020
        assert result["Month"] == 6
        assert result["Day"] == 15
        assert result["Publisher"] == "DC Comics"
        assert result["Writer"] == "Tom King"
        assert result["Penciller"] == "David Finch"
        assert "Batman" in result["Characters"]
        assert result["Genre"] == "Superhero"
        assert result["LanguageISO"] == "en"
        assert result["MetronId"] == 500

    def test_minimal_data(self):
        from models.metron import map_to_comicinfo

        result = map_to_comicinfo({"id": 1, "number": "1"})
        assert "Number" in result
        assert result["Number"] == "1"
        assert "Notes" in result


class TestExtractCreditsByRole:

    def test_extracts_writers(self):
        from models.metron import extract_credits_by_role

        credits = [
            {"creator": "Tom King", "role": [{"name": "Writer"}]},
            {"creator": "David Finch", "role": [{"name": "Penciller"}]},
        ]
        result = extract_credits_by_role(credits, ["Writer"])
        assert result == "Tom King"

    def test_multiple_matches(self):
        from models.metron import extract_credits_by_role

        credits = [
            {"creator": "Tom King", "role": [{"name": "Writer"}]},
            {"creator": "Scott Snyder", "role": [{"name": "Writer"}]},
        ]
        result = extract_credits_by_role(credits, ["Writer"])
        assert "Tom King" in result
        assert "Scott Snyder" in result

    def test_no_matches(self):
        from models.metron import extract_credits_by_role

        credits = [{"creator": "David Finch", "role": [{"name": "Penciller"}]}]
        result = extract_credits_by_role(credits, ["Writer"])
        assert result == ""


class TestCalculateComicWeek:

    def test_returns_tuple(self):
        from models.metron import calculate_comic_week
        from datetime import datetime

        start, end = calculate_comic_week(datetime(2024, 1, 15))  # Monday
        assert start.weekday() == 6  # Sunday
        assert end.weekday() == 5    # Saturday

    def test_string_date(self):
        from models.metron import calculate_comic_week

        start, end = calculate_comic_week("2024-01-15")
        assert start is not None
        assert end is not None

    def test_defaults_to_now(self):
        from models.metron import calculate_comic_week

        start, end = calculate_comic_week()
        assert start is not None


class TestUpdateCvinfoWithMetronId:

    def test_appends_series_id(self, tmp_path):
        from models.metron import update_cvinfo_with_metron_id

        cvinfo = tmp_path / "cvinfo"
        cvinfo.write_text("https://comicvine.gamespot.com/batman/4050-12345/\n")

        assert update_cvinfo_with_metron_id(str(cvinfo), 100) is True
        content = cvinfo.read_text()
        assert "series_id: 100" in content

    def test_updates_existing(self, tmp_path):
        from models.metron import update_cvinfo_with_metron_id

        cvinfo = tmp_path / "cvinfo"
        cvinfo.write_text("series_id: 50\n")

        assert update_cvinfo_with_metron_id(str(cvinfo), 100) is True
        content = cvinfo.read_text()
        assert "series_id: 100" in content
        assert "series_id: 50" not in content
