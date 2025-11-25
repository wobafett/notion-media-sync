import os
import unittest

import router
from main import _resolve_target_name


class RouterTests(unittest.TestCase):
    def test_targets_present(self):
        targets = router.available_targets()
        self.assertIn("games", targets)
        self.assertIn("music", targets)
        self.assertIn("movies", targets)
        self.assertIn("books", targets)

    def test_games_database_id_normalization(self):
        target = router.get_target("games")
        originals = {
            "NOTION_GAMES_DATABASE_ID": os.environ.get("NOTION_GAMES_DATABASE_ID"),
            "NOTION_DATABASE_ID": os.environ.get("NOTION_DATABASE_ID"),
        }
        try:
            os.environ.pop("NOTION_DATABASE_ID", None)
            os.environ["NOTION_GAMES_DATABASE_ID"] = "1234-ABcd-5678"
            database_ids = target.database_ids()
            self.assertEqual(database_ids, ["1234abcd5678"])
        finally:
            for key, value in originals.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_music_database_ids_collect_all(self):
        target = router.get_target("music")
        originals = {
            "NOTION_ARTISTS_DATABASE_ID": os.environ.get("NOTION_ARTISTS_DATABASE_ID"),
            "NOTION_ALBUMS_DATABASE_ID": os.environ.get("NOTION_ALBUMS_DATABASE_ID"),
            "NOTION_SONGS_DATABASE_ID": os.environ.get("NOTION_SONGS_DATABASE_ID"),
            "NOTION_LABELS_DATABASE_ID": os.environ.get("NOTION_LABELS_DATABASE_ID"),
        }
        try:
            os.environ["NOTION_ARTISTS_DATABASE_ID"] = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"
            os.environ["NOTION_ALBUMS_DATABASE_ID"] = "1111-2222-3333-4444"
            os.environ["NOTION_SONGS_DATABASE_ID"] = ""
            os.environ["NOTION_LABELS_DATABASE_ID"] = "abcd1234"
            database_ids = target.database_ids()
            self.assertEqual(
                database_ids,
                [
                    "aaaabbbbccccddddeeeeffffffffffff",
                    "1111222233334444",
                    "abcd1234",
                ],
            )
        finally:
            for key, value in originals.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_movies_database_id_normalization(self):
        target = router.get_target("movies")
        originals = {
            "NOTION_MOVIETV_DATABASE_ID": os.environ.get("NOTION_MOVIETV_DATABASE_ID"),
            "NOTION_DATABASE_ID": os.environ.get("NOTION_DATABASE_ID"),
        }
        try:
            os.environ.pop("NOTION_DATABASE_ID", None)
            os.environ["NOTION_MOVIETV_DATABASE_ID"] = "abcd-1234-efgh-5678"
            self.assertEqual(target.database_ids(), ["abcd1234efgh5678"])
        finally:
            for key, value in originals.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_books_database_id_normalization(self):
        target = router.get_target("books")
        originals = {
            "NOTION_BOOKS_DATABASE_ID": os.environ.get("NOTION_BOOKS_DATABASE_ID"),
            "NOTION_DATABASE_ID": os.environ.get("NOTION_DATABASE_ID"),
        }
        try:
            os.environ.pop("NOTION_DATABASE_ID", None)
            os.environ["NOTION_BOOKS_DATABASE_ID"] = "feed-face-beef-dead-bead12345678"
            self.assertEqual(target.database_ids(), ["feedfacebeefdeadbead12345678"])
        finally:
            for key, value in originals.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


class MainHelpersTests(unittest.TestCase):
    def test_resolve_target_name_priority(self):
        targets = ["games", "movies", "music", "books"]
        os.environ.pop("SYNC_TARGET", None)
        resolved = _resolve_target_name("music", None, targets)
        self.assertEqual(resolved, "music")

        resolved = _resolve_target_name(None, "games", targets)
        self.assertEqual(resolved, "games")

        os.environ["SYNC_TARGET"] = "music"
        resolved = _resolve_target_name(None, None, targets)
        self.assertEqual(resolved, "music")


if __name__ == "__main__":
    unittest.main()


