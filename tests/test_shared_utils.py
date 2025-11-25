import unittest

from shared.change_detection import has_property_changes
from shared.utils import clean_multi_select_value, normalize_id


class UtilsTestCase(unittest.TestCase):
    def test_normalize_id_strips_dashes(self):
        self.assertEqual(normalize_id("1234-ABcd-5678"), "1234abcd5678")

    def test_normalize_id_handles_none(self):
        self.assertIsNone(normalize_id(None))

    def test_clean_multi_select_value_truncates_and_strips(self):
        dirty_value = " Action,  Adventure;\nEpic Saga "
        self.assertEqual(clean_multi_select_value(dirty_value), "Action Adventure Epic Saga")


class ChangeDetectionTestCase(unittest.TestCase):
    def test_no_changes_detected(self):
        page_properties = {
            "Description": {
                "type": "rich_text",
                "rich_text": [{"plain_text": "Same"}],
            },
            "Genres": {
                "type": "multi_select",
                "multi_select": [{"name": "Action"}],
            },
        }
        new_properties = {
            "Description": {"rich_text": [{"text": {"content": "Same"}}]},
            "Genres": {"multi_select": [{"name": "Action"}]},
        }

        has_changes, differences = has_property_changes(page_properties, new_properties)
        self.assertFalse(has_changes)
        self.assertEqual(differences, [])

    def test_detects_content_changes(self):
        page_properties = {
            "Description": {
                "type": "rich_text",
                "rich_text": [{"plain_text": "Old"}],
            }
        }
        new_properties = {
            "Description": {"rich_text": [{"text": {"content": "New"}}]}
        }

        has_changes, differences = has_property_changes(page_properties, new_properties)
        self.assertTrue(has_changes)
        self.assertIn("Description: rich_text changed", differences)


if __name__ == "__main__":
    unittest.main()


