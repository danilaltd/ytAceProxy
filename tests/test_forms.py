from playgate.forms import (
    ChannelForm,
    effective_format,
    format_form_data,
)


class FormData(dict):
    """Minimal werkzeug-like form data; raw_data is needed by Optional()."""

    def getlist(self, key):
        value = self.get(key)
        if value is None:
            return []
        return value if isinstance(value, list) else [value]


def test_format_form_data_preset():
    assert format_form_data("best") == {"format": "best", "format_custom": ""}


def test_format_form_data_custom():
    assert format_form_data("best[height<=720]") == {
        "format": "",
        "format_custom": "best[height<=720]",
    }


def test_format_form_data_empty():
    assert format_form_data(None) == {"format": "", "format_custom": ""}


def test_effective_format_prefers_custom():
    form = ChannelForm(
        data={
            "name": "nsf",
            "url": "http://source",
            "format": "best",
            "format_custom": "worst",
        }
    )
    assert effective_format(form) == "worst"


def test_effective_format_falls_back_to_preset():
    form = ChannelForm(
        data={
            "name": "nsf",
            "url": "http://source",
            "format": "best",
            "format_custom": "",
        }
    )
    assert effective_format(form) == "best"


def test_merge_format_is_rejected():
    form = ChannelForm(
        FormData(
            {
                "name": "nsf",
                "url": "http://source",
                "format": "",
                "format_custom": "bestvideo+bestaudio",
            }
        )
    )

    assert form.validate() is False
    assert "ffmpeg" in form.format_custom.errors[0]


def test_name_rejects_url_separators():
    for name in ("a/b", "a?b", "a#b"):
        form = ChannelForm(
            FormData(
                {
                    "name": name,
                    "url": "http://source",
                    "format": "",
                    "format_custom": "",
                }
            )
        )
        assert form.validate() is False, name
