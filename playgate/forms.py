from wtforms import Form, SelectField, StringField, validators

FORMAT_CHOICES = [
    ("", "Default (best with audio)"),
    ("best[acodec!=none]", "best[acodec!=none]"),
    ("best", "best"),
    ("worst", "worst"),
    ("18", "18 — MP4 360p"),
]

PRESET_FORMATS = {value for value, _ in FORMAT_CHOICES if value}


def no_merge_format(form, field):
    value = (field.data or "").strip()
    if "+" in value:
        raise validators.ValidationError(
            "merge formats ('+') need ffmpeg, which is not in the image"
        )
    if len(value) > 64:
        raise validators.ValidationError("format is too long (max 64)")


class ChannelForm(Form):
    name = StringField(
        "Name",
        [
            validators.DataRequired(),
            validators.Length(min=1, max=100),
            validators.Regexp(
                r"^[^/?#]+$",
                message='Name must not contain "/", "?" or "#"',
            ),
        ],
    )
    url = StringField("Source URL", [validators.DataRequired()])
    format = SelectField("Quality", choices=FORMAT_CHOICES, default="")
    format_custom = StringField(
        "Custom format",
        [
            validators.Optional(),
            no_merge_format,
        ],
    )
    redirect_url = StringField("Redirect override", [])


def normalize_channel_form(form: ChannelForm) -> None:
    form.name.data = (form.name.data or "").strip()
    form.url.data = (form.url.data or "").strip()
    form.format_custom.data = (form.format_custom.data or "").strip()
    form.redirect_url.data = (form.redirect_url.data or "").strip()


def effective_format(form: ChannelForm) -> str | None:
    custom = (form.format_custom.data or "").strip()
    selected = (form.format.data or "").strip()
    return custom or selected or None


def format_form_data(fmt: str | None) -> dict[str, str]:
    if not fmt:
        return {"format": "", "format_custom": ""}
    if fmt in PRESET_FORMATS:
        return {"format": fmt, "format_custom": ""}
    return {"format": "", "format_custom": fmt}
