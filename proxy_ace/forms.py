from wtforms import Form, StringField, validators


class RedirectForm(Form):
    name = StringField('Name', [validators.DataRequired(), validators.Length(min=1)])
    url = StringField('Url', [validators.DataRequired()])
    redirect_url = StringField('Redirect', [])


class AceChannelForm(Form):
    name = StringField('Name', [
        validators.DataRequired(),
        validators.Length(min=1, max=100),
        validators.Regexp(
            r'^[^/]+$',
            message='Name must not contain "/"',
        ),
    ])
    content_id = StringField('Content ID', [
        validators.DataRequired(),
        validators.Length(min=40, max=40),
        validators.Regexp(
            r'^[0-9a-fA-F]{40}$',
            message='Content ID must be 40 hexadecimal characters',
        ),
    ])
