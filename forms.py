from wtforms import Form, StringField, validators

class AceChannelForm(Form):
    name = StringField('Name', [validators.DataRequired(), validators.Length(min=1)])
    hash = StringField('HASH', [validators.DataRequired()])

class RedirectForm(Form):
    name = StringField('Name', [validators.DataRequired(), validators.Length(min=1)])
    url = StringField('Url', [validators.DataRequired()])
    redirect_url = StringField('Redirect', [])