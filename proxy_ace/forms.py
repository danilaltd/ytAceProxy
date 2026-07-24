from wtforms import Form, StringField, validators

class RedirectForm(Form):
    name = StringField('Name', [validators.DataRequired(), validators.Length(min=1)])
    url = StringField('Url', [validators.DataRequired()])
    redirect_url = StringField('Redirect', [])