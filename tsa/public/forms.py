# -*- coding: utf-8 -*-
"""Public forms."""
from flask import g
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired, NumberRange
from wtforms.fields import SelectMultipleField, IntegerField, RadioField
from wtforms.widgets import HiddenInput


class ConfigForm(FlaskForm):
    """Configuration form.
       Note, that entry points and rules are dynamically generated fields,
       so they are validated separately.
    """

    filters = SelectMultipleField()
    maxDepth = IntegerField(validators=[NumberRange(min=0)])
    agent = StringField()
    maxAttempts = IntegerField(validators=[NumberRange(min=0)])
    verifyHttps = RadioField(choices=[('Yes', 'Yes'), ('No', 'No')], default='Yes')
    logLink = RadioField(choices=[('Yes', 'Yes'), ('No', 'No')], default='Yes')
    recordParams = RadioField(choices=[('Yes', 'Yes'), ('No', 'No')], default='No')
    recordHeaders = RadioField(choices=[('Yes', 'Yes'), ('No', 'No')], default='No')
    ruleCount = IntegerField(widget=HiddenInput(), validators=[NumberRange(min=0)])
    restricted = set(["contentLength", "acceptedType", "acceptedUri", "redis-progress"])

    def __init__(self, *args, **kwargs):
        """Create instance."""
        super(ConfigForm, self).__init__(*args, **kwargs)
        try:
            available = [(p['tag'], p['description']) for p in g.all_plugins if (p['type'] == 'filter' or p['type'] == 'header') and p['tag'] not in ConfigForm.restricted]
            checked = [p['tag'] for p in g.all_plugins if p['checked']]
            self.filters.choices=available
            self.filters.default=checked
        except AttributeError:
            pass

    def validate(self):
        initial_validation = super(ConfigForm, self).validate()
        if not initial_validation:
            return False
        return True
