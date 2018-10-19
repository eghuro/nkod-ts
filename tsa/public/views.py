# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
from flask import Blueprint, flash, session, redirect, render_template, request, url_for, g, jsonify, abort
from flask import current_app as app

from tsa.extensions import db, cache
from tsa.cache import cached
from tsa.utils import flash_errors

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/', methods=['GET'])
def home():
    """Landing page."""
    return render_template('public/landing.html')


