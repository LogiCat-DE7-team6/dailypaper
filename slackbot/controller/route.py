import json
from flask import Blueprint, request

from services.SlackActions import SlackActions as sa

bp = Blueprint('main', __name__, url_prefix='/')

@bp.route("/slack/actions", methods=["POST"])
def slack_actions():
    try:
        payload = json.loads(request.form["payload"])
        action = payload.get("actions")[0]
        button_value = action.get("value", None)

        return sa.send_user_select_result(payload, button_value)
        
    except Exception as e:
        raise e
    
@bp.route("/slack/actions/info", methods=["POST"])
def get_paper_info():
    try:
        return sa.send_info_data()
    except Exception as e:
        raise e