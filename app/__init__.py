from flask import Flask, render_template
from flask_login import LoginManager
from config import Config

app = Flask(__name__)
app.config.from_object(Config)


@app.errorhandler(404)
def not_found(e):
    return render_template("404.html", title="Page not found"), 404

# Flask-Login setup
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'info'
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    from app.models import User
    return User.get_by_id(int(user_id))


# Initialize the database and seed users from env
from app.models import init_db, seed_users_from_env, ensure_demo_user
init_db()
seed_users_from_env()
ensure_demo_user()

from app import routes
from app import auth
from app import upload
from app import insights
from app import taxes
from app import journal
from app import weekly_review
from app import schwab
